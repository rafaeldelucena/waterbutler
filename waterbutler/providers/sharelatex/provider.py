import os
import asyncio
import hashlib
from urllib import parse

import xmltodict

from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import SubdomainCallingFormat

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.sharelatex import settings
from waterbutler.providers.sharelatex.metadata import ShareLatexRevision
from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexFolderMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexFolderKeyMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadataHeaders


class ShareLatexProvider(provider.BaseProvider):
    """Provider for the ShareLatex
    """
    NAME = 'sharelatex'

    def __init__(self, auth, credentials, settings):
        """
        .. note::

            Neither `ShareLatexConnection#__init__` nor `ShareLatexConnection#get_bucket`
            sends a request.

        :param dict auth: Not used
        :param dict credentials: Dict containing `access_key` and `secret_key`
        :param dict settings: Dict containing `bucket`
        """
        super().__init__(auth, credentials, settings)

        # If a bucket has capital letters in the name
        # ordinary calling format MUST be used
        if settings['bucket'] != settings['bucket'].lower():
            calling_format = OrdinaryCallingFormat()
        else:
            # if a bucket is out of the us Subdomain calling format MUST be used
            calling_format = SubdomainCallingFormat()

        self.connection = S3Connection(credentials['access_key'],
                credentials['secret_key'], calling_format=calling_format)
        self.bucket = self.connection.get_bucket(settings['bucket'], validate=False)
        self.encrypt_uploads = self.settings.get('encrypt_uploads', False)

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    def can_intra_move(self, dest_provider, path=None):
        return type(self) == type(dest_provider) and not getattr(path, 'is_dir', False)

    @asyncio.coroutine
    def intra_copy(self, dest_provider, source_path, dest_path):
        """Copy key from one ShareLatex bucket to another. The credentials specified in
        `dest_provider` must have read access to `source.bucket`.
        """
        exists = yield from dest_provider.exists(dest_path)
        dest_key = dest_provider.bucket.new_key(dest_path.path)

        # ensure no left slash when joining paths
        source_path = '/' + os.path.join(self.settings['bucket'], source_path.path)
        headers = {'x-amz-copy-source': parse.quote(source_path)}
        url = dest_key.generate_url(
            settings.TEMP_URL_SECS,
            'PUT',
            headers=headers,
        )
        yield from self.make_request(
            'PUT', url,
            headers=headers,
            expects=(200, ),
            throws=exceptions.IntraCopyError,
        )
        return (yield from dest_provider.metadata(dest_path)), not exists

    @asyncio.coroutine
    def download(self, path, accept_url=False, version=None, range=None, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from ShareLatex is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        if not path.is_file:
            raise exceptions.DownloadError('No file specified for download', code=400)

        if not version or version.lower() == 'latest':
            query_parameters = None
        else:
            query_parameters = {'versionId': version}

        if kwargs.get('displayName'):
            response_headers = {'response-content-disposition': 'attachment; filename*=UTF-8\'\'{}'.format(parse.quote(kwargs['displayName']))}
        else:
            response_headers = {'response-content-disposition': 'attachment'}

        url = self.bucket.new_key(
            path.path
        ).generate_url(
            settings.TEMP_URL_SECS,
            query_parameters=query_parameters,
            response_headers=response_headers
        )

        if accept_url:
            return url

        resp = yield from self.make_request(
            'GET',
            url,
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):
        """Uploads the given stream to ShareLatex

        :param waterbutler.core.streams.RequestWrapper stream: The stream to put to ShareLatex
        :param str path: The full path of the key to upload to/into

        :rtype: dict, bool
        """
        path, exists = yield from self.handle_name_conflict(path, conflict=conflict)
        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        resp = yield from self.make_request(
            'PUT',
            self.bucket.new_key(path.path).generate_url(
                settings.TEMP_URL_SECS,
                'PUT',
                encrypt_key=self.encrypt_uploads
            ),
            data=stream,
            headers={'Content-Length': str(stream.size)},
            expects=(200, 201, ),
            throws=exceptions.UploadError,
        )
        # md5 is returned as ETag header as long as server side encryption is not used.
        # TODO: nice assertion error goes here
        assert resp.headers['ETag'].replace('"', '') == stream.writers['md5'].hexdigest

        return (yield from self.metadata(path, **kwargs)), not exists

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete
        """
        yield from self.make_request(
            'DELETE',
            self.bucket.new_key(path.path).generate_url(settings.TEMP_URL_SECS, 'DELETE'),
            expects=(200, 204, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        """Get past versions of the requested key

        :param str path: The path to a key
        :rtype list:
        """
        url = self.bucket.generate_url(settings.TEMP_URL_SECS, 'GET', query_parameters={'versions': ''})
        resp = yield from self.make_request(
            'GET',
            url,
            params={'prefix': path.path, 'delimiter': '/'},
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        content = yield from resp.read_and_close()
        versions = xmltodict.parse(content)['ListVersionsResult'].get('Version') or []

        if isinstance(versions, dict):
            versions = [versions]

        return [
            ShareLatexRevision(item)
            for item in versions
            if item['Key'] == path.path
        ]

    @asyncio.coroutine
    def metadata(self, path, revision=None, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """
        if path.is_dir:
            return (yield from self._metadata_folder(path))

        return (yield from self._metadata_file(path, revision=revision))

    @asyncio.coroutine
    def create_folder(self, path, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        WaterButlerPath.validate_folder(path)

        if (yield from self.exists(path)):
            raise exceptions.FolderNamingConflict(str(path))

        yield from self.make_request(
            'PUT',
            self.bucket.new_key(path.path).generate_url(settings.TEMP_URL_SECS, 'PUT'),
            expects=(200, 201),
            throws=exceptions.CreateFolderError
        )

        return ShareLatexFolderMetadata({'Prefix': path.path})

    @asyncio.coroutine
    def _metadata_file(self, path, revision=None):
        if revision == 'Latest':
            revision = None
        resp = yield from self.make_request(
            'HEAD',
            self.bucket.new_key(
                path.path
            ).generate_url(
                settings.TEMP_URL_SECS,
                'HEAD',
                query_parameters={'versionId': revision} if revision else None
            ),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )
        return ShareLatexFileMetadataHeaders(path.path, resp.headers)

    @asyncio.coroutine
    def _metadata_folder(self, path):
        resp = yield from self.make_request(
            'GET',
            self.bucket.generate_url(settings.TEMP_URL_SECS, 'GET'),
            params={'prefix': path.path, 'delimiter': '/'},
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        contents = yield from resp.read_and_close()

        parsed = xmltodict.parse(contents, strip_whitespace=False)['ListBucketResult']

        contents = parsed.get('Contents', [])
        prefixes = parsed.get('CommonPrefixes', [])

        if not contents and not prefixes and not path.is_root:
            # If contents and prefixes are empty then this "folder"
            # must exist as a key with a / at the end of the name
            # if the path is root there is no need to test if it exists
            yield from self.make_request(
                'HEAD',
                self.bucket.new_key(path.path).generate_url(settings.TEMP_URL_SECS, 'HEAD'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            )

        if isinstance(contents, dict):
            contents = [contents]

        if isinstance(prefixes, dict):
            prefixes = [prefixes]

        items = [
            ShareLatexFolderMetadata(item)
            for item in prefixes
        ]

        for content in contents:
            if content['Key'] == path.path:
                continue

            if content['Key'].endswith('/'):
                items.append(ShareLatexFolderKeyMetadata(content))
            else:
                items.append(ShareLatexFileMetadata(content))

        return items
