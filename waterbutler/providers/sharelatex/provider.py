from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.sharelatex import settings
from waterbutler.providers.sharelatex.metadata import ShareLatexFileMetadata
from waterbutler.providers.sharelatex.metadata import ShareLatexFolderMetadata


class ShareLatexProvider(provider.BaseProvider):
    """Provider for the ShareLatex
    """
    NAME = 'sharelatex'
    BASE_URL = settings.BASE_URL

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

        self.project_id = settings.get('project_id')

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_intra_copy(self, dest_provider, path=None):
        return False

    def can_intra_move(self, dest_provider, path=None):
        return False

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)

    @asyncio.coroutine
    def download(self, path, **kwargs):
        """Returns a ResponseWrapper (Stream) for the specified path
        raises FileNotFoundError if the status from ShareLatex is not 200

        :param str path: Path to the key you want to download
        :param dict \*\*kwargs: Additional arguments that are ignored
        :rtype: :class:`waterbutler.core.streams.ResponseStreamReader`
        :raises: :class:`waterbutler.core.exceptions.DownloadError`
        """
        url = self._build_content_url(path.path)

        resp = yield from self.make_request(
            'GET',
            url,
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
        url = self._build_content_url(path.full_path)

        resp = yield from self.make_request(
            'PUT',
            url,
            data=stream,
            headers={'Content-Length': str(stream.size)},
            expects=(200, 201, ),
            throws=exceptions.UploadError,
        )

        return (yield from self.metadata(path, **kwargs)), not exists

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        """Deletes the key at the specified path

        :param str path: The path of the key to delete

        """

        url = self._build_content_url(path.full_path)
        yield from self.make_request(
            'DELETE',
            url,
            expects=(200, 204, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def metadata(self, path, **kwargs):
        """Get Metadata about the requested file or folder

        :param WaterButlerPath path: The path to a key or folder
        :rtype: dict or list
        """

        url = self._build_content_url(path.path)

        resp = yield from self.make_request(
            'GET', url,
            expects=(200, ),
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()

        if path.is_dir:
            ret = []
            for item in data['contents']:
                if item['is_dir']:
                    ret.append(ShareLatexFolderMetadata(item))
                else:
                    ret.append(ShareLatexFileMetadata(item))
            return ret

        return ShareLatexFileMetadata(data)

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        raise exceptions.ProviderError({'message': 'figshare does not support file revisions.'}, code=405)
