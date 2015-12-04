import pytest

from tests.utils import async

import io
import json
import random

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions

from waterbutler.providers.sharelatex import metadata
from waterbutler.providers.sharelatex import provider

from tests.providers.sharelatex import fixtures


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }

@pytest.fixture
def credentials():
    return {
        'access_key': 'brian',
    }

@pytest.fixture
def settings():
    return {
        'project': 'to define',
    }

@pytest.fixture
def empty_project_settings():
    return {
        'project': fixtures.empty_project_id
    }

@pytest.fixture
def empty_project_provider(auth, credentials, empty_project_settings):
    return provider.ShareLatexProvider(auth, credentials, empty_project_settings)

@pytest.fixture
def only_files_metadata():
    return fixtures.only_files_metadata

@pytest.fixture
def only_folders_metadata():
    return fixtures.only_folders_metadata

@pytest.fixture
def empty_metadata():
    return {}

@pytest.fixture
def default_project_provider(auth, credentials, settings):
    return provider.ShareLatexProvider(auth, credentials, settings)

@pytest.fixture
def default_project_metadata():
    return fixtures.default_project_metadata


class TestMetadata:


    def check_metadata_is_folder_with_path_and_name(self, metadata, path):
        assert metadata[0].kind == 'file'
        assert metadata[0].provider == 'sharelatex'
        assert metadata[0].path == '/projetoprincipal.tex'
        # TODO: test size, mimetime, other files and folders.


    @async
    @pytest.mark.aiohttpretty
    def test_no_root_folder(self, empty_project_provider, empty_metadata):
        root_folder_path = yield from empty_project_provider.validate_path('/')
        root_folder_url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'docs')
        aiohttpretty.register_json_uri('GET', root_folder_url, body=empty_metadata)

        with pytest.raises(exceptions.NotFoundError) as e:
            yield from empty_project_provider.metadata(root_folder_path)

    @async
    @pytest.mark.aiohttpretty
    def test_root_folder_with_one_folder(self, default_project_provider, default_project_metadata):

        root_folder_path = yield from default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=default_project_metadata)

        result = yield from default_project_provider.metadata(root_folder_path)

        self.check_metadata_is_folder_with_path_and_name(result, root_folder_path)

    @async
    @pytest.mark.aiohttpretty
    def test_root_folder_without_folders(self, default_project_provider, only_files_metadata):

        root_folder_path = yield from default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=only_files_metadata)

        result = yield from default_project_provider.metadata(root_folder_path)
        for f in result:
            assert f.kind is 'file'


    @async
    @pytest.mark.aiohttpretty
    def test_root_folder_without_files(self, default_project_provider, only_folders_metadata):

        root_folder_path = yield from default_project_provider.validate_path('/')
        root_folder_url = default_project_provider.build_url('project', default_project_provider.project_id, 'docs')

        aiohttpretty.register_json_uri('GET', root_folder_url, body=only_folders_metadata)

        result = yield from default_project_provider.metadata(root_folder_path)
        for f in result:
            assert f.kind not 'file'


class TestCRUD:


    @async
    @pytest.mark.aiohttpretty
    def test_download_error(self, empty_project_provider):
        path = yield from empty_project_provider.validate_path('/')
        url = empty_project_provider.build_url('project', empty_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_json_uri('GET', url)

        with pytest.raises(exceptions.DownloadError) as e:
            yield from empty_project_provider.download(path)

    @async
    @pytest.mark.aiohttpretty
    def test_download_any_content(self, default_project_provider):
        body = b'castle on a cloud'
        path = yield from default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_json_uri('GET', url, body=body)

        result = yield from default_project_provider.download(path)
        content = yield from result.read()

        assert content == body

    @async
    @pytest.mark.aiohttpretty
    def test_download_when_accept_url(self, default_project_provider):
        path = yield from default_project_provider.validate_path('/raw.txt')
        url = default_project_provider.build_url('project', default_project_provider.project_id, 'file', path.path)
        aiohttpretty.register_json_uri('GET', url)

        result = yield from default_project_provider.download(path, accept_url=True)
        assert result == url

    @async
    @pytest.mark.aiohttpretty
    def test_create_folder(self, default_project_provider):
        path = yield from default_project_provider.validate_path('/')
        with pytest.raises(exceptions.NotImplementedError) as e:
            yield from empty_project_provider.create_folder(path)

    @async
    @pytest.mark.aiohttpretty
    def test_upload(self, default_project_provider):
        path = yield from default_project_provider.validate_path('/')
        with pytest.raises(exceptions.NotImplementedError) as e:
            yield from empty_project_provider.upload(path)

    @async
    @pytest.mark.aiohttpretty
    def test_delete(self, default_project_provider):
        path = yield from default_project_provider.validate_path('/')
        with pytest.raises(exceptions.NotImplementedError) as e:
            yield from empty_project_provider.delete(path)
