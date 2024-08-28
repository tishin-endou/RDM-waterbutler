import pytest

from tests.utils import MockCoroutine

import io
import time
import base64
import logging
import hashlib
from http import client
from unittest import mock

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.weko import WEKOProvider
from waterbutler.providers.osfstorage.provider import OSFStorageProvider
from waterbutler.providers.osfstorage.metadata import (
    OsfStorageFileMetadata,
    OsfStorageFolderMetadata,
)
from waterbutler.providers.weko.client import Index, Item, File
from waterbutler.providers.weko.metadata import (
    WEKOIndexMetadata, WEKOItemMetadata,  WEKOFileMetadata,
    WEKODraftFileMetadata, WEKODraftFolderMetadata,
)


logger = logging.getLogger(__name__)


fake_weko_host = 'https://test.sample.nii.ac.jp/sword'
fake_weko_indices = [
    {
        'id': '100',
        'name': 'Sample Index',
        'children': [
            {
                'id': '101',
                'name': 'Sub Index',
                'children': [],
            },
        ],
    },
]
fake_weko_item_1000 = {
    'id': '1000',
    'metadata': {
        'title': 'Sample Item',
        '_item_metadata': {
            'title': 'Sample Item',
            'item_dummy_content': {
            },
            'item_dummy_files': {
                'attribute_type': 'file',
                'attribute_value_mlt': [
                    {
                        'filename': 'file.txt',
                        'version_id': 1,
                        'url': {
                            'url': 'https://test.sample.nii.ac.jp/objects/1000/file.txt',
                        },
                    },
                ],
            },
            'item_dummy_title': {
                'attribute_name': 'title',
                'attribute_value_mlt': [
                    {
                        'subitem_title': 'Sample Item',
                        'subitem_title_language': 'en',
                    },
                    {
                        'subitem_title': 'サンプルアイテム',
                        'subitem_title_language': 'ja',
                    },
                ],
            },
        },
    },
}
fake_weko_item_1001 = {
    'id': '1001',
    'metadata': {
        'title': 'Sub Item',
        '_item_metadata': {
            'title': 'Sub Item',
            'item_dummy_content': {
            },
            'item_dummy_files': {
                'attribute_type': 'file',
                'attribute_value_mlt': [
                    {
                        'filename': 'sub_file.txt',
                        'version_id': 1,
                        'url': {
                            'url': 'https://test.sample.nii.ac.jp/objects/1001/sub_file.txt',
                        },
                    },
                ],
            },
        },
    },
}
fake_weko_items = {
    'hits': {
        'hits': [
            fake_weko_item_1000,
        ]
    },
}
fake_weko_sub_items = {
    'hits': {
        'hits': [
            fake_weko_item_1001,
        ]
    },
}


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com'
    }


@pytest.fixture(scope='module', params=['token'])
def credentials(request):
    return {
        request.param: 'open inside',
        'user_id': 'requester',
        'default_storage': {
            'storage': {
                'access_key': 'Dont dead',
                'secret_key': 'open inside',
            },
        }
    }


@pytest.fixture
def settings():
    return {
        'url': fake_weko_host,
        'index_id': '100',
        'index_title': 'sample archive',
        'nid': 'project_id',
        'default_storage': {
            'nid': 'project_id',
            'justa': 'setting',
            'rootId': 'rootId',
            'baseUrl': 'https://waterbutler.io',
            'storage': {
                'provider': 'mock',
            },
        },
    }

@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    provider = WEKOProvider(auth, credentials, settings)
    return provider


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def file_metadata():
    metadata_weko_folder = OsfStorageFolderMetadata({
        'name': '.weko',
        'path': '/0123456789abcdefg000/',
        'materialized': '/.weko/',
        'provider': 'osfstorage',
    }, '/.weko/')
    metadata_index_folder = OsfStorageFolderMetadata({
        'name': '100',
        'path': '/0123456789abcdefg001/',
        'materialized': '/.weko/100/',
        'provider': 'osfstorage',
    }, '/.weko/100/')
    metadata_draft_file = OsfStorageFileMetadata({
        'name': 'birdie.jpg',
        'materialized': '/.weko/100/birdie.jpg',
        'provider': 'osfstorage',
        'modified': '2024-01-01T00:00:00+00:00',
        'path': '/0123456789abcdefg002',
        'size': 6,
        'version': 1,
        'downloads': 0,
        'checkout': None,
        'md5': 'md5hash',
        'sha256': 'sha256hash',
    }, '/.weko/100/birdie.jpg')
    metadata_draft_folder = OsfStorageFolderMetadata({
        'name': 'test_folder',
        'path': '/0123456789abcdefg003/',
        'materialized': '/.weko/100/test_folder/',
        'provider': 'osfstorage',
    }, '/.weko/100/test_folder/')
    metadata_sub_draft_file = OsfStorageFileMetadata({
        'name': 'sub_file.txt',
        'materialized': '/.weko/100/test_folder/sub_file.txt',
        'provider': 'osfstorage',
        'modified': '2024-01-01T00:00:00+00:00',
        'path': '/0123456789abcdefg004',
        'size': 6,
        'version': 1,
        'downloads': 0,
        'checkout': None,
        'md5': 'md5hash',
        'sha256': 'sha256hash',
    }, '/.weko/100/test_folder/sub_file.txt')
    metadata_sub_draft_folder = OsfStorageFolderMetadata({
        'name': 'sub_folder',
        'path': '/0123456789abcdefg005/',
        'materialized': '/.weko/100/test_folder/sub_folder/',
        'provider': 'osfstorage',
    }, '/.weko/100/test_folder/sub_folder/')
    return dict(
        weko_folder=metadata_weko_folder,
        index_folder=metadata_index_folder,
        draft_file=metadata_draft_file,
        draft_folder=metadata_draft_folder,
        sub_draft_file=metadata_sub_draft_file,
        sub_draft_folder=metadata_sub_draft_folder,
    )


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_item_file(self, provider, mock_time):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        path = await provider.validate_path('/Sample Item/file.txt')
        assert path.name == 'file.txt'
        assert path.identifier == ('item_file', 'file.txt', 'file.txt')
        assert path.parent.name == 'Sample Item'
        assert path.parent.identifier == ('item', '1000', 'Sample Item')
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_sub_index(self, provider, mock_time):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        path = await provider.validate_path('/Sub Index/')
        assert path.name == 'Sub Index'
        assert path.identifier == ('index', '101', 'Sub Index')
        assert path.parent.name == ''
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_root_metadata(self, provider, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/')
        assert path.is_root

        result = await provider.metadata(path)

        assert len(result) == 2

        index_metadata = result[0]
        assert index_metadata.name == 'Sub Index'
        assert index_metadata.extra['weko'] == 'index'
        assert index_metadata.extra['weko_web_url'] == 'https://test.sample.nii.ac.jp/search?q=101'
        assert index_metadata.extra['indexId'] == '101'
        assert index_metadata.provider == 'weko'
        assert index_metadata.path == '/weko:101/'
        assert index_metadata.materialized_path == '/Sub Index/'

        item_metadata = result[1]
        assert item_metadata.name == 'Sample Item'
        assert item_metadata.extra['weko'] == 'item'
        assert item_metadata.extra['weko_web_url'] == 'https://test.sample.nii.ac.jp/records/1000'
        assert item_metadata.extra['fileId'] == 'item1000'
        assert item_metadata.extra['item_title'] == [
            {'subitem_title': 'Sample Item', 'subitem_title_language': 'en'},
            {'subitem_title': 'サンプルアイテム', 'subitem_title_language': 'ja'},
        ]
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/weko:item1000/'
        assert item_metadata.materialized_path == '/Sample Item/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_item_metadata(self, provider, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/records/1000',
            body=fake_weko_item_1000,
        )
        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/weko:item1000/')
        assert path.is_item

        result = await provider.metadata(path)

        assert len(result) == 1

        item_metadata = result[0]
        assert item_metadata.name == 'file.txt'
        assert item_metadata.extra['weko'] == 'file'
        assert item_metadata.extra['itemId'] == 'item1000'
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/weko:item1000/file.txt'
        assert item_metadata.materialized_path == '/Sample Item/file.txt'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_sub_item_metadata(self, provider, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=101',
            body=fake_weko_sub_items,
        )
        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/weko:101/')
        assert path.is_index

        result = await provider.metadata(path)

        assert len(result) == 1

        item_metadata = result[0]
        assert item_metadata.name == 'Sub Item'
        assert item_metadata.extra['weko'] == 'item'
        assert item_metadata.extra['weko_web_url'] == 'https://test.sample.nii.ac.jp/records/1001'
        assert item_metadata.extra['fileId'] == 'item1001'
        assert item_metadata.extra['item_title'] == [{'subitem_title': 'Sub Item'}]
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/weko:101/weko:item1001/'
        assert item_metadata.materialized_path == '/Sub Index/Sub Item/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_item_file_metadata(self, provider, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/records/1000',
            body=fake_weko_item_1000,
        )
        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/weko:item1000/file.txt')
        assert path.is_item_file

        item_metadata = await provider.metadata(path)
        assert item_metadata.name == 'file.txt'
        assert item_metadata.extra['weko'] == 'file'
        assert item_metadata.extra['itemId'] == 'item1000'
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/weko:item1000/file.txt'
        assert item_metadata.materialized_path == '/Sample Item/file.txt'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_draft_file_metadata(self, provider, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']
        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_file]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/birdie.jpg')
        assert path.is_draft_file

        item_metadata = await provider.metadata(path)
        assert item_metadata.name == 'birdie.jpg'
        assert item_metadata.extra['weko'] == 'draft'
        assert item_metadata.extra['index'] == '100'
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/birdie.jpg'
        assert item_metadata.materialized_path == '/birdie.jpg'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_draft_folder_metadata(self, provider, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']
        metadata_draft_folder = file_metadata['draft_folder']
        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_file, metadata_draft_folder]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/')
        result = await provider.metadata(path)

        assert len(result) == 4

        index_metadata = result[0]
        assert index_metadata.name == 'Sub Index'
        assert index_metadata.extra['weko'] == 'index'
        assert index_metadata.extra['weko_web_url'] == 'https://test.sample.nii.ac.jp/search?q=101'
        assert index_metadata.extra['indexId'] == '101'
        assert index_metadata.provider == 'weko'
        assert index_metadata.path == '/weko:101/'
        assert index_metadata.materialized_path == '/Sub Index/'

        item_metadata = result[1]
        assert item_metadata.name == 'Sample Item'
        assert item_metadata.extra['weko'] == 'item'
        assert item_metadata.extra['weko_web_url'] == 'https://test.sample.nii.ac.jp/records/1000'
        assert item_metadata.extra['fileId'] == 'item1000'
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/weko:item1000/'
        assert item_metadata.materialized_path == '/Sample Item/'

        item_metadata = result[2]
        assert item_metadata.name == 'birdie.jpg'
        assert item_metadata.extra['weko'] == 'draft'
        assert item_metadata.extra['index'] == '100'
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/birdie.jpg'
        assert item_metadata.materialized_path == '/birdie.jpg'

        index_metadata = result[3]
        assert index_metadata.name == 'test_folder'
        assert index_metadata.extra['weko'] == 'draft'
        assert index_metadata.extra['index'] == '100'
        assert index_metadata.provider == 'weko'
        assert index_metadata.path == '/test_folder/'
        assert index_metadata.materialized_path == '/test_folder/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_sub_draft_file_metadata(self, provider, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']
        metadata_sub_draft_file = file_metadata['sub_draft_file']
        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_folder]
            if str(path) == '/0123456789abcdefg003/':
                return [metadata_sub_draft_file]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/test_folder/')
        assert path.is_draft_file

        result = await provider.metadata(path)
        item_metadata = result[0]
        assert item_metadata.name == 'sub_file.txt'
        assert item_metadata.extra['weko'] == 'draft'
        assert item_metadata.extra['index'] == '100'
        assert item_metadata.provider == 'weko'
        assert item_metadata.path == '/test_folder/sub_file.txt'
        assert item_metadata.materialized_path == '/test_folder/sub_file.txt'


class TestPathFromMetadata:

    def test_path_from_index_metadata(self, provider):
        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        index = WEKOIndexMetadata(index.identifier, mock_client, index)
        assert index.materialized_path == '/'

        parent_path = WaterButlerPath('/')
        path = provider.path_from_metadata(parent_path, index)

        assert path.name == 'Test Index'
        assert path.identifier == ('index', '100', 'Test Index')

    def test_path_from_sub_index_metadata(self, provider):
        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        index = WEKOIndexMetadata(parent.identifier, mock_client, index)
        assert index.materialized_path == '/Test Index/'

        parent_path = WaterButlerPath('/')
        path = provider.path_from_metadata(parent_path, index)

        assert path.name == 'Test Index'
        assert path.identifier == ('index', '101', 'Test Index')

    def test_path_from_sub_index_metadata_as_root(self, provider):
        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        index = WEKOIndexMetadata(index.identifier, mock_client, index)
        assert index.materialized_path == '/'

        parent_path = WaterButlerPath('/')
        path = provider.path_from_metadata(parent_path, index)

        assert path.name == 'Test Index'
        assert path.identifier == ('index', '101', 'Test Index')

    def test_path_from_item_metadata(self, provider):
        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        item = Item(fake_weko_item_1000, index)
        metadata = WEKOItemMetadata(index.identifier, mock_client, item, index, 'weko')
        assert metadata.materialized_path == '/Sample Item/'

        parent_path = WaterButlerPath('/')
        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'Sample Item'
        assert path.identifier == ('item', '1000', 'Sample Item')

    def test_path_from_item_in_sub_index_metadata(self, provider):
        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        item = Item(fake_weko_item_1000, index)
        metadata = WEKOItemMetadata(parent.identifier, mock_client, item, index, 'weko')
        assert metadata.materialized_path == '/Test Index/Sample Item/'

        parent_path = WaterButlerPath('/')
        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'Sample Item'
        assert path.identifier == ('item', '1000', 'Sample Item')

    def test_path_from_item_in_sub_index_metadata_as_root(self, provider):
        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        item = Item(fake_weko_item_1000, index)
        metadata = WEKOItemMetadata(index.identifier, mock_client, item, index, 'weko')
        assert metadata.materialized_path == '/Sample Item/'

        parent_path = WaterButlerPath('/')
        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'Sample Item'
        assert path.identifier == ('item', '1000', 'Sample Item')

    def test_path_from_item_file_metadata(self, provider):
        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        item = Item(fake_weko_item_1000, index)
        file = File(fake_weko_item_1000['metadata']['_item_metadata']['item_dummy_files']['attribute_value_mlt'][0])
        metadata = WEKOFileMetadata(index.identifier, file, item, index)
        assert metadata.materialized_path == '/Sample Item/file.txt'

        parent_path = WaterButlerPath('/weko:1000/')
        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'file.txt'
        assert path.identifier == ('item_file', 'file.txt', 'file.txt')

    def test_path_from_item_file_in_sub_index_metadata(self, provider):
        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        item = Item(fake_weko_item_1000, index)
        file = File(fake_weko_item_1000['metadata']['_item_metadata']['item_dummy_files']['attribute_value_mlt'][0])
        metadata = WEKOFileMetadata(parent.identifier, file, item, index)
        assert metadata.materialized_path == '/Test Index/Sample Item/file.txt'

        parent_path = WaterButlerPath('/weko:101/')
        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'file.txt'
        assert path.identifier == ('item_file', 'file.txt', 'file.txt')

    def test_path_from_item_file_in_sub_index_metadata_as_root(self, provider):
        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        item = Item(fake_weko_item_1000, index)
        file = File(fake_weko_item_1000['metadata']['_item_metadata']['item_dummy_files']['attribute_value_mlt'][0])
        metadata = WEKOFileMetadata(index.identifier, file, item, index)
        assert metadata.materialized_path == '/Sample Item/file.txt'

        parent_path = WaterButlerPath('/weko:101/')
        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'file.txt'
        assert path.identifier == ('item_file', 'file.txt', 'file.txt')

    def test_path_from_draft_file_metadata(self, provider, file_metadata):
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']

        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        parent_path = WaterButlerPath('/0123456789abcdefg001/')
        metadata = WEKODraftFileMetadata(index.identifier, metadata_draft_file, metadata_index_folder, index)
        assert metadata.materialized_path == '/birdie.jpg'

        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'birdie.jpg'
        assert path.identifier == ('draft_file', 'birdie.jpg', 'birdie.jpg')

    def test_path_from_draft_file_in_sub_index_metadata(self, provider, file_metadata):
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']

        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        parent_path = WaterButlerPath('/0123456789abcdefg001/')
        metadata = WEKODraftFileMetadata(parent.identifier, metadata_draft_file, metadata_index_folder, index)
        assert metadata.materialized_path == '/Test Index/birdie.jpg'

        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'birdie.jpg'
        assert path.identifier == ('draft_file', 'birdie.jpg', 'birdie.jpg')

    def test_path_from_draft_file_in_sub_index_metadata_as_root(self, provider, file_metadata):
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']

        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        parent_path = WaterButlerPath('/0123456789abcdefg001/')
        metadata = WEKODraftFileMetadata(index.identifier, metadata_draft_file, metadata_index_folder, index)
        assert metadata.materialized_path == '/birdie.jpg'

        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'birdie.jpg'
        assert path.identifier == ('draft_file', 'birdie.jpg', 'birdie.jpg')

    def test_path_from_draft_folder_metadata(self, provider, file_metadata):
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']

        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        parent_path = WaterButlerPath('/0123456789abcdefg001/')
        metadata = WEKODraftFolderMetadata(index.identifier, metadata_draft_folder, metadata_index_folder, index)

        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'test_folder'
        assert path.identifier == ('draft_file', 'test_folder', 'test_folder')

    def test_path_from_draft_folder_in_sub_index_metadata(self, provider, file_metadata):
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']

        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        parent_path = WaterButlerPath('/0123456789abcdefg001/')
        metadata = WEKODraftFolderMetadata(parent.identifier, metadata_draft_folder, metadata_index_folder, index)
        assert metadata.materialized_path == '/Test Index/test_folder/'

        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'test_folder'
        assert path.identifier == ('draft_file', 'test_folder', 'test_folder')

    def test_path_from_draft_folder_in_sub_index_metadata_as_root(self, provider, file_metadata):
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']

        mock_client = mock.MagicMock()
        parent = Index(mock_client, {
            'id': '100',
            'name': 'Parent Index'
        })
        index = Index(
            mock_client,
            {
                'id': '101',
                'name': 'Test Index',
            },
            parent=parent,
        )
        parent_path = WaterButlerPath('/0123456789abcdefg001/')
        metadata = WEKODraftFolderMetadata(index.identifier, metadata_draft_folder, metadata_index_folder, index)
        assert metadata.materialized_path == '/test_folder/'

        path = provider.path_from_metadata(parent_path, metadata)

        assert path.name == 'test_folder'
        assert path.identifier == ('draft_file', 'test_folder', 'test_folder')


class TestUpload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_draft_file(self, provider, file_stream, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']
        mock_default_storage_create_folder = MockCoroutine(
            side_effect=lambda path: metadata_weko_folder if str(path) == '/.weko/' else metadata_index_folder
        )
        monkeypatch.setattr(OSFStorageProvider, 'create_folder', mock_default_storage_create_folder)

        mock_default_storage_upload = MockCoroutine(return_value=(metadata_draft_file, True))
        monkeypatch.setattr(OSFStorageProvider, 'upload', mock_default_storage_upload)

        path = await provider.validate_path('/birdie.jpg')
        result, created = await provider.upload(file_stream, path)

        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        expected = WEKODraftFileMetadata(index.identifier, metadata_draft_file, metadata_index_folder, index)

        assert created is True
        assert result == expected
        assert mock_default_storage_create_folder.call_count == 2
        assert str(mock_default_storage_create_folder.call_args_list[0][0][0]) == '/.weko/'
        assert str(mock_default_storage_create_folder.call_args_list[1][0][0]) == '/0123456789abcdefg000/100/'
        assert mock_default_storage_upload.call_count == 1
        assert str(mock_default_storage_upload.call_args[0][1]) == '/0123456789abcdefg001/birdie.jpg'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_sub_draft_file(self, provider, file_stream, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )

        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']
        metadata_sub_draft_file = file_metadata['sub_draft_file']

        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_folder]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        mock_default_storage_upload = MockCoroutine(return_value=(metadata_sub_draft_file, True))
        monkeypatch.setattr(OSFStorageProvider, 'upload', mock_default_storage_upload)

        path = await provider.validate_path('/test_folder/sub_file.txt')
        result, created = await provider.upload(file_stream, path)

        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        expected = WEKODraftFileMetadata(index.identifier, metadata_sub_draft_file, metadata_index_folder, index)

        assert created is True
        assert result == expected


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_draft_folder(self, provider, file_stream, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )

        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']
        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        def resolve_create_folder(path):
            logger.info(f'create_folder: {path}')
            if str(path) == '/.weko/':
                return metadata_weko_folder
            if str(path) == '/0123456789abcdefg000/100/':
                return metadata_index_folder
            if str(path) == '/0123456789abcdefg001/test_folder':
                return metadata_draft_folder
            assert False
        mock_default_storage_create_folder = MockCoroutine(side_effect=resolve_create_folder)
        monkeypatch.setattr(OSFStorageProvider, 'create_folder', mock_default_storage_create_folder)

        path = await provider.validate_path('/test_folder/')
        result = await provider.create_folder(path)

        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        expected = WEKODraftFolderMetadata(index.identifier, metadata_draft_folder, metadata_index_folder, index)
        assert result == expected
        assert mock_default_storage_create_folder.call_count == 3
        assert str(mock_default_storage_create_folder.call_args_list[0][0][0]) == '/.weko/'
        assert str(mock_default_storage_create_folder.call_args_list[1][0][0]) == '/0123456789abcdefg000/100/'
        assert str(mock_default_storage_create_folder.call_args_list[2][0][0]) == '/0123456789abcdefg001/test_folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_create_sub_draft_folder(self, provider, file_stream, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )

        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']

        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_folder]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        def resolve_create_folder(path):
            logger.info(f'create_folder: {path}')
            if str(path) == '/0123456789abcdefg003/sub_folder':
                return metadata_draft_folder
            assert False
        mock_default_storage_create_folder = MockCoroutine(side_effect=resolve_create_folder)
        monkeypatch.setattr(OSFStorageProvider, 'create_folder', mock_default_storage_create_folder)

        path = await provider.validate_path('/test_folder/sub_folder/')
        result = await provider.create_folder(path)

        mock_client = mock.MagicMock()
        index = Index(mock_client, {
            'id': '100',
            'name': 'Test Index'
        })
        expected = WEKODraftFolderMetadata(index.identifier, metadata_draft_folder, metadata_index_folder, index)
        assert result == expected
        assert mock_default_storage_create_folder.call_count == 1
        assert str(mock_default_storage_create_folder.call_args_list[0][0][0]) == '/0123456789abcdefg003/sub_folder'

class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_draft_file(self, provider, file_stream, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']
        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_file]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/birdie.jpg')

        mock_default_storage_download = MockCoroutine(return_value=file_stream)
        monkeypatch.setattr(OSFStorageProvider, 'download', mock_default_storage_download)

        await provider.download(path)

        mock_default_storage_download.assert_called_once()
        assert str(mock_default_storage_download.call_args[0][0]) == '/0123456789abcdefg002'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_sub_draft_file(self, provider, file_stream, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']
        metadata_sub_draft_file = file_metadata['sub_draft_file']
        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_folder]
            if str(path) == '/0123456789abcdefg003/':
                return [metadata_sub_draft_file]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/test_folder/sub_file.txt')

        mock_default_storage_download = MockCoroutine(return_value=file_stream)
        monkeypatch.setattr(OSFStorageProvider, 'download', mock_default_storage_download)

        await provider.download(path)

        mock_default_storage_download.assert_called_once()
        assert str(mock_default_storage_download.call_args[0][0]) == '/0123456789abcdefg004'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_item_file(self, provider, file_stream, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/records/1000',
            body=fake_weko_item_1000,
        )
        aiohttpretty.register_uri(
            'GET',
            'https://test.sample.nii.ac.jp/objects/1000/file.txt',
            body=b'sleepy',
            headers={'Content-Length': '6'},
        )
        mock_default_storage_metadata = MockCoroutine(return_value=[])
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        path = await provider.validate_path('/Sample Item/file.txt')

        mock_default_storage_download = MockCoroutine(return_value=file_stream)
        monkeypatch.setattr(OSFStorageProvider, 'download', mock_default_storage_download)

        await provider.download(path)

        mock_default_storage_download.assert_not_called()
        aiohttpretty.has_call(method='GET', uri='https://test.sample.nii.ac.jp/objects/1000/file.txt')

class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_draft_file(self, provider, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_file = file_metadata['draft_file']

        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_file]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        mock_default_storage_delete = MockCoroutine(return_value=metadata_draft_file)
        monkeypatch.setattr(OSFStorageProvider, 'delete', mock_default_storage_delete)

        path = await provider.validate_path('/birdie.jpg')
        await provider.delete(path)

        assert mock_default_storage_delete.call_count == 1
        assert str(mock_default_storage_delete.call_args[0][0]) == '/0123456789abcdefg002'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_draft_folder(self, provider, file_metadata, monkeypatch):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree?action=browsing',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?page=1&size=1000&sort=-createdate&q=100',
            body=fake_weko_items,
        )
        metadata_weko_folder = file_metadata['weko_folder']
        metadata_index_folder = file_metadata['index_folder']
        metadata_draft_folder = file_metadata['draft_folder']

        def resolve_metadata(path):
            if str(path) == '/':
                return [metadata_weko_folder]
            if str(path) == '/0123456789abcdefg000/':
                return [metadata_index_folder]
            if str(path) == '/0123456789abcdefg001/':
                return [metadata_draft_folder]
            assert False
        mock_default_storage_metadata = MockCoroutine(side_effect=resolve_metadata)
        monkeypatch.setattr(OSFStorageProvider, 'metadata', mock_default_storage_metadata)

        mock_default_storage_validate_path = MockCoroutine(side_effect=lambda path: WaterButlerPath(path))
        monkeypatch.setattr(OSFStorageProvider, 'validate_path', mock_default_storage_validate_path)

        mock_default_storage_delete = MockCoroutine(return_value=metadata_draft_folder)
        monkeypatch.setattr(OSFStorageProvider, 'delete', mock_default_storage_delete)

        path = await provider.validate_path('/test_folder/')
        await provider.delete(path)

        assert mock_default_storage_delete.call_count == 1
        assert str(mock_default_storage_delete.call_args[0][0]) == '/0123456789abcdefg003/'

class TestOperations:

    async def test_equality(self, provider, mock_time):
        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)
