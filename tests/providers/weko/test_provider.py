import pytest

from tests.utils import MockCoroutine

import io
import time
import base64
import hashlib
from http import client
from unittest import mock

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.weko import WEKOProvider
from waterbutler.providers.weko.metadata import WEKOItemMetadata
from waterbutler.providers.weko.metadata import WEKOIndexMetadata


fake_weko_host = 'https://test.sample.nii.ac.jp/sword'
fake_weko_indices = [
    {
        'id': 100,
        'name': 'Sample Index',
        'children': [
            {
                'id': 101,
                'name': 'Sub Index',
                'children': [],
            },
        ],
    },
]
fake_weko_item = {
    'id': 1000,
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
                    },
                ],
            },
        },
    },
}
fake_weko_items = {
    'hits': {
        'hits': [
            fake_weko_item,
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
        'user_id': 'requester'
    }


@pytest.fixture
def settings():
    return {
        'url': fake_weko_host,
        'index_id': '100',
        'index_title': 'sample archive',
        'nid': 'project_id'
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


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_item_file(self, provider, mock_time):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?search_type=2&q=100',
            body=fake_weko_items,
        )
        path = await provider.validate_path('/Sample Item/file.txt')
        assert path.name == 'file.txt'
        assert path.identifier == ('item_file', 'file.txt', 'file.txt')
        assert path.parent.name == 'Sample Item'
        assert path.parent.identifier == ('item', 1000, 'Sample Item')
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_sub_index(self, provider, mock_time):
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/tree',
            body=fake_weko_indices,
        )
        aiohttpretty.register_json_uri(
            'GET',
            'https://test.sample.nii.ac.jp/api/index/?search_type=2&q=100',
            body=fake_weko_items,
        )
        path = await provider.validate_path('/Sub Index/')
        assert path.name == 'Sub Index'
        assert path.identifier == ('index', 101, 'Sub Index')
        assert path.parent.name == ''
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root


class TestOperations:

    async def test_equality(self, provider, mock_time):
        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)
