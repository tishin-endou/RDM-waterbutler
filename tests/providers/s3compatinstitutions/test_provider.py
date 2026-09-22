import pytest

from waterbutler.core.path import WaterButlerPath
from tests import utils

from waterbutler.providers.s3compatinstitutions import S3CompatInstitutionsProvider
from tests.providers.s3compat.test_provider import (
    TestProviderConstruction,
    TestValidatePath,
    TestCRUD,
    TestMetadata,
    TestCreateFolder,
    TestOperations,
    auth,
    credentials,
    mock_time,
    file_content,
    file_like,
    file_stream,
    file_header_metadata,
    file_metadata_headers_object,
    file_metadata_object,
    folder_key_metadata_object,
    folder_metadata_object,
    revision_metadata_object,
    copy_object_resp,
    api_error_resp,
    single_version_metadata,
    version_metadata,
    folder_and_contents,
    folder_empty_metadata,
    folder_item_metadata,
    folder_metadata,
    folder_single_item_metadata,
    complete_upload_resp,
    create_session_resp,
    generic_http_403_resp,
    generic_http_404_resp,
    list_parts_resp_empty,
    list_parts_resp_not_empty,
    upload_parts_headers_list
)

@pytest.fixture
def base_prefix():
    return ''


@pytest.fixture
def settings(base_prefix):
    return {
        'bucket': 'that kerning',
        'prefix': base_prefix,
        'encrypt_uploads': False
    }


@pytest.fixture
def provider(auth, credentials, settings):
    return S3CompatInstitutionsProvider(auth, credentials, settings)


class TestProviderConstruction2(TestProviderConstruction):
    pass


class TestValidatePath2(TestValidatePath):
    pass


class TestCRUD2(TestCRUD):
    pass


class TestMetadata2(TestMetadata):
    pass


class TestCreateFolder2(TestCreateFolder):
    pass


class TestOperations2(TestOperations):

    def test_can_intra_copy_true_for_same_provider_and_small_file(self, provider):
        """Allows intra-copy when dest provider is same class, path is a file, and size < limit"""
        path = WaterButlerPath('/some-file.txt')
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT - 1
        assert provider.can_intra_copy(provider, path=path, file_size=file_size) is True

    def test_can_intra_move_true_for_same_provider_and_small_file(self, provider):
        """Allows intra-move when dest provider is same class, path is a file, and size < limit"""
        path = WaterButlerPath('/some-file.txt')
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT - 1
        assert provider.can_intra_move(provider, path=path, file_size=file_size) is True

    def test_can_intra_copy_path_none(self, provider):
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT - 1
        with pytest.raises(AttributeError):
            provider.can_intra_copy(provider, path=None, file_size=file_size)

    def test_can_intra_move_path_none(self, provider):
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT - 1
        with pytest.raises(AttributeError):
            provider.can_intra_move(provider, path=None, file_size=file_size)

    def test_can_intra_copy_path_invalid_type(self, provider):
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT - 1
        with pytest.raises(AttributeError):
            provider.can_intra_copy(provider, path='not-a-path-object', file_size=file_size)

    def test_can_intra_move_path_invalid_type(self, provider):
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT - 1
        with pytest.raises(AttributeError):
            provider.can_intra_move(provider, path='not-a-path-object', file_size=file_size)
