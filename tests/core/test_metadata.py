import hashlib

import pytest

from tests import utils
from waterbutler.core import metadata


class TestBaseMetadata:

    def test_file_metadata(self):
        file_metadata = utils.MockFileMetadata()

        assert file_metadata.is_file is True
        assert file_metadata.is_folder is False

    def test_folder_metadata(self):
        folder_metadata = utils.MockFolderMetadata()

        assert folder_metadata.is_file is False
        assert folder_metadata.is_folder is True

    def test_file_json_api_serialize(self):
        file_metadata = utils.MockFileMetadata()
        serialized = file_metadata.json_api_serialized('n0d3z')
        link_suffix = '/v1/resources/n0d3z/providers/MockProvider/Foo.name'
        etag = hashlib.sha256('{}::{}'.format('MockProvider', 'etag').encode('utf-8')).hexdigest()

        assert serialized['id'] == 'MockProvider/Foo.name'
        assert serialized['type'] == 'files'
        assert serialized['attributes'] == {
            'extra': {},
            'kind': 'file',
            'name': 'Foo.name',
            'path': '/Foo.name',
            'provider': 'MockProvider',
            'materialized': '/Foo.name',
            'etag': etag,
            'contentType': 'application/octet-stream',
            'modified': '9/25/2017',
            'modified_utc': '1991-09-25T19:20:30.45+01:00',
            'created_utc': 'always',
            'size': 1337,
            'sizeInt': 1337,
            'resource': 'n0d3z',
        }
        assert 'new_folder' not in serialized['links']
        assert serialized['links']['move'].endswith(link_suffix)
        assert serialized['links']['upload'].endswith(link_suffix + '?kind=file')
        assert serialized['links']['download'].endswith(link_suffix)
        assert serialized['links']['delete'].endswith(link_suffix)

    def test_folder_json_api_serialize(self):
        folder_metadata = utils.MockFolderMetadata()
        serialized = folder_metadata.json_api_serialized('n0d3z')
        link_suffix = '/v1/resources/n0d3z/providers/MockProvider/Bar/'
        etag = hashlib.sha256('{}::{}'.format('MockProvider', 'etag').encode('utf-8')).hexdigest()

        assert serialized['id'] == 'MockProvider/Bar/'
        assert serialized['type'] == 'files'
        assert serialized['attributes'] == {
            'extra': {},
            'kind': 'folder',
            'name': 'Bar',
            'path': '/Bar/',
            'provider': 'MockProvider',
            'materialized': '/Bar/',
            'etag': etag,
            'size': None,
            'sizeInt': None,
            'resource': 'n0d3z',
            'created': 'never',
            'created_utc': '1991-09-25T19:20:30.45+01:00',
            'modified': 'never',
            'modified_utc': '1991-09-25T19:20:30.45+01:00'
        }
        assert serialized['links']['new_folder'].endswith(link_suffix + '?kind=folder')
        assert serialized['links']['move'].endswith(link_suffix)
        assert serialized['links']['upload'].endswith(link_suffix + '?kind=file')
        assert 'download' not in serialized['links']
        assert serialized['links']['delete'].endswith(link_suffix)

    def test_folder_json_api_size_serialize(self):
        folder_metadata = utils.MockFolderMetadata()
        folder_metadata.children = [utils.MockFileMetadata()]
        serialized = folder_metadata.json_api_serialized('n0d3z')
        child = serialized['attributes']['children'][0]
        etag = hashlib.sha256('{}::{}'.format('MockProvider', 'etag').encode('utf-8')).hexdigest()

        assert len(serialized['attributes']['children']) == 1
        assert child == {
            'extra': {},
            'kind': 'file',
            'name': 'Foo.name',
            'path': '/Foo.name',
            'provider': 'MockProvider',
            'materialized': '/Foo.name',
            'etag': etag,
            'contentType': 'application/octet-stream',
            'modified': '9/25/2017',
            'modified_utc': '1991-09-25T19:20:30.45+01:00',
            'created_utc': 'always',
            'size': 1337,
            'sizeInt': 1337,
        }

    def test_file_revision_json_api_serialize(self):
        file_revision_metadata = utils.MockFileRevisionMetadata()
        serialized = file_revision_metadata.json_api_serialized()

        assert serialized['id'] == 1
        assert serialized['type'] == 'file_versions'
        assert serialized['attributes'] == {
            'extra': {},
            'version': 1,
            'modified': 'never',
            'modified_utc': 'never',
            'versionIdentifier': 'versions',
        }


class TestNoPayloadDrivenConstruction:
    """CL M-1 / 決定-21: core carries no way to build a metadata class named by a payload.

    The ``dehydrate``/``rehydrate`` pair came from COS's Celery serialization work (ENG-7534)
    and has nothing to do with SigV4.  ``rehydrate`` read a dotted name out of the payload and
    turned it into a class -- ``importlib.import_module(...)`` then ``getattr`` -- so anything
    that ever reached it with attacker-shaped input would be choosing which class WaterButler
    instantiates and what it is called with.  Nothing in GRDM calls either method, so the
    gadget sat there earning nothing.

    ``core`` is inherited by every provider, so this is also the one place where the S3 branch
    was widening its blast radius beyond S3.  These assertions are what keeps a later merge
    from restoring the pair without the decision being revisited (台帳 U-11).
    """

    @pytest.mark.parametrize('name', ['dehydrate', '_dehydrate', 'rehydrate', '_rehydrate'])
    def test_base_metadata_has_no_hydration_hooks(self, name):
        assert not hasattr(metadata.BaseMetadata, name)

    def test_core_metadata_does_not_import_importlib(self):
        """The import is the gadget's only ingredient; its absence is what makes the removal
        real rather than a rename."""
        assert not hasattr(metadata, 'importlib')
