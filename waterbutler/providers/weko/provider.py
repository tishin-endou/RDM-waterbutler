import logging
import hashlib

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core import utils
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart

from waterbutler.providers.weko.metadata import (
    ITEM_PREFIX,
    parse_item_file_id,
    WEKOFileMetadata,
    WEKOItemMetadata,
    WEKOIndexMetadata,
    WEKODraftFolderMetadata,
    WEKODraftFileMetadata,
)
from waterbutler.providers.weko.client import Client

logger = logging.getLogger(__name__)
METADATA_JSON_SUFFIX = '-metadata.json'


class WEKOPathPart(WaterButlerPathPart):

    @property
    def is_index(self):
        if not self._id:
            return False
        t, _, _ = self._id
        return t == 'index'

    @property
    def is_item(self):
        if not self._id:
            return False
        t, _, _ = self._id
        return t == 'item'

    @property
    def is_item_file(self):
        if not self._id:
            return False
        t, _, _ = self._id
        return t == 'item_file'

    @property
    def is_draft_file(self):
        if not self._id:
            return True
        t, _, _ = self._id
        return t == 'draft_file'

    @property
    def identifier_value(self):
        if not self._id:
            return None
        _, i, _ = self._id
        return i

    @property
    def materialized(self):
        if not self._id:
            return self.value
        _, _, v = self._id
        return v


class WEKOPath(WaterButlerPath):
    PART_CLASS = WEKOPathPart

    @property
    def materialized_path(self):
        return '/'.join([x.materialized for x in self.parts]) + ('/' if self.is_dir else '')

    @property
    def is_index(self):
        return self.parts[-1].is_index

    @property
    def is_item(self):
        return self.parts[-1].is_item

    @property
    def is_item_file(self):
        return self.parts[-1].is_item_file

    @property
    def is_draft_file(self):
        return self.parts[-1].is_draft_file

    @property
    def as_file(self):
        parts = self.parts
        path = '/'.join([x.materialized for x in parts])
        return WEKOPath(path, _ids=[p._id for p in parts])

    def split_draft_file_path(self):
        pos_ = [i for i, part in enumerate(self.parts) if i > 0 and part.is_draft_file]
        if len(pos_) == 0:
            return self, None
        pos = pos_[0]
        parent_parts = self.parts[:pos]
        parent_path = '/'.join([x.materialized for x in parent_parts]) + '/'
        child_parts = [self.parts[0]] + self.parts[pos:]
        child_path = '/'.join([x.materialized for x in child_parts]) + ('/' if self.is_dir else '')
        logger.debug(f'Split {parent_path}({parent_parts}) -> {child_path}({child_parts})')
        return (
            WEKOPath(parent_path, _ids=[p._id for p in parent_parts]),
            WEKOPath(child_path, _ids=[p._id for p in child_parts]),
        )

    def split_path(self):
        if len(self.parts) == 1:
            return self, None
        parent_parts = self.parts[:-1]
        parent_path = '/'.join([x.materialized for x in parent_parts]) + '/'
        return (
            WEKOPath(parent_path, _ids=[p._id for p in parent_parts]),
            self.parts[-1],
        )


class WEKOProvider(provider.BaseProvider):
    """Provider for WEKO"""

    NAME = 'weko'
    connection = None

    def __init__(self, auth, credentials, settings, **kwargs):
        """
        :param dict auth: Not used
        :param dict credentials: Contains `token`
        :param dict settings: Contains `url`, `index_id` and `index_title` of a repository.
        """
        super().__init__(auth, credentials, settings, **kwargs)
        self.nid = self.settings['nid']
        self.BASE_URL = self.settings['url']

        self.user_id = self.credentials['user_id']
        self.index_id = self.settings['index_id']
        self.index_title = self.settings['index_title']
        self.default_storage_credentials = credentials.get('default_storage', None)
        self.default_storage_settings = settings.get('default_storage', None)
        self.client = Client(
            self,
            self.BASE_URL,
            token=self.credentials['token'],
        )

    def make_default_provider(self):
        if not getattr(self, '_default_provider', None):
            self._default_provider = utils.make_provider(
                'osfstorage',
                self.auth,
                self.default_storage_credentials,
                self.default_storage_settings,
                is_celery_task=self.is_celery_task,
            )
        return self._default_provider

    def path_from_metadata(self, parent_path, metadata):
        return parent_path.child(metadata.name,
                                 _id=self._metadata_to_id(metadata),
                                 folder=metadata.is_folder)

    def build_url(self, path, *segments, **query):
        return super().build_url(*(tuple(path.split('/')) + segments), **query)

    def can_duplicate_names(self):
        return False

    async def validate_v1_path(self, path, **kwargs):
        return await self.validate_path(path, **kwargs)

    async def validate_path(self, path, revision=None, **kwargs):
        """Ensure path is in configured index

        :param str path: The path to a file
        :param list metadata: List of file metadata from _get_data
        """
        parts = path.rstrip('/').split('/')
        ids = []
        index = None
        item = None
        file = None
        for i, part in enumerate(parts):
            # Specialized Path?
            if part.startswith(ITEM_PREFIX):
                item_id = parse_item_file_id(part)
                if item_id is None:
                    if item is not None:
                        raise exceptions.MetadataError('Invalid path: No indexes under item', code=400)
                    index = await self.client.get_index_by_id(part[len(ITEM_PREFIX):])
                    ids.append(('index', index.identifier, index.title))
                else:
                    if item is not None:
                        raise exceptions.MetadataError('Invalid path: No item under item', code=400)
                    if index is None:
                        index = await self.client.get_index_by_id(str(self.index_id))
                    item = await index.get_item_by_id(item_id)
                    ids.append(('item', item.identifier, item.primary_title))
                continue
            if file is not None:
                raise exceptions.MetadataError('Invalid path: No path segment below the item file', code=400)
            # Root path segment
            if part == '' and i == 0:
                ids.append(('root', part, part))
                continue
            # File?
            if item is not None:
                file_cands = [f for f in item.files if f.filename == part]
                if len(file_cands) == 0:
                    raise exceptions.MetadataError('File not found', code=404)
                file = file_cands[0]
                ids.append(('item_file', file.filename, file.filename))
                continue
            if index is None:
                index = await self.client.get_index_by_id(str(self.index_id))
            # Index?
            index_cands = [i for i in index.children if i.title == part]
            if len(index_cands) > 0:
                index = index_cands[0]
                ids.append(('index', index.identifier, index.title))
                continue
            # Item?
            item_cands = [i for i in await index.get_items() if i.primary_title == part]
            if len(item_cands) > 0:
                item = item_cands[0]
                ids.append(('item', item.identifier, item.primary_title))
                continue
            # Draft files
            ids.append(('draft_file', part, part))
        logger.debug(f'WEKOPath {path} -> {ids}')
        return WEKOPath(path, _ids=ids)

    def _metadata_to_id(self, metadata):
        if isinstance(metadata, WEKOIndexMetadata):
            return ('index', metadata.index_identifier, metadata.name)
        if isinstance(metadata, WEKOItemMetadata):
            return ('item', metadata.item_identifier, metadata.name)
        if isinstance(metadata, WEKOFileMetadata):
            return ('item_file', metadata.name, metadata.name)
        if isinstance(metadata, WEKODraftFileMetadata):
            return ('draft_file', metadata.name, metadata.name)
        if isinstance(metadata, WEKODraftFolderMetadata):
            return ('draft_file', metadata.name, metadata.name)
        raise exceptions.MetadataError('Unexpected metadata', code=400)

    async def create_folder(self, path, **kwargs):
        if not path.is_draft_file:
            raise exceptions.MetadataError('Cannot create folders to the item', code=400)
        index = await self._get_last_index_for(path)
        default_provider, index_folder = await self.get_index_folder(index.identifier, creates=True)

        logger.debug(f'Draft folder: {index_folder}')
        _, draft_path = path.split_draft_file_path()
        draft_parent_path, last_part = draft_path.split_path()
        if len(draft_parent_path.parts) == 1:
            parent_folder_metadata = index_folder
        else:
            draft_parent_path = draft_parent_path.as_file
            logger.debug(f'Target path: {draft_parent_path}')
            parent_folder_metadata = await self.get_draft_file_metadata(
                default_provider,
                index_folder,
                draft_parent_path,
            )
        logger.debug(f'Target folder: {parent_folder_metadata.path}')
        draft_path = await default_provider.validate_path(
            parent_folder_metadata.path + last_part.value
        )
        metadata = await default_provider.create_folder(
            draft_path, **kwargs
        )
        return WEKODraftFolderMetadata(self.index_id, metadata, index_folder, index)

    async def upload(self, stream, path, **kwargs):
        if not path.is_draft_file:
            raise exceptions.MetadataError('Cannot upload files to the item', code=404)
        index = await self._get_last_index_for(path)
        default_provider, index_folder = await self.get_index_folder(index.identifier, creates=True)

        logger.debug(f'Draft folder: {index_folder}')
        _, draft_path = path.split_draft_file_path()
        draft_parent_path, last_part = draft_path.split_path()
        if len(draft_parent_path.parts) == 1:
            parent_folder_metadata = index_folder
        else:
            draft_parent_path = draft_parent_path.as_file
            logger.debug(f'Target path: {draft_parent_path}')
            parent_folder_metadata = await self.get_draft_file_metadata(
                default_provider,
                index_folder,
                draft_parent_path,
            )
        logger.debug(f'Target folder: {parent_folder_metadata.path}')
        draft_path = await default_provider.validate_path(
            parent_folder_metadata.path + last_part.value
        )

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        stream.add_writer('sha1', streams.HashStreamWriter(hashlib.sha1))
        stream.add_writer('sha256', streams.HashStreamWriter(hashlib.sha256))
        stream.add_writer('sha512', streams.HashStreamWriter(hashlib.sha512))

        metadata, created = await default_provider.upload(
            stream, draft_path, **kwargs
        )
        return WEKODraftFileMetadata(self.index_id, metadata, index_folder, index), created

    async def delete(self, path, confirm_delete=0, **kwargs):
        if not path.is_draft_file:
            raise exceptions.MetadataError('Unsupported operation', code=400)
        index = await self._get_last_index_for(path)
        default_provider, index_folder = await self.get_index_folder(index.identifier)
        if index_folder is None:
            raise exceptions.MetadataError('Unexpected path', code=404)
        _, last_path = path.split_draft_file_path()
        file_metadata = await self.get_draft_file_metadata(
            default_provider,
            index_folder,
            last_path.as_file,
        )
        file_path = await default_provider.validate_path(file_metadata.path)
        metadata = await default_provider.delete(file_path, confirm_delete=confirm_delete, **kwargs)
        if metadata is None:
            return None
        return self._wrap_draft_metadata(metadata, index_folder, index)

    async def download(self, path, revision=None, range=None, **kwargs):
        index = await self._get_last_index_for(path)
        if path.is_draft_file:
            # Draft file or item file
            default_provider, index_folder = await self.get_index_folder(index.identifier)
            if index_folder is None:
                raise exceptions.MetadataError('Unexpected path', code=404)
            _, last_path = path.split_draft_file_path()
            file_metadata = await self.get_draft_file_metadata(default_provider, index_folder, last_path)
            file_path = await default_provider.validate_path(file_metadata.path)
            return await default_provider.download(file_path, range=range, **kwargs)
        if not path.is_item_file:
            raise exceptions.MetadataError('Illegal parts', code=400)
        # File of Item
        item = await self._get_last_item_for(index, path)
        last_part = path.parts[-1]
        files = [f for f in item.files if f.filename == last_part.identifier_value]
        if len(files) == 0:
            raise exceptions.MetadataError('File not found', code=404)
        file = files[0]
        resp = await self.make_request(
            'GET',
            file.download_url,
            range=range,
            headers=self.client.request_headers(),
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        return streams.ResponseStreamReader(resp)

    async def metadata(self, path, version=None, **kwargs):
        """
        :param str version:

            - 'latest' for draft files
            - 'latest-published' for published files
            - None for all data
        """
        if path.is_root:
            # Root Index
            index = await self.client.get_index_by_id(str(self.index_id))
            return await self.get_index_metadata(index)
        if path.is_index:
            # Sub Index
            index = await self.client.get_index_by_id(path.parts[-1].identifier_value)
            return await self.get_index_metadata(index)
        index = await self._get_last_index_for(path)
        if path.is_item:
            # Item
            item = await index.get_item_by_id(path.parts[-1].identifier_value)
            return self.get_item_metadata(index, item)
        if path.is_item_file:
            # File of Item
            item = await self._get_last_item_for(index, path)
            files = [f for f in item.files if f.filename == path.parts[-1].identifier_value]
            if len(files) == 0:
                raise exceptions.MetadataError('Illegal parts', code=400)
            return WEKOFileMetadata(self.index_id, files[0], item, index)
        if not path.is_draft_file:
            raise exceptions.MetadataError('unsupported', code=400)
        # Draft of Index
        default_provider, index_folder = await self.get_index_folder(index.identifier)
        if index_folder is None:
            raise exceptions.MetadataError('Unexpected path', code=404)
        _, last_path = path.split_draft_file_path()
        file_metadata = await self.get_draft_file_metadata(default_provider, index_folder, last_path)
        return self._wrap_draft_metadata(file_metadata, index_folder, index)

    async def revisions(self, path, **kwargs):
        """Get past versions of the request file.

        :param str path: The path to a key
        :rtype list:
        """
        return []

    async def get_index_metadata(self, index):
        ritems = [
            WEKOItemMetadata(self.index_id, self.client, item, index, self.NAME)
            for item in await index.get_items()
        ]
        rindices = [WEKOIndexMetadata(self.index_id, self.client, i) for i in index.children]
        default_provider, index_folder = await self.get_index_folder(index.identifier)
        rdrafts = []
        if index_folder is not None:
            index_folder_path = await default_provider.validate_path(index_folder.path)
            index_folder_metadata = await default_provider.metadata(index_folder_path)
            for f in index_folder_metadata:
                rdrafts.append(self._wrap_draft_metadata(f, index_folder, index))
        return rindices + ritems + rdrafts

    def get_item_metadata(self, index, item):
        return [WEKOFileMetadata(self.index_id, f, item, index) for f in item.files]

    async def get_draft_folder(self, creates=False):
        default_provider = self.make_default_provider()
        root_folder_path = await default_provider.validate_path('/')
        root_folder_metadata = await default_provider.metadata(
            root_folder_path
        )
        draft_folders = [child
                         for child in root_folder_metadata
                         if child.name == f'.{self.NAME}']
        if len(draft_folders) > 0:
            return default_provider, draft_folders[0]
        if not creates:
            return default_provider, None
        # Create draft folder
        draft_folder_path = await default_provider.validate_path(f'/.{self.NAME}/')
        folder = await default_provider.create_folder(draft_folder_path)
        return default_provider, folder

    async def get_index_folder(self, index_id, creates=False):
        default_provider, draft_folder = await self.get_draft_folder(creates=creates)
        if draft_folder is None:
            return default_provider, None
        draft_folder_path = await default_provider.validate_path(draft_folder.path)
        draft_folder_metadata = await default_provider.metadata(
            draft_folder_path
        )
        index_folders = [child
                         for child in draft_folder_metadata
                         if child.name == index_id]
        if len(index_folders) > 0:
            return default_provider, index_folders[0]
        if not creates:
            return default_provider, None
        # Create index folder
        index_folder_path = await default_provider.validate_path(f'{draft_folder.path}{index_id}/')
        index_folder = await default_provider.create_folder(index_folder_path)
        return default_provider, index_folder

    async def get_draft_file_metadata(self, default_provider, index_folder, draft_path):
        logger.debug(f'Draft File: index_folder={index_folder.materialized_path}, draft_path={draft_path.path}')
        draft_parent_path, last_part = draft_path.split_path()
        if last_part is None:
            # root
            index_folder_path = await default_provider.validate_path(index_folder.path)
            return await default_provider.metadata(index_folder_path)
        draft_parent_metadata = await self.get_draft_file_metadata(
            default_provider,
            index_folder,
            draft_parent_path,
        )
        if hasattr(draft_parent_metadata, 'kind') and draft_parent_metadata.kind == 'file':
            raise exceptions.MetadataError('invalid', code=400)
        draft_files = [child
                       for child in draft_parent_metadata
                       if child.name == last_part.materialized]
        if len(draft_files) == 0:
            raise exceptions.MetadataError('File not found', code=404)
        if not draft_path.is_dir:
            return draft_files[0]
        draft_folder_path = await default_provider.validate_path(draft_files[0].path)
        return await default_provider.metadata(draft_folder_path)

    async def _get_last_index_for(self, path):
        index_parts = [p for p in path.parts if p.is_index]
        if len(index_parts) > 0:
            # Sub index
            return await self.client.get_index_by_id(index_parts[-1].identifier_value)
        return await self.client.get_index_by_id(str(self.index_id))

    async def _get_last_item_for(self, index, path):
        item_parts = [p for p in path.parts if p.is_item]
        if len(item_parts) == 0:
            raise exceptions.MetadataError('Illegal parts', code=400)
        return await index.get_item_by_id(item_parts[-1].identifier_value)

    def _wrap_draft_metadata(self, file_metadata, index_folder, index):
        if isinstance(file_metadata, (list, tuple)):
            return [self._wrap_draft_metadata(f, index_folder, index) for f in file_metadata]
        if file_metadata.kind == 'folder':
            return WEKODraftFolderMetadata(self.index_id, file_metadata, index_folder, index)
        return WEKODraftFileMetadata(self.index_id, file_metadata, index_folder, index)
