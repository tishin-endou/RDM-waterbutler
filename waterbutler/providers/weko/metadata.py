import re
from typing import List, Union
from waterbutler.core import metadata
from .client import Index, Item


ITEM_PREFIX = 'weko:'


def _get_item_file_id(item: Item):
    return 'item{}'.format(item.identifier)


def parse_item_file_id(part):
    m = re.match(r'^' + ITEM_PREFIX + r'item([0-9]+)$', part)
    if not m:
        return None
    return m.group(1)


def _get_parent_for_non_root_index(root_index_id: str, target: Index):
    if target is None:
        return None
    if target.identifier == root_index_id:
        return None
    return target.parent


def _index_to_path_parts(root_index_id: str, target: Index) -> List[Index]:
    parent_index = _get_parent_for_non_root_index(root_index_id, target)
    if parent_index is None:
        return []
    parts = [target]
    target = parent_index
    while target is not None \
            and _get_parent_for_non_root_index(root_index_id, target) is not None:
        parts.insert(0, target)
        target = _get_parent_for_non_root_index(root_index_id, target)
    return parts


def _index_to_path(root_index_id: str, target: Index) -> str:
    r = '/'.join([
        ITEM_PREFIX + part.identifier
        for part in _index_to_path_parts(root_index_id, target)
    ])
    if len(r) == 0:
        return r
    return r + '/'


def _index_to_materialized_path(root_index_id: str, target: Index) -> str:
    r = '/'.join([
        part.title
        for part in _index_to_path_parts(root_index_id, target)
    ])
    if len(r) == 0:
        return r
    return r + '/'


class BaseWEKOMetadata(metadata.BaseMetadata):
    @property
    def provider(self):
        return 'weko'

    @property
    def created_utc(self):
        return None


class WEKOFileMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index_identifier: str = None
    index_path: str = None
    index_materialized_path: str = None
    item_file_id: str = None
    item_title: str = None

    def __init__(self, root_index_id: str, file, item: Item, index: Index):
        super().__init__({
            'filename': file.filename,
            'format': file.format,
            'version_id': file.version_id
        })
        self.index_identifier = index.identifier
        self.index_path = _index_to_path(root_index_id, index)
        self.index_materialized_path = _index_to_materialized_path(root_index_id, index)
        self.item_file_id = _get_item_file_id(item)
        self.item_title = item.primary_title

    @property
    def file_id(self):
        return self.raw['filename']

    @property
    def name(self):
        return self.raw['filename']

    @property
    def content_type(self):
        return self.raw['format']

    @property
    def identifier(self):
        return self.raw['filename']

    @property
    def path(self):
        return '/' + self.index_path + ITEM_PREFIX + self.item_file_id + '/' + self.identifier

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path + self.item_title + '/' + self.identifier

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.raw['version_id']

    @property
    def extra(self):
        return {
            'weko': 'file',
            'itemId': self.item_file_id,
            'metadata': {
                'can_edit': False,
                'can_register': False,
            },
        }


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    file_id: str = None
    provider_name: str = None
    index_identifier: str = None
    index_path: str = None
    index_materialized_path: str = None
    item_identifier: Union[str, int] = None
    weko_web_url = None

    def __init__(self, root_index_id: str, client, raw: Item, index: Index, provider_name: str):
        super().__init__({
            'primary_title': raw.primary_title,
            'metadata': raw.raw['metadata'],
        })
        self.file_id = _get_item_file_id(raw)
        self.item_identifier = raw.identifier
        self.index_identifier = index.identifier
        self.index_path = _index_to_path(root_index_id, index)
        self.index_materialized_path = _index_to_materialized_path(root_index_id, index)
        self.provider_name = provider_name
        self.weko_web_url = client.get_item_records_url(str(raw.identifier))

    @property
    def name(self):
        return self.raw['primary_title']

    @property
    def content_type(self):
        return None

    @property
    def identifier(self):
        return ITEM_PREFIX + self.file_id

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path + self.name + '/'

    @property
    def path(self):
        return '/' + self.index_path + self.identifier + '/'

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.file_id

    @property
    def item_title(self):
        if '_item_metadata' not in self.raw['metadata']:
            return [
                {
                    'subitem_title': self.raw['primary_title'],
                }
            ]
        _item_metadata = self.raw['metadata']['_item_metadata']
        items = [i
                 for i in _item_metadata.values()
                 if isinstance(i, dict) and 'attribute_value_mlt' in i and
                     all(['subitem_title' in v for v in i['attribute_value_mlt']])]
        if len(items) == 0:
            return [
                {
                    'subitem_title': self.raw['primary_title'],
                }
            ]
        return items[0]['attribute_value_mlt']

    @property
    def extra(self):
        return {
            'weko': 'item',
            'weko_web_url': self.weko_web_url,
            'fileId': self.file_id,
            'item_title': self.item_title,
            'metadata': {
                'can_edit': False,
                'can_register': False,
            },
        }


class WEKOIndexMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    index_identifier: str = None
    index_path: str = None
    index_materialized_path: str = None
    weko_web_url = None

    def __init__(self, root_index_id: str, client, raw: Index):
        super().__init__({
            'title': raw.title,
        })
        self.index_identifier = raw.identifier
        self.index_path = _index_to_path(root_index_id, raw)
        self.index_materialized_path = _index_to_materialized_path(root_index_id, raw)
        self.weko_web_url = client.get_index_items_url(raw.identifier)

    @property
    def name(self):
        return self.raw['title']

    @property
    def identifier(self):
        return ITEM_PREFIX + self.index_identifier

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path

    @property
    def path(self):
        return '/' + self.index_path

    @property
    def extra(self):
        return {
            'weko': 'index',
            'weko_web_url': self.weko_web_url,
            'indexId': self.index_identifier,
            'metadata': {
                'can_edit': False,
                'can_register': False,
            },
        }


class BaseWEKODraftMetadata(BaseWEKOMetadata):
    index_identifier: str = None
    index_path: str = None
    index_materialized_path: str = None
    index_folder = None

    def __init__(self, root_index_id: str, file, index_folder, index: Index):
        super().__init__(file)
        self.index_identifier = index.identifier
        self.index_path = _index_to_path(root_index_id, index)
        self.index_materialized_path = _index_to_materialized_path(root_index_id, index)
        self.index_folder = index_folder

    @property
    def extra(self):
        r = {
            'weko': 'draft',
            'index': self.index_identifier,
            'metadata': {
                'can_edit': True,
                'can_register': False,
            },
            'source': {
                'provider': self.raw.provider,
                'path': self.raw.path,
                'materialized_path': self.raw.materialized_path,
            },
        }
        r.update(self.raw.extra)
        return r

    @property
    def path(self):
        return '/' + self.index_path + self._relative_path

    @property
    def materialized_path(self):
        return '/' + self.index_materialized_path + self._relative_path

    @property
    def _relative_path(self):
        base_path = self.index_folder.materialized_path
        item_path = self.raw.materialized_path
        if not item_path.startswith(base_path):
            raise ValueError(f'Unexpected path: base_path={base_path}, item_path={item_path}')
        return item_path[len(base_path):]


class WEKODraftFolderMetadata(BaseWEKODraftMetadata, metadata.BaseFolderMetadata):
    @property
    def name(self):
        return self.raw.name

    @property
    def _relative_path(self):
        r = super(WEKODraftFolderMetadata, self)._relative_path
        if r.endswith('/'):
            return r
        # Ensure that the return value of create_folder is also interpreted as a folder
        return r + '/'


class WEKODraftFileMetadata(BaseWEKODraftMetadata, metadata.BaseFileMetadata):
    @property
    def name(self):
        return self.raw.name

    @property
    def content_type(self):
        return self.raw.content_type

    @property
    def identifier(self):
        return self.raw.name

    @property
    def size(self):
        return self.raw.size

    @property
    def modified(self):
        return self.raw.modified

    @property
    def etag(self):
        return self.raw.etag
