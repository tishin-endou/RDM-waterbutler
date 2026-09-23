import os
import io
import xml
import json
import time
import base64
import asyncio
import hashlib
import aiohttp
import aiohttpretty
from http import client
from urllib import parse
from unittest import mock

import pytest
# from boto.compat import BytesIO
# from boto.utils import compute_md5

from waterbutler.providers.s3 import S3Provider
from waterbutler.core.path import WaterButlerPath
from waterbutler.core import streams, metadata, exceptions
from waterbutler.providers.s3 import settings as pd_settings

from tests.utils import MockCoroutine
from tests.providers.s3.fixtures import (auth,
                                         settings,
                                         credentials,
                                         file_content,
                                         folder_metadata,
                                         folder_metadata,
                                         version_metadata,
                                         create_session_resp,
                                         folder_and_contents,
                                         complete_upload_resp,
                                         file_header_metadata,
                                         file_metadata_object,
                                         folder_item_metadata,
                                         generic_http_403_resp,
                                         generic_http_404_resp,
                                         list_parts_resp_empty,
                                         folder_empty_metadata,
                                         single_version_metadata,
                                         revision_metadata_object,
                                         upload_parts_headers_list,
                                         list_parts_resp_not_empty,
                                         folder_key_metadata_object,
                                         folder_single_item_metadata,
                                         file_metadata_headers_object,
                                         )


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    prov = S3Provider(auth, credentials, settings)
    prov._check_region = MockCoroutine()

    async def _gen_presigned(path, method='head_object', query_parameters=None, default_params=True):
        clean = path.lstrip('/')
        if clean:
            return f'https://that-kerning.s3.amazonaws.com/{clean}'
        return 'https://that-kerning.s3.amazonaws.com/'

    prov.generate_generic_presigned_url = _gen_presigned

    async def _check_key(path, expects=(200,), query_parameters=None):
        url = f'https://that-kerning.s3.amazonaws.com/{path}'
        return await prov.make_request('HEAD', url, expects=expects, throws=exceptions.MetadataError)

    prov.check_key_existence = _check_key

    return prov


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


def location_response(location):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        '{}</LocationConstraint>'
    ).format(location)


def list_objects_response(keys, truncated=False):
    response = '''<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>bucket</Name>
        <Prefix/>
        <Marker/>
        <MaxKeys>1000</MaxKeys>'''

    response += '<IsTruncated>' + str(truncated).lower() + '</IsTruncated>'
    response += ''.join(map(
        lambda x: f'<Contents><Key>{x}</Key></Contents>',
        keys
    ))

    response += '</ListBucketResult>'

    return response.encode('utf-8')


def bulk_delete_body(keys):
    payload = '<?xml version="1.0" encoding="UTF-8"?>'
    payload += '<Delete>'
    payload += ''.join(map(
        lambda x: f'<Object><Key>{x}</Key></Object>',
        keys
    ))
    payload += '</Delete>'
    payload = payload.encode('utf-8')

    md5 = base64.b64encode(hashlib.md5(payload).digest())
    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5.decode('ascii'),
        'Content-Type': 'text/xml',
    }

    return (payload, headers)


class MockS3Response:

    text = MockCoroutine(return_value='''<?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <IsTruncated>false</IsTruncated>
        </ListVersionsResult>
    ''')


def list_upload_chunks_body(parts_metadata):
    payload = b'''<?xml version="1.0" encoding="UTF-8"?>
        <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Bucket>example-bucket</Bucket>
            <Key>example-object</Key>
            <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
            <Initiator>
                <ID>arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx</ID>
                <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-b288870f11xx</DisplayName>
            </Initiator>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>someName</DisplayName>
            </Owner>
            <StorageClass>STANDARD</StorageClass>
            <PartNumberMarker>1</PartNumberMarker>
            <NextPartNumberMarker>3</NextPartNumberMarker>
            <MaxParts>2</MaxParts>
            <IsTruncated>false</IsTruncated>
            <Part>
                <PartNumber>2</PartNumber>
                <LastModified>2010-11-10T20:48:34.000Z</LastModified>
                <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
                <Size>10485760</Size>
            </Part>
            <Part>
                <PartNumber>3</PartNumber>
                <LastModified>2010-11-10T20:48:33.000Z</LastModified>
                <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
                <Size>10485760</Size>
            </Part>
        </ListPartsResult>
    '''

    # md5 = compute_md5(BytesIO(payload))
    # md5 = compute_md5(payload)
    md5 = hashlib.md5(payload)

    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5.hexdigest(),
        'Content-Type': 'text/xml',
    }

    return payload, headers


def build_folder_params(path):
    return {'prefix': path.path, 'delimiter': '/'}


BUCKET_URL = 'https://that-kerning.s3.amazonaws.com/'


def install_query_encoding_presigned_url(provider):
    """Replace the ``provider`` fixture's presigned-URL stub with one that encodes the query
    parameters into the URL, which is what a real presigned URL does.  The default stub throws
    the parameters away, so every page of a paged listing would collapse onto a single URL and
    aiohttpretty would be unable to tell one page request from the next.

    :return: the list of query-parameter dicts, one per call, in call order
    """
    calls = []

    async def _gen_presigned(path, method='head_object', query_parameters=None,
                             default_params=True):
        params = dict(query_parameters or {})
        calls.append(params)
        url = BUCKET_URL + (path or '').lstrip('/')
        if params:
            url += '?' + parse.urlencode(sorted(params.items()))
        return url

    provider.generate_generic_presigned_url = _gen_presigned
    return calls


def versions_url(**params):
    """The URL that :func:`install_query_encoding_presigned_url` produces for a
    ``list_object_versions`` call made with ``params``."""
    return BUCKET_URL + '?' + parse.urlencode(sorted(params.items()))


def objects_url(**params):
    """The URL that :func:`install_query_encoding_presigned_url` produces for a
    ``list_objects_v2`` call made with ``params``."""
    return BUCKET_URL + '?' + parse.urlencode(sorted(params.items()))


def list_objects_v2_response(keys, is_truncated=False, next_continuation_token=None):
    """Build a ListObjectsV2 response body listing ``keys``."""
    body = '<?xml version="1.0" encoding="UTF-8"?>'
    body += '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    body += '<Name>that-kerning</Name>'
    body += '<MaxKeys>1000</MaxKeys>'
    body += '<IsTruncated>{}</IsTruncated>'.format('true' if is_truncated else 'false')
    if next_continuation_token is not None:
        body += f'<NextContinuationToken>{next_continuation_token}</NextContinuationToken>'
    for key in keys:
        body += ('<Contents>'
                 f'<Key>{key}</Key>'
                 '<LastModified>2016-02-05T14:28:50.000Z</LastModified>'
                 '<ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>'
                 '<Size>1234</Size>'
                 '<StorageClass>STANDARD</StorageClass>'
                 '</Contents>')
    body += '</ListBucketResult>'
    return body.encode('utf-8')


def list_versions_response(versions=(), delete_markers=(), is_truncated=False,
                           next_key_marker=None, next_version_id_marker=None):
    """Build a ListObjectVersions response body.

    ``versions`` and ``delete_markers`` are iterables of ``(key, version_id)`` pairs.
    """
    body = '<?xml version="1.0" encoding="UTF-8"?>'
    body += '<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    body += '<Name>that-kerning</Name>'
    body += '<IsTruncated>{}</IsTruncated>'.format('true' if is_truncated else 'false')
    if next_key_marker is not None:
        body += f'<NextKeyMarker>{next_key_marker}</NextKeyMarker>'
    if next_version_id_marker is not None:
        body += f'<NextVersionIdMarker>{next_version_id_marker}</NextVersionIdMarker>'
    for key, version_id in versions:
        body += ('<Version>'
                 f'<Key>{key}</Key>'
                 f'<VersionId>{version_id}</VersionId>'
                 '<IsLatest>false</IsLatest>'
                 '<LastModified>2016-02-05T14:28:50.000Z</LastModified>'
                 '<ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>'
                 '<Size>1234</Size>'
                 '<StorageClass>STANDARD</StorageClass>'
                 '</Version>')
    for key, version_id in delete_markers:
        body += ('<DeleteMarker>'
                 f'<Key>{key}</Key>'
                 f'<VersionId>{version_id}</VersionId>'
                 '<IsLatest>true</IsLatest>'
                 '<LastModified>2016-02-05T14:28:50.000Z</LastModified>'
                 '</DeleteMarker>')
    body += '</ListVersionsResult>'
    return body.encode('utf-8')


class _AsyncClientCtx:
    """``session.create_client()`` returns an async context manager, and ``mock.AsyncMock``
    needs Python 3.8+."""

    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *args):
        return False


def patch_aiobotocore_client(**methods):
    """Patch the aiobotocore session the provider builds its clients from, so that
    ``create_client()`` yields a mock client with ``methods`` bound on it.  This injects at the
    aiobotocore boundary only; the provider method under test still runs for real.

    :return: ``(patcher, client)`` -- use the patcher as a context manager
    """
    client = mock.Mock()
    for name, coroutine in methods.items():
        setattr(client, name, coroutine)
    session = mock.Mock()
    session.create_client = mock.Mock(return_value=_AsyncClientCtx(client))
    patcher = mock.patch('waterbutler.providers.s3.provider.get_session', return_value=session)
    return patcher, client


class TestRegionDetection:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("region_name,expected_region", [
        # ('',               's3.amazonaws.com'),
        ('EU',             'eu-west-1'),
        ('us-east-2',      'us-east-2'),
        ('us-west-1',      'us-west-1'),
        ('us-west-2',      'us-west-2'),
        ('ca-central-1',   'ca-central-1'),
        ('eu-central-1',   'eu-central-1'),
        ('eu-west-2',      'eu-west-2'),
        ('ap-northeast-1', 'ap-northeast-1'),
        ('ap-northeast-2', 'ap-northeast-2'),
        ('ap-south-1',     'ap-south-1'),
        ('ap-southeast-1', 'ap-southeast-1'),
        ('ap-southeast-2', 'ap-southeast-2'),
        ('sa-east-1',      'sa-east-1'),
    ])
    async def test_region_host(self, auth, credentials, settings, region_name, expected_region, mock_time):
        provider = S3Provider(auth, credentials, settings)
        region_url = await provider.generate_generic_presigned_url(
            '', method='get_bucket_location', query_parameters={'Bucket': settings['bucket']},  default_params=False
        )
        aiohttpretty.register_uri('GET', region_url, status=200, body=location_response(region_name),
                                  match_querystring=False)
        async def mock_get_location():
            return await provider.make_request('GET', region_url, expects=(200,), throws=exceptions.MetadataError)
        provider.get_s3_bucket_object_location = mock_get_location
        await provider._check_region()
        assert provider.region == expected_region
        # provider = S3Provider(auth, credentials, settings)
        # await provider._check_region()
        # await provider._check_region()
        # res = await provider._get_bucket_region()
        # # region_url = provider.bucket.generate_url(
        # #     100,
        # #     'GET',
        # #     query_parameters={'location': ''},
        # # )
        # region_url = 'https://s3.amazonaws.com/that-kerning?location=&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=Dont%20dead%2F20250526%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20250526T134653Z&X-Amz-Expires=100&X-Amz-SignedHeaders=host&X-Amz-Signature=80f8426c4fc6d0af68bd3e52a553c9e4d838144b9a70600aff507f70056696f1 '
        # aiohttpretty.register_uri('GET',
        #                           region_url,
        #                           status=200,
        #                           body=location_response(region_name))
        #
        # await provider._check_region()
        # assert provider.connection.host == host


class TestInitialization:

    @pytest.mark.parametrize(('provider_settings', 'expected_base_folder'), [
        ({'id': 'that-kerning:/my-subfolder/'}, 'my-subfolder/'),
        ({'id': 'that-kerning'}, ''),
        ({'id': None}, ''),
    ])
    def test_base_folder_parsing(self, auth, credentials, settings, provider_settings, expected_base_folder):
        provider_settings = dict(settings, **provider_settings)
        if provider_settings['id'] is None:
            del provider_settings['id']

        provider = S3Provider(auth, credentials, provider_settings)

        assert provider.base_folder == expected_base_folder


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_header_metadata, mock_time):
        file_path = 'foobah'

        root_listing_url = 'https://that-kerning.s3.amazonaws.com/my-subfolder/'
        file_head_url = f'https://that-kerning.s3.amazonaws.com/my-subfolder/{file_path}'
        bucket_listing_url = 'https://that-kerning.s3.amazonaws.com/'

        aiohttpretty.register_uri(
            'GET',
            root_listing_url,
            body=b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><Prefix>my-subfolder/</Prefix><IsTruncated>false</IsTruncated></ListBucketResult>',
            headers={'Content-Type': 'application/xml'},
            match_querystring=False,
        )
        aiohttpretty.register_uri(
            'HEAD',
            file_head_url,
            headers=file_header_metadata,
            match_querystring=False,
        )

        assert WaterButlerPath('/my-subfolder/', prepend=None) == await provider.validate_v1_path('/')

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        wb_path_v0 = await provider.validate_path('/' + file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file_with_subfolder(self, provider, file_header_metadata, mock_time):
        file_path = '/foobah'

        listing_url = 'https://that-kerning.s3.amazonaws.com/my-subfolder/'
        file_head_url = f'https://that-kerning.s3.amazonaws.com/my-subfolder{file_path}'

        aiohttpretty.register_uri(
            'GET',
            listing_url,
            body=b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><Prefix>my-subfolder/</Prefix><IsTruncated>false</IsTruncated></ListBucketResult>',
            headers={'Content-Type': 'application/xml'},
            match_querystring=False,
        )
        aiohttpretty.register_uri(
            'HEAD',
            file_head_url,
            headers=file_header_metadata,
            match_querystring=False,
        )

        assert WaterButlerPath('/my-subfolder/') == await provider.validate_v1_path('/')
        wb_path_v1 = await provider.validate_v1_path(file_path)
        wb_path_v0 = await provider.validate_path(file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata, mock_time):
        folder_path = '/Photos'

        listing_url = 'https://that-kerning.s3.amazonaws.com/my-subfolder/Photos/'

        aiohttpretty.register_uri(
            'GET',
            listing_url,
            body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'},
            match_querystring=False,
        )
        aiohttpretty.register_uri(
            'HEAD',
            f'https://that-kerning.s3.amazonaws.com/my-subfolder{folder_path}',
            status=404,
            match_querystring=False,
        )

        wb_path_v1 = await provider.validate_v1_path(folder_path + '/')
        wb_path_v0 = await provider.validate_path(folder_path + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_normal_name(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/path.txt')
        assert path.name == 'path.txt'
        assert path.parent.name == 'a'
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_folder(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert path.name == 'folder'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_subfolder(self, provider, mock_time):
        path = await provider.validate_path('/')
        assert path.name == 'my-subfolder'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True,
                                  match_querystring=False)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'de', auto_length=True, status=206,
                                  match_querystring=False)

        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        assert content == b'de'
        assert aiohttpretty.has_call(method='GET', uri=url, headers={'Range': 'bytes=0-1'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_version(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True,
                                  match_querystring=False)

        result = await provider.download(path, revision='someversion')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("display_name_arg,expected_name", [
        ('meow.txt', 'meow.txt'),
        ('',         'muhtriangle'),
        (None,       'muhtriangle'),
    ])
    async def test_download_with_display_name(self, provider, mock_time, display_name_arg,
                                              expected_name):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True,
                                  match_querystring=False)

        result = await provider.download(path, display_name=display_name_arg)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, status=404, match_querystring=False)

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder_400s(self, provider, mock_time):
        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(WaterButlerPath('/cool/folder/mom/'))
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_to_subfolder_as_root(self,
                                               provider,
                                               file_content,
                                               file_stream,
                                               file_header_metadata,
                                               mock_time
                                               ):

        provider.settings['id'] = 'the-bucket:/my-subfolder/'
        path = WaterButlerPath('/my-subfolder/foobah')

        content_md5 = hashlib.md5(file_content).hexdigest()

        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata,
                                  match_querystring=False)
        header = {'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=201, headers=header,
                                  match_querystring=False)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.path == '/foobah'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self,
                                 provider,
                                 file_content,
                                 file_stream,
                                 file_header_metadata,
                                 mock_time):

        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata,
                                  match_querystring=False)
        header = {'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=201, headers=header,
                                  match_querystring=False)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_encrypted(self,
                                    provider,
                                    file_content,
                                    file_stream,
                                    file_header_metadata,
                                    mock_time):

        # Set trigger for encrypt_key=True in s3.provider.upload
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
            match_querystring=False,
        )
        headers={'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers,
                                  match_querystring=False)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.extra['encryption'] == 'AES256'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_chunked(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        provider._chunked_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._chunked_upload.assert_called_with(file_stream, path)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_contiguous(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 10
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        provider._contiguous_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._contiguous_upload.assert_called_with(file_stream, path)

        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_no_encryption(self, provider,
                                                                      create_session_resp,
                                                                      mock_time):
        path = WaterButlerPath('/foobah')
        init_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200,
                                  match_querystring=False)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_with_encryption(self, provider,
                                                                        create_session_resp,
                                                                        mock_time):
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah')
        init_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200,
                                  match_querystring=False)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts(self, provider, file_stream,
                                               upload_parts_headers_list):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        parts_metadata = await provider._upload_parts(file_stream, path, upload_id)

        assert provider._upload_part.call_count == 3
        assert len(parts_metadata) == 3
        assert parts_metadata == side_effect

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts_remainder(self, provider,
                                                         upload_parts_headers_list):

        file_stream = streams.StringStream('abcdefghijklmnopqrst')
        assert file_stream.size == 20
        provider.CHUNK_SIZE = 9

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        parts_metadata = await provider._upload_parts(file_stream, path, upload_id)

        assert provider._upload_part.call_count == 3
        provider._upload_part.assert_has_calls([
            mock.call(file_stream, path, upload_id, 1, 9),
            mock.call(file_stream, path, upload_id, 2, 9),
            mock.call(file_stream, path, upload_id, 3, 2),
        ])
        assert len(parts_metadata) == 3
        assert parts_metadata == side_effect

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_part(self, provider, file_stream,
                                              upload_parts_headers_list,
                                              mock_time):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        chunk_number = 1
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        upload_part_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        # aiohttp resp headers use upper case
        part_headers = json.loads(upload_parts_headers_list).get('headers_list')[0]
        part_headers = {k.upper(): v for k, v in part_headers.items()}
        aiohttpretty.register_uri('PUT', upload_part_url, status=200, headers=part_headers,
                                  params={'partNumber': str(chunk_number), 'uploadId': upload_id})

        part_metadata = await provider._upload_part(file_stream, path, upload_id, chunk_number,
                                                    provider.CHUNK_SIZE)

        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url,
                                     params={'partNumber': str(chunk_number), 'uploadId': upload_id})
        assert part_headers == part_metadata

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_multipart_upload(self, provider,
                                                            upload_parts_headers_list,
                                                            complete_upload_resp, mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        payload = '<?xml version="1.0" encoding="UTF-8"?>'
        payload += '<CompleteMultipartUpload>'
        # aiohttp resp headers are upper case
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]
        for i, part in enumerate(headers_list):
            payload += '<Part>'
            payload += f'<PartNumber>{i+1}</PartNumber>'  # part number must be >= 1
            payload += '<ETag>{}</ETag>'.format(xml.sax.saxutils.escape(part['ETAG']))
            payload += '</Part>'
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')

        complete_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        aiohttpretty.register_uri(
            'POST',
            complete_url,
            status=200,
            body=complete_upload_resp,
            match_querystring=False,
        )

        await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_session_deleted(self, provider, generic_http_404_resp,
                                                        mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', abort_url, status=204, match_querystring=False)
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404,
                                  match_querystring=False)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_empty(self, provider, list_parts_resp_empty,
                                                   mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', abort_url, status=204, match_querystring=False)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200,
                                  match_querystring=False)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_not_empty(self,
                                                       provider,
                                                       list_parts_resp_not_empty,
                                                       mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('DELETE', abort_url, status=204, match_querystring=False)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200,
                                  match_querystring=False)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_session_not_found(self,
                                                          provider,
                                                          generic_http_404_resp,
                                                          mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404,
                                  match_querystring=False)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_empty_list(self,
                                                   provider,
                                                   list_parts_resp_empty,
                                                   mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200,
                                  match_querystring=False)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_list_not_empty(self,
                                                       provider,
                                                       list_parts_resp_not_empty,
                                                       mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200,
                                  match_querystring=False)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, mock_time):
        """GRDM: deleting a file purges every version of the key, not only the current one.

        A plain DELETE only writes a new delete marker, so the old versions keep occupying the
        user's quota forever.
        """
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-file'),
            body=list_versions_response(
                versions=[('some-file', 'version-two'), ('some-file', 'version-one')],
                delete_markers=[('some-file', 'marker-one')],
            ),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'some-file', 'VersionId': 'version-two'},
                                {'Key': 'some-file', 'VersionId': 'version-one'},
                                {'Key': 'some-file', 'VersionId': 'marker-one'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_leaves_keys_that_merely_share_the_prefix(self, provider, mock_time):
        """Prefix= is a prefix match, so 'some-file.bak' comes back alongside 'some-file'."""
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-file'),
            body=list_versions_response(
                versions=[('some-file', 'version-one'), ('some-file.bak', 'version-bak')],
            ),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'some-file', 'VersionId': 'version-one'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_on_bucket_without_versioning(self, provider, mock_time):
        """V-3: a bucket with versioning disabled reports the single live object with the
        literal version id 'null', which DeleteObjects accepts verbatim."""
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-file'),
            body=list_versions_response(versions=[('some-file', 'null')]),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'some-file', 'VersionId': 'null'}], 'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_with_no_versions_makes_no_delete_call(self, provider, mock_time):
        """DeleteObjects rejects an empty object list, so there is nothing to send."""
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-file'),
            body=list_versions_response(),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        assert s3_client.delete_objects.called is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_file_partial_failure_raises(self, provider, mock_time):
        """V-4: DeleteObjects reports per-object failures in the 200 body.  Fail closed, and
        name the objects that survived so the caller can retry them."""
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-file'),
            body=list_versions_response(
                versions=[('some-file', 'version-two'), ('some-file', 'version-one')]),
            status=200,
        )

        delete_result = {
            'Deleted': [{'Key': 'some-file', 'VersionId': 'version-two'}],
            'Errors': [{'Key': 'some-file', 'VersionId': 'version-one',
                        'Code': 'AccessDenied', 'Message': 'Access Denied'}],
        }
        patcher, _ = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value=delete_result))
        with patcher:
            with pytest.raises(exceptions.DeleteError) as exc_info:
                await provider.delete(path)

        message = exc_info.value.message
        assert 'some-file' in message
        assert 'version-one' in message
        assert 'AccessDenied' in message
        # the survivors are what matters; a presigned URL in an error message is a credential leak
        assert 'X-Amz-Signature' not in message
        assert 'https://' not in message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('status', [403, 500])
    async def test_delete_file_versions_listing_http_error(self, provider, status, mock_time):
        """V-5: a failed version listing must surface as a DeleteError, not as whatever the
        listing helper happens to throw, and must not leak the raw S3 error document."""
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-file'),
            body=b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code>'
                 b'<Message>Access Denied</Message></Error>',
            status=status,
        )

        with pytest.raises(exceptions.DeleteError) as exc_info:
            await provider.delete(path)

        assert exc_info.value.code == status
        assert 'DownloadError' in exc_info.value.message
        assert '<Error>' not in exc_info.value.message
        assert 'Access Denied' not in exc_info.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('transport_error', [aiohttp.ClientError, asyncio.TimeoutError])
    async def test_delete_file_versions_listing_transport_error(self, provider, transport_error,
                                                                monkeypatch, mock_time):
        """V-5: transport failures are not WaterButlerErrors and would otherwise escape
        delete() unconverted."""
        path = WaterButlerPath('/some-file')
        install_query_encoding_presigned_url(provider)

        async def _fail(*args, **kwargs):
            raise transport_error()

        monkeypatch.setattr(aiohttp.ClientSession, '_request', _fail)

        with pytest.raises(exceptions.DeleteError) as exc_info:
            await provider.delete(path)

        assert transport_error.__name__ in exc_info.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_confirm_delete(self, provider, mock_time):
        path = WaterButlerPath('/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix=''),
            body=list_objects_v2_response(['some-folder/', 'some-folder/file.txt']),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            with pytest.raises(exceptions.DeleteError):
                await provider.delete(path)

            assert s3_client.delete_objects.called is False

            await provider.delete(path, confirm_delete=1)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'some-folder/'}, {'Key': 'some-folder/file.txt'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix='some-folder/'),
            body=list_objects_v2_response(['some-folder/', 'some-folder/my-image.jpg']),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'some-folder/'}, {'Key': 'some-folder/my-image.jpg'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_item_folder_delete(self, provider, mock_time):
        path = WaterButlerPath('/single-thing-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix='single-thing-folder/'),
            body=list_objects_v2_response(['single-thing-folder/item']),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'single-thing-folder/item'}], 'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_folder_delete(self, provider, mock_time):
        path = WaterButlerPath('/empty-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix='empty-folder/'),
            body=list_objects_v2_response([]),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        assert s3_client.delete_objects.called is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_large_folder_delete(self, provider, mock_time):
        """DeleteObjects takes at most 1000 objects per call."""
        path = WaterButlerPath('/some-folder/')
        install_query_encoding_presigned_url(provider)

        keys = [f'some-folder/file-{index:05d}' for index in range(1001)]
        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix='some-folder/'),
            body=list_objects_v2_response(keys),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        batches = [call[1]['Delete']['Objects'] for call in s3_client.delete_objects.call_args_list]
        assert [len(batch) for batch in batches] == [1000, 1]
        assert [entry['Key'] for batch in batches for entry in batch] == keys

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete_truncated_listing(self, provider, mock_time):
        """A folder holding more than one page of keys must be listed to the end before any of
        it is deleted, otherwise the tail of the folder silently survives."""
        path = WaterButlerPath('/large-folder/')
        install_query_encoding_presigned_url(provider)

        page_one_url = objects_url(Bucket='that-kerning', Prefix='large-folder/')
        page_two_url = objects_url(Bucket='that-kerning', Prefix='large-folder/',
                                   ContinuationToken='token-for-page-two')

        aiohttpretty.register_uri(
            'GET', page_one_url,
            body=list_objects_v2_response(['large-folder/file1.txt'], is_truncated=True,
                                          next_continuation_token='token-for-page-two'),
            status=200,
        )
        aiohttpretty.register_uri(
            'GET', page_two_url,
            body=list_objects_v2_response(['large-folder/file2.txt']),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=page_two_url)
        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'large-folder/file1.txt'},
                                {'Key': 'large-folder/file2.txt'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete_partial_failure_raises(self, provider, mock_time):
        """V-4, folder side: refusals reported inside the 200 body must not read as success."""
        path = WaterButlerPath('/error-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix='error-folder/'),
            body=list_objects_v2_response(['error-folder/file1.txt', 'error-folder/file2.txt']),
            status=200,
        )

        delete_result = {
            'Deleted': [{'Key': 'error-folder/file1.txt'}],
            'Errors': [{'Key': 'error-folder/file2.txt', 'Code': 'AccessDenied',
                        'Message': 'Access Denied'}],
        }
        patcher, _ = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value=delete_result))
        with patcher:
            with pytest.raises(exceptions.DeleteError) as exc_info:
                await provider.delete(path)

        assert 'error-folder/file2.txt' in exc_info.value.message
        assert 'AccessDenied' in exc_info.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete_listing_error(self, provider, mock_time):
        path = WaterButlerPath('/error-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', objects_url(Bucket='that-kerning', Prefix='error-folder/'),
            body=b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code></Error>',
            status=403,
        )

        with pytest.raises(exceptions.DownloadError):
            await provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_accepts_url(self, provider, mock_time):
        path = WaterButlerPath('/my-image')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('GET', url, body=b'content', auto_length=True,
                                  match_querystring=False)
        result = await provider.download(path)
        content = await result.read()
        assert content == b'content'


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/darp/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == 'photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2].extra['md5'] == '1b2cf535f27731c974343645a3985328'
        assert result[2].extra['hashes']['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_self_listing(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/thisfolder/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_and_contents if isinstance(folder_and_contents, bytes) else folder_and_contents.encode('utf-8'),
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result:
            assert fobj.name != path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata_folder_item(self, provider, folder_item_metadata, mock_time):
        path = WaterButlerPath('/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_item_metadata if isinstance(folder_item_metadata, bytes) else folder_item_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_metadata_folder(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/this-is-not-the-root/')
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_empty_metadata if isinstance(folder_empty_metadata, bytes) else folder_empty_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        aiohttpretty.register_uri('HEAD', metadata_url, headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata,
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_lastest_revision(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata,
                                  match_querystring=False)

        result = await provider.metadata(path, revision='Latest')

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        path = WaterButlerPath('/notfound.txt')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri('HEAD', url, status=404, match_querystring=False)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self,
                          provider,
                          file_content,
                          file_stream,
                          file_header_metadata,
                          mock_time):

        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
            match_querystring=False,
        )
        headers = {'ETag': f'"{content_md5}"'}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers,
                                  match_querystring=False),

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self,
                                            provider,
                                            file_stream,
                                            file_header_metadata,
                                            mock_time):
        path = WaterButlerPath('/foobah')
        url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        metadata_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
            match_querystring=False,
        )
        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"bad hash"'},
                                  match_querystring=False)

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raise_409(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "alreadyexists", because a file or '
                                   'folder already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_start_with_slash(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists')

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        create_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        head_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        empty_xml = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><IsTruncated>false</IsTruncated></ListBucketResult>'
        aiohttpretty.register_uri('GET', url, status=200, body=empty_xml,
                                  headers={'Content-Type': 'application/xml'}, match_querystring=False)
        aiohttpretty.register_uri('HEAD', head_url, status=404, match_querystring=False)
        aiohttpretty.register_uri('PUT', create_url, status=403, match_querystring=False)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out_metadata(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET', url, status=403, match_querystring=False)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        create_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'
        head_url = f'https://that-kerning.s3.amazonaws.com/{path.path}'

        empty_xml = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><IsTruncated>false</IsTruncated></ListBucketResult>'
        aiohttpretty.register_uri('GET', url, status=200, body=empty_xml,
                                  headers={'Content-Type': 'application/xml'}, match_querystring=False)
        aiohttpretty.register_uri('HEAD', head_url, status=404, match_querystring=False)
        aiohttpretty.register_uri('PUT', create_url, status=200, match_querystring=False)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/doesntalreadyexists/'


class TestOperations:

    @pytest.mark.asyncio
    async def test_get_object_versions_adds_bucket_to_presigned_params(self, provider):
        provider.generate_generic_presigned_url = MockCoroutine(return_value='http://example.com')
        provider.make_request = MockCoroutine(return_value=MockS3Response())

        await provider.get_object_versions({'Prefix': 'my-image.jpg', 'Delimiter': '/'})

        _, kwargs = provider.generate_generic_presigned_url.call_args
        assert kwargs['query_parameters']['Bucket'] == provider.bucket_name
        assert kwargs['query_parameters']['Prefix'] == 'my-image.jpg'
        assert kwargs['default_params'] is False

    @pytest.mark.asyncio
    async def test_intra_copy(self, provider, file_metadata_object, mock_time):
        source_path = WaterButlerPath('/source')
        dest_path = WaterButlerPath('/dest')

        # Mock dest_provider (exists=True → file already at dest, intra_copy returns not True=False)
        # Original test registered HEAD 200 for dest → exists=True; assert not exists checks False
        dest_provider = mock.Mock()
        dest_provider.exists = MockCoroutine(return_value=True)
        dest_provider.metadata = MockCoroutine(return_value=file_metadata_object)
        dest_provider.bucket_name = provider.bucket_name

        # Mock aiobotocore session → client (intra_copy uses copy_object directly)
        # mock.AsyncMock requires Python 3.8+; use MockCoroutine + inline async ctx manager
        mock_s3_client = mock.Mock()
        mock_s3_client.copy_object = MockCoroutine(return_value={})

        class _AsyncClientCtx:
            async def __aenter__(self_):
                return mock_s3_client
            async def __aexit__(self_, *args):
                return False

        mock_session = mock.Mock()
        mock_session.create_client = mock.Mock(return_value=_AsyncClientCtx())

        with mock.patch('waterbutler.providers.s3.provider.get_session', return_value=mock_session):
            metadata, exists = await provider.intra_copy(dest_provider, source_path, dest_path)

        assert metadata.kind == 'file'
        assert not exists
        provider._check_region.assert_called()
        mock_s3_client.copy_object.assert_called_once_with(
            Bucket=provider.bucket_name,
            Key=dest_path.path,
            CopySource={'Bucket': provider.bucket_name, 'Key': source_path.path},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_metadata(self, provider, version_metadata, mock_time):
        path = WaterButlerPath('/my-image.jpg')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, body=version_metadata if isinstance(version_metadata, bytes) else version_metadata.encode('utf-8'),
                                  status=200, match_querystring=False)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 3

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_version_metadata(self, provider, single_version_metadata, mock_time):
        path = WaterButlerPath('/single-version.file')
        url = 'https://that-kerning.s3.amazonaws.com/'
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET',
                                  url,
                                  body=single_version_metadata if isinstance(single_version_metadata, bytes) else single_version_metadata.encode('utf-8'),
                                  status=200,
                                  match_querystring=False)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 1

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url)

    def test_can_intra_move(self, provider):

        file_path = WaterButlerPath('/my-image.jpg')
        folder_path = WaterButlerPath('/folder/', folder=True)

        assert not provider.can_intra_move(provider)
        assert not provider.can_intra_move(provider, file_path)
        assert not provider.can_intra_move(provider, folder_path)

    def test_can_intra_copy(self, provider):

        file_path = WaterButlerPath('/my-image.jpg')
        folder_path = WaterButlerPath('/folder/', folder=True)

        assert not provider.can_intra_copy(provider)
        assert not provider.can_intra_copy(provider, file_path)
        assert not provider.can_intra_copy(provider, folder_path)

    def test_can_intra_copy_true_for_same_provider_and_small_file(self, provider):
        """Allows intra-copy when dest provider is same class, path is a file, and size < limit"""
        path = WaterButlerPath('/some-file.txt')
        # Use a size strictly less than the provider limit
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

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()


class TestObjectVersionsPaging:
    """U-1: ``get_object_versions`` drives the ListObjectVersions API, but pages it with the
    ListObjectsV2 continuation contract (``NextContinuationToken``/``ContinuationToken``).
    ListObjectVersions never returns a ``NextContinuationToken``; it continues with
    ``NextKeyMarker``/``NextVersionIdMarker``.  It also reports deleted objects in separate
    ``DeleteMarker`` elements, which the current collector ignores entirely.
    """

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_pages_with_key_marker(self, provider, mock_time):
        """A truncated first page must be continued with KeyMarker/VersionIdMarker.

        The first page is registered as a two-element response list rather than as a single
        response on purpose: it caps how many times that page can be served.  Code that cannot
        advance past a truncated page re-requests the very same URL forever, so without the cap
        this test would hang instead of fail.  With the cap, the third request raises
        aiohttpretty's "No responses left." and the test fails in bounded time.
        """
        install_query_encoding_presigned_url(provider)

        page_one_url = versions_url(Bucket='that-kerning', Prefix='my-image.jpg')
        page_two_url = versions_url(Bucket='that-kerning', Prefix='my-image.jpg',
                                    KeyMarker='my-image.jpg', VersionIdMarker='version-one')

        page_one_body = list_versions_response(versions=[('my-image.jpg', 'version-one')],
                                               is_truncated=True,
                                               next_key_marker='my-image.jpg',
                                               next_version_id_marker='version-one')
        aiohttpretty.register_uri(
            'GET', page_one_url,
            responses=[{'body': page_one_body, 'status': 200},
                       {'body': page_one_body, 'status': 200}],
        )
        aiohttpretty.register_uri(
            'GET', page_two_url,
            body=list_versions_response(versions=[('my-image.jpg', 'version-two')]),
            status=200,
        )

        versions = await provider.get_object_versions({'Prefix': 'my-image.jpg'})

        assert [item['VersionId'] for item in versions] == ['version-one', 'version-two']
        assert aiohttpretty.has_call(method='GET', uri=page_two_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_collects_delete_markers(self, provider, mock_time):
        """Delete markers are versions too and must be collectable for a full purge."""
        install_query_encoding_presigned_url(provider)

        url = versions_url(Bucket='that-kerning', Prefix='my-image.jpg')
        aiohttpretty.register_uri(
            'GET', url,
            body=list_versions_response(versions=[('my-image.jpg', 'version-one')],
                                        delete_markers=[('my-image.jpg', 'marker-one')]),
            status=200,
        )

        versions = await provider.get_object_versions({'Prefix': 'my-image.jpg'},
                                                      include_delete_markers=True)

        assert sorted(item['VersionId'] for item in versions) == ['marker-one', 'version-one']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_omits_delete_markers_by_default(self, provider, mock_time):
        """revisions() must not grow delete markers as a side effect of the fix."""
        install_query_encoding_presigned_url(provider)

        url = versions_url(Bucket='that-kerning', Prefix='my-image.jpg')
        aiohttpretty.register_uri(
            'GET', url,
            body=list_versions_response(versions=[('my-image.jpg', 'version-one')],
                                        delete_markers=[('my-image.jpg', 'marker-one')]),
            status=200,
        )

        versions = await provider.get_object_versions({'Prefix': 'my-image.jpg'})

        assert [item['VersionId'] for item in versions] == ['version-one']
