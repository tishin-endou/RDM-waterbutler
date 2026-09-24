import os
import io
import xml
import json
import time
import base64
import asyncio
import hashlib
import inspect
import aiohttp
import aiohttpretty
import botocore.exceptions
from aiohttp import web
from aiobotocore import session as aiobotocore_session
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
from waterbutler.providers.s3.metadata import S3FileMetadataHeaders

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
        ('',               ''),
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize('region,expected_host,expected_scope', [
        # A bucket in us-east-1 answers GetBucketLocation with an empty LocationConstraint,
        # so `region` is falsy for the whole of that bucket's traffic and the endpoint keeps
        # the global host.  botocore then signs for us-east-1 by default.
        (None,             's3.amazonaws.com',                'us-east-1'),
        ('',               's3.amazonaws.com',                'us-east-1'),
        ('us-east-1',      's3.us-east-1.amazonaws.com',      'us-east-1'),
        ('ap-northeast-1', 's3.ap-northeast-1.amazonaws.com', 'ap-northeast-1'),
        # `_check_region` rewrites the legacy 'EU' constraint to 'eu-west-1' before it can
        # reach the endpoint, which is what keeps 's3.EU.amazonaws.com' from being signed.
        ('eu-west-1',      's3.eu-west-1.amazonaws.com',      'eu-west-1'),
    ])
    async def test_signing_target_follows_the_region(self, auth, credentials, settings,
                                                     region, expected_host, expected_scope):
        provider = S3Provider(auth, credentials, settings)
        provider.region = region

        url = await provider.generate_generic_presigned_url('my-subfolder/thefile.txt')

        base, _, query = url.partition('?')
        assert base == 'https://{}/{}/my-subfolder/thefile.txt'.format(expected_host,
                                                                       settings['bucket'])

        credential = [part for part in query.split('&')
                      if part.startswith('X-Amz-Credential=')]
        assert len(credential) == 1
        assert '%2F{}%2Fs3%2Faws4_request'.format(expected_scope) in credential[0]


class TestInitialization:

    @pytest.mark.parametrize(('provider_settings', 'expected_base_folder'), [
        # The three shapes `addons.s3.models.NodeSettings.serialize_waterbutler_settings`
        # sends: a prefixed folder, a bare bucket left over from before the prefix feature,
        # and a bucket selected with no prefix.
        ({'id': 'that-kerning:/my-subfolder/'}, 'my-subfolder/'),
        ({'id': 'that-kerning'}, ''),
        ({'id': 'that-kerning:/'}, ''),
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

    @pytest.mark.asyncio
    async def test_root(self, auth, credentials, settings, mock_time):
        """T-2: a connection made at the bucket root resolves ``/`` to the root path.

        The shared ``provider`` fixture is scoped to ``/my-subfolder/`` (see ``test_subfolder``)
        because GRDM lets a node connect to a folder inside the bucket, so this case needs a
        provider whose ``id`` carries no base folder.
        """
        root_provider = S3Provider(auth, credentials, dict(settings, id='that-kerning:/'))

        path = await root_provider.validate_path('/')

        assert path.name == ''
        assert not path.is_file
        assert path.is_dir
        assert path.is_root


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
            'GET', versions_url(Bucket='that-kerning', Prefix=''),
            body=list_versions_response(
                versions=[('some-folder/', 'v1'), ('some-folder/file.txt', 'v2')]),
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
            Delete={'Objects': [{'Key': 'some-folder/', 'VersionId': 'v1'},
                                {'Key': 'some-folder/file.txt', 'VersionId': 'v2'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_with_versions(self, provider, mock_time):
        """V-6: deleting a folder purges every version and every delete marker under the
        prefix.  Deleting only the live keys leaves the folder's whole history -- and the
        storage it occupies -- behind on a versioned bucket.
        """
        path = WaterButlerPath('/folder-to-delete/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='folder-to-delete/'),
            body=list_versions_response(
                versions=[('folder-to-delete/file1.txt', '111'),
                          ('folder-to-delete/file1.txt', '222')],
                delete_markers=[('folder-to-delete/file2.txt', '333')],
            ),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'folder-to-delete/file1.txt', 'VersionId': '111'},
                                {'Key': 'folder-to-delete/file1.txt', 'VersionId': '222'},
                                {'Key': 'folder-to-delete/file2.txt', 'VersionId': '333'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_item_folder_delete(self, provider, mock_time):
        path = WaterButlerPath('/single-thing-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='single-thing-folder/'),
            body=list_versions_response(versions=[('single-thing-folder/item', 'v1')]),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'single-thing-folder/item', 'VersionId': 'v1'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_folder_delete(self, provider, mock_time):
        """V-6: an empty folder still exists as the 0-byte ``prefix/`` key, which is one
        version of its own.  Deleting it must remove that key, not report the folder missing.
        """
        path = WaterButlerPath('/empty-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='empty-folder/'),
            body=list_versions_response(versions=[('empty-folder/', 'v1')]),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'empty-folder/', 'VersionId': 'v1'}], 'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_not_found(self, provider, mock_time):
        """V-6: a prefix with neither a version nor a delete marker under it is a folder that
        does not exist, and must not be reported as a successful delete."""
        path = WaterButlerPath('/not-found-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='not-found-folder/'),
            body=list_versions_response(),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            with pytest.raises(exceptions.NotFoundError):
                await provider.delete(path)

        assert s3_client.delete_objects.called is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_of_delete_markers_only(self, provider, mock_time):
        """V-6: a folder whose keys have all been delete-marked still has versions to purge."""
        path = WaterButlerPath('/tombstone-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='tombstone-folder/'),
            body=list_versions_response(
                delete_markers=[('tombstone-folder/file1.txt', '111')]),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'tombstone-folder/file1.txt', 'VersionId': '111'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_large_folder_delete(self, provider, mock_time):
        """DeleteObjects takes at most 1000 objects per call."""
        path = WaterButlerPath('/some-folder/')
        install_query_encoding_presigned_url(provider)

        keys = [f'some-folder/file-{index:05d}' for index in range(1001)]
        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='some-folder/'),
            body=list_versions_response(versions=[(key, 'v1') for key in keys]),
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
    async def test_delete_folder_truncated_response(self, provider, mock_time):
        """V-6: a folder holding more than one page of versions must be listed to the end
        before any of it is deleted, otherwise the tail of the folder silently survives.
        ListObjectVersions resumes from the last key *and* version id, not a continuation
        token."""
        path = WaterButlerPath('/large-folder/')
        install_query_encoding_presigned_url(provider)

        page_one_url = versions_url(Bucket='that-kerning', Prefix='large-folder/')
        page_two_url = versions_url(Bucket='that-kerning', Prefix='large-folder/',
                                    KeyMarker='large-folder/file2.txt', VersionIdMarker='222')

        aiohttpretty.register_uri(
            'GET', page_one_url,
            body=list_versions_response(versions=[('large-folder/file1.txt', '111')],
                                        is_truncated=True,
                                        next_key_marker='large-folder/file2.txt',
                                        next_version_id_marker='222'),
            status=200,
        )
        aiohttpretty.register_uri(
            'GET', page_two_url,
            body=list_versions_response(versions=[('large-folder/file2.txt', '222')]),
            status=200,
        )

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=page_two_url)
        s3_client.delete_objects.assert_called_once_with(
            Bucket='that-kerning',
            Delete={'Objects': [{'Key': 'large-folder/file1.txt', 'VersionId': '111'},
                                {'Key': 'large-folder/file2.txt', 'VersionId': '222'}],
                    'Quiet': False},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete_partial_failure_raises(self, provider, mock_time):
        """V-4, folder side: refusals reported inside the 200 body must not read as success."""
        path = WaterButlerPath('/error-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='error-folder/'),
            body=list_versions_response(versions=[('error-folder/file1.txt', '111'),
                                                  ('error-folder/file2.txt', '222')]),
            status=200,
        )

        delete_result = {
            'Deleted': [{'Key': 'error-folder/file1.txt', 'VersionId': '111'}],
            'Errors': [{'Key': 'error-folder/file2.txt', 'VersionId': '222',
                        'Code': 'AccessDenied', 'Message': 'Access Denied'}],
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
    async def test_delete_folder_delete_error(self, provider, mock_time):
        """V-6: a refused DeleteObjects call surfaces as a DeleteError."""
        path = WaterButlerPath('/error-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='error-folder/'),
            body=list_versions_response(versions=[('error-folder/file1.txt', '111')]),
            status=200,
        )

        patcher, _ = patch_aiobotocore_client(
            delete_objects=MockCoroutine(side_effect=Exception('AccessDenied')))
        with patcher:
            with pytest.raises(exceptions.DeleteError):
                await provider.delete(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete_listing_error(self, provider, mock_time):
        path = WaterButlerPath('/error-folder/')
        install_query_encoding_presigned_url(provider)

        aiohttpretty.register_uri(
            'GET', versions_url(Bucket='that-kerning', Prefix='error-folder/'),
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
    async def test_handle_data(self, provider):
        """P-3: the trailing continuation token is split off the listing."""
        data = ['txt001.txt', 'abc']
        result, token = provider.handle_data(data)
        assert token == 'abc'
        assert result == ['txt001.txt']

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
    async def test_metadata_have_next_token(self, provider, folder_metadata, mock_time):
        """P-1: ``metadata()`` accepts ``next_token`` instead of dropping it into ``**kwargs``."""
        path = WaterButlerPath('/darp/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        aiohttpretty.register_uri('GET', url, body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider.metadata(path, revision=None, next_token='')

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == 'photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2].extra['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_have_next_token(self, provider, folder_metadata, mock_time):
        """P-1: ``_metadata_folder()`` takes the token positionally as well."""
        path = WaterButlerPath('/darp/')
        url = 'https://that-kerning.s3.amazonaws.com/'
        aiohttpretty.register_uri('GET', url, body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
                                  headers={'Content-Type': 'application/xml'},
                                  match_querystring=False)

        result = await provider._metadata_folder(path, next_token='')

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


class TestFolderListingPaging:
    """P-1〜P-5: one page at a time for the UI, the whole listing for everyone else.

    The GRDM file browser walks a folder page by page, so ``metadata()`` has to be able to
    stop after one page and hand back a token for the next one.  Every other caller of
    ``metadata()`` -- ``BaseProvider._folder_file_op``, ``BaseProvider.zip``,
    ``ZipStreamGenerator`` -- wants the complete listing and would choke on a token mixed in
    among the metadata objects, so the two behaviours are told apart by whether the caller
    passed a ``next_token`` keyword at all.
    """

    PREFIX = 'darp/'

    def _register_page(self, url, keys, is_truncated=False, next_continuation_token=None):
        aiohttpretty.register_uri(
            'GET', url,
            body=list_objects_v2_response(keys, is_truncated=is_truncated,
                                          next_continuation_token=next_continuation_token),
            headers={'Content-Type': 'application/xml'},
        )

    def _page_url(self, **extra):
        params = {'Bucket': 'that-kerning', 'Prefix': self.PREFIX, 'Delimiter': '/'}
        params.update(extra)
        return objects_url(**params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_page_ends_with_the_continuation_token(self, provider, mock_time):
        """P-2: a truncated page is returned as-is with the token appended as a bare str."""
        calls = install_query_encoding_presigned_url(provider)
        self._register_page(self._page_url(MaxKeys='1000'),
                            ['darp/a.txt', 'darp/b.txt'],
                            is_truncated=True, next_continuation_token='page-2-token')

        result = await provider.metadata(WaterButlerPath('/darp/'), next_token='')

        assert [item.name for item in result[:-1]] == ['a.txt', 'b.txt']
        assert result[-1] == 'page-2-token'
        # One page means one request -- the provider must not drain the listing here.
        assert len(calls) == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_page_asks_for_at_most_1000_keys(self, provider, mock_time):
        """P-2: the page size is pinned so the token round trip stays bounded."""
        calls = install_query_encoding_presigned_url(provider)
        self._register_page(self._page_url(MaxKeys='1000'), ['darp/a.txt'])

        await provider.metadata(WaterButlerPath('/darp/'), next_token='')

        assert calls[0]['MaxKeys'] == '1000'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_last_page_has_no_trailing_token(self, provider, mock_time):
        """P-4: ``IsTruncated`` false means the caller must not see a str at the end."""
        install_query_encoding_presigned_url(provider)
        self._register_page(self._page_url(MaxKeys='1000'), ['darp/a.txt', 'darp/b.txt'])

        result = await provider.metadata(WaterButlerPath('/darp/'), next_token='')

        assert not isinstance(result[-1], str)
        assert [item.name for item in result] == ['a.txt', 'b.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_token_is_sent_back_as_the_continuation_token(self, provider, mock_time):
        """P-5: the token the UI received comes back unchanged and selects the next page."""
        calls = install_query_encoding_presigned_url(provider)
        self._register_page(self._page_url(MaxKeys='1000', ContinuationToken='page-2-token'),
                            ['darp/c.txt'])

        result = await provider.metadata(WaterButlerPath('/darp/'), next_token='page-2-token')

        assert calls[0]['ContinuationToken'] == 'page-2-token'
        assert [item.name for item in result] == ['c.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_listing_without_next_token_returns_every_page(self, provider, mock_time):
        """Regression guard: ``metadata(path)`` with no ``next_token`` keyword must return the
        complete listing and nothing but metadata objects.

        ``BaseProvider._folder_file_op`` reads ``item.name`` off every element and
        ``ZipStreamGenerator`` feeds every element to ``path_from_metadata``; a str token among
        them raises ``AttributeError`` mid-copy or mid-download.
        """
        install_query_encoding_presigned_url(provider)
        self._register_page(self._page_url(), ['darp/a.txt'],
                            is_truncated=True, next_continuation_token='page-2-token')
        self._register_page(self._page_url(ContinuationToken='page-2-token'), ['darp/b.txt'])

        result = await provider.metadata(WaterButlerPath('/darp/'))

        assert [item.name for item in result] == ['a.txt', 'b.txt']
        assert not any(isinstance(item, str) for item in result)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_handle_data_leaves_a_single_file_alone(self, provider, file_header_metadata,
                                                          mock_time):
        """P-3: a file's metadata is not a listing, so nothing may be popped off it."""
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        aiohttpretty.register_uri('HEAD',
                                  f'https://that-kerning.s3.amazonaws.com/{path.path}',
                                  headers=file_header_metadata, match_querystring=False)

        file_metadata = await provider.metadata(path)
        data, token = provider.handle_data(file_metadata)

        assert isinstance(data, S3FileMetadataHeaders)
        assert data is file_metadata
        assert token == ''


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
    async def test_create_folder_with_folder_precheck_is_false(self, provider, mock_time):
        """T-2: skipping the "does it already exist" check does not skip the check that the
        path names a folder at all, so no request is made for a path that cannot be created."""
        path = WaterButlerPath('/alreadyexists')

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path, folder_precheck=False)

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


def make_client_error(code, message, status, operation='CopyObject'):
    """Build the ``ClientError`` botocore raises for a failed S3 operation."""
    return botocore.exceptions.ClientError(
        {
            'Error': {'Code': code, 'Message': message},
            'ResponseMetadata': {'HTTPStatusCode': status, 'RequestId': 'a-request-id'},
        },
        operation,
    )


class _FakeHTTPResponse:
    """The minimum surface ``aiobotocore.endpoint`` needs from an HTTP response.

    ``convert_to_response_dict`` reads ``raw_headers``/``status_code`` and awaits ``read()``;
    botocore's ``check_for_200_error`` reads ``content`` and *writes* ``status_code``.
    """

    def __init__(self, status, body):
        self.status_code = status
        self.content = body
        self.raw_headers = ((b'Content-Type', b'application/xml'),)
        self.raw = None

    async def read(self):
        return self.content


def patch_session_with_before_send(monkeypatch, http_response_factory, operation='CopyObject'):
    """Let the provider build a *real* aiobotocore client, but answer its HTTP request locally.

    botocore offers a ``before-send.<service>.<Operation>`` event precisely so the transport can
    be replaced without disturbing anything above it.  Injecting there means the request is still
    signed, the response is still parsed by botocore's rest-xml parser, and the whole
    ``needs-retry`` handler chain -- including S3's 200-with-error special case -- still runs.
    Injecting at the aiobotocore client boundary instead would skip all of that, which is exactly
    the behaviour K-3 needs to measure.

    :return: the list of sent requests, in order
    """
    # botocore's legacy retry mode would replay the request four more times, and the backoff
    # sleeps are real.  The retry count is irrelevant to what is being measured here.
    monkeypatch.setenv('AWS_RETRY_MODE', 'standard')
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '1')

    sent = []

    def before_send(request, **kwargs):
        sent.append(request)
        return http_response_factory()

    def _get_session():
        session = aiobotocore_session.get_session()
        session.register('before-send.s3.{}'.format(operation), before_send)
        return session

    monkeypatch.setattr('waterbutler.providers.s3.provider.get_session', _get_session)
    return sent


COPY_OBJECT_SUCCESS_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<CopyObjectResult><ETag>"fba9dede5f27731c9771645a39863328"</ETag>'
    b'<LastModified>2009-10-12T17:50:30.000Z</LastModified></CopyObjectResult>'
)

COPY_OBJECT_ERROR_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<Error><Code>InternalError</Code>'
    b'<Message>We encountered an internal error. Please try again.</Message>'
    b'<RequestId>656c76696e</RequestId></Error>'
)

COPY_OBJECT_EMPTY_ERROR_BODY = b'<?xml version="1.0" encoding="UTF-8"?>\n<Error/>'


class TestIntraCopy:
    """I-2〜I-5: the ``intra_copy`` contract and how it reports provider failures."""

    def _dest_provider(self, provider, file_metadata_object, exists):
        dest_provider = mock.Mock()
        dest_provider.exists = MockCoroutine(return_value=exists)
        dest_provider.metadata = MockCoroutine(return_value=file_metadata_object)
        dest_provider.bucket_name = provider.bucket_name
        return dest_provider

    @pytest.mark.asyncio
    async def test_intra_copy_reports_created_when_dest_is_absent(self, provider,
                                                                  file_metadata_object,
                                                                  mock_time):
        """I-5: ``(metadata, created)`` -- ``created`` is True only when nothing was overwritten."""
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=False)
        patcher, client = patch_aiobotocore_client(copy_object=MockCoroutine(return_value={}))

        with patcher:
            metadata_result, created = await provider.intra_copy(
                dest_provider, WaterButlerPath('/source'), WaterButlerPath('/dest'))

        assert created is True
        assert metadata_result is file_metadata_object
        dest_provider.exists.assert_called_once_with(WaterButlerPath('/dest'))

    @pytest.mark.asyncio
    async def test_intra_copy_reports_not_created_when_dest_exists(self, provider,
                                                                   file_metadata_object,
                                                                   mock_time):
        """I-5: an overwrite reports ``created`` False."""
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=True)
        patcher, client = patch_aiobotocore_client(copy_object=MockCoroutine(return_value={}))

        with patcher:
            metadata_result, created = await provider.intra_copy(
                dest_provider, WaterButlerPath('/source'), WaterButlerPath('/dest'))

        assert created is False
        assert metadata_result is file_metadata_object

    @pytest.mark.asyncio
    async def test_intra_copy_converts_client_error(self, provider, file_metadata_object,
                                                    mock_time):
        """K-8: a botocore ``ClientError`` must become an ``IntraCopyError`` that carries the
        provider's HTTP status, not a blanket 500.
        """
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=False)
        error = make_client_error('AccessDenied', 'Access Denied', 403)
        patcher, client = patch_aiobotocore_client(
            copy_object=MockCoroutine(side_effect=error))

        with patcher:
            with pytest.raises(exceptions.IntraCopyError) as exc_info:
                await provider.intra_copy(dest_provider, WaterButlerPath('/source'),
                                          WaterButlerPath('/dest'))

        assert exc_info.value.code == 403
        assert 'ClientError' in exc_info.value.message
        assert 'AccessDenied' in exc_info.value.message

    @pytest.mark.asyncio
    async def test_intra_copy_error_message_omits_provider_detail(self, provider,
                                                                  file_metadata_object,
                                                                  mock_time):
        """K-9: the error surfaced to the user names the failure; it does not quote the provider's
        own message, which is where request ids, bucket names and signed urls leak from.
        """
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=False)
        error = make_client_error(
            'AccessDenied',
            'Access Denied for arn:aws:iam::123456789012:user/some-user',
            403,
        )
        patcher, client = patch_aiobotocore_client(
            copy_object=MockCoroutine(side_effect=error))

        with patcher:
            with pytest.raises(exceptions.IntraCopyError) as exc_info:
                await provider.intra_copy(dest_provider, WaterButlerPath('/source'),
                                          WaterButlerPath('/dest'))

        message = exc_info.value.message
        assert 'arn:aws:iam' not in message
        assert 'An error occurred' not in message
        assert provider.aws_secret_access_key not in message

    @pytest.mark.asyncio
    async def test_intra_copy_succeeds_through_botocore(self, provider, file_metadata_object,
                                                        monkeypatch, mock_time):
        """K-3 control: the same real-botocore harness lets an ordinary 200 through, so a failure
        in the sibling tests is attributable to the response body and not to the harness.
        """
        provider.region = 'us-east-1'
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=False)
        sent = patch_session_with_before_send(
            monkeypatch, lambda: _FakeHTTPResponse(200, COPY_OBJECT_SUCCESS_BODY))

        metadata_result, created = await provider.intra_copy(
            dest_provider, WaterButlerPath('/source'), WaterButlerPath('/dest'))

        assert created is True
        assert metadata_result is file_metadata_object
        assert len(sent) == 1
        assert 'Signature=' in sent[0].headers['Authorization'].decode('utf-8') \
            or 'AWS4-HMAC-SHA256' in sent[0].headers['Authorization'].decode('utf-8')

    @pytest.mark.asyncio
    @pytest.mark.parametrize('body', [COPY_OBJECT_ERROR_BODY, COPY_OBJECT_EMPTY_ERROR_BODY])
    async def test_intra_copy_200_with_error_body_fails_closed(self, provider,
                                                               file_metadata_object,
                                                               monkeypatch, mock_time, body):
        """K-3: S3 can answer CopyObject with 200 and an ``<Error>`` body.  Measure whether
        botocore's ``check_for_200_error`` catches it under aiobotocore, and make sure whatever
        it produces reaches the caller as a failure -- never as a successful copy, and never with
        a 2xx status code attached to the WaterButler error.
        """
        provider.region = 'us-east-1'
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=False)
        patch_session_with_before_send(monkeypatch, lambda: _FakeHTTPResponse(200, body))

        with pytest.raises(exceptions.IntraCopyError) as exc_info:
            await provider.intra_copy(dest_provider, WaterButlerPath('/source'),
                                      WaterButlerPath('/dest'))

        # botocore reports the *original* 200 in ResponseMetadata even after rewriting the
        # response's status code, so a naive passthrough would hand a 2xx to the API layer.
        assert exc_info.value.code == 500
        assert 'ClientError' in exc_info.value.message
        assert 'An error occurred' not in exc_info.value.message
        assert dest_provider.metadata.called is False


class TestIntraCopySizeLimit:
    """I-3: core must not route an oversized file through ``intra_copy``."""

    def _spy_on_intra(self, provider):
        """Record calls without replacing the implementation, so an unexpected call still runs
        (and fails loudly) instead of being silently swallowed by a mock.
        """
        calls = {'copy': [], 'move': []}
        original_copy, original_move = provider.intra_copy, provider.intra_move

        async def copy_spy(*args, **kwargs):
            calls['copy'].append(args)
            return await original_copy(*args, **kwargs)

        async def move_spy(*args, **kwargs):
            calls['move'].append(args)
            return await original_move(*args, **kwargs)

        provider.intra_copy, provider.intra_move = copy_spy, move_spy
        return calls

    def _register_copy_traffic(self, provider, file_content, file_header_metadata):
        src_url = 'https://that-kerning.s3.amazonaws.com/source.txt'
        dest_url = 'https://that-kerning.s3.amazonaws.com/dest.txt'
        headers = dict(file_header_metadata)
        headers['Content-Length'] = str(len(file_content))

        aiohttpretty.register_uri('GET', src_url, body=file_content,
                                  headers={'Content-Length': str(len(file_content))},
                                  status=200, match_querystring=False)
        aiohttpretty.register_uri('HEAD', dest_url,
                                  responses=[{'status': 404}, {'headers': headers}],
                                  match_querystring=False)
        aiohttpretty.register_uri(
            'PUT', dest_url, status=200,
            headers={'ETag': '"{}"'.format(hashlib.md5(file_content).hexdigest())},
            match_querystring=False)
        return src_url, dest_url

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_copy_over_limit_falls_back_to_stream_copy(self, provider, file_content,
                                                             file_header_metadata, mock_time):
        calls = self._spy_on_intra(provider)
        src_url, dest_url = self._register_copy_traffic(provider, file_content,
                                                        file_header_metadata)

        metadata_result, created = await provider.copy(
            provider,
            WaterButlerPath('/source.txt'),
            WaterButlerPath('/dest.txt'),
            handle_naming=False,
            file_size=provider.FILE_SIZE_INTRA_COPY_LIMIT + 1,
        )

        assert calls['copy'] == []
        assert created is True
        assert metadata_result.kind == 'file'
        assert aiohttpretty.has_call(method='GET', uri=src_url)
        assert aiohttpretty.has_call(method='PUT', uri=dest_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_move_over_limit_falls_back_to_copy_then_delete(self, provider, file_content,
                                                                  file_header_metadata,
                                                                  mock_time):
        calls = self._spy_on_intra(provider)
        src_url, dest_url = self._register_copy_traffic(provider, file_content,
                                                        file_header_metadata)
        # the source is deleted after the copy; it has a single version and no delete markers.
        # the fixture's presigned-url stub drops query parameters, so the version listing lands
        # on the bare bucket url rather than on the source key's url.
        aiohttpretty.register_uri(
            'GET', BUCKET_URL,
            body=list_versions_response(versions=[('source.txt', 'v1')]), status=200,
            match_querystring=False)
        delete_patcher, delete_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [{'Key': 'source.txt'}]}))

        with delete_patcher:
            metadata_result, created = await provider.move(
                provider,
                WaterButlerPath('/source.txt'),
                WaterButlerPath('/dest.txt'),
                handle_naming=False,
                file_size=provider.FILE_SIZE_INTRA_COPY_LIMIT + 1,
            )

        assert calls['move'] == []
        assert calls['copy'] == []
        assert created is True
        assert metadata_result.kind == 'file'
        assert delete_client.delete_objects.called

    @pytest.mark.parametrize('method_name', ['can_intra_copy', 'can_intra_move'])
    @pytest.mark.parametrize('offset,expected', [
        (-1, True),
        (0, True),    # a file of exactly the limit is still copied server side
        (1, False),
    ])
    def test_size_limit_boundary(self, provider, method_name, offset, expected):
        """The limit is inclusive.  The fallback tests above only exercise limit + 1, which
        leaves ``>`` and ``>=`` indistinguishable; this pins which one it is.
        """
        decide = getattr(provider, method_name)
        file_size = provider.FILE_SIZE_INTRA_COPY_LIMIT + offset

        assert decide(provider, WaterButlerPath('/source.txt'), file_size) is expected

    @pytest.mark.parametrize('method_name', ['can_intra_copy', 'can_intra_move'])
    def test_unknown_size_is_not_copied_server_side(self, provider, method_name):
        decide = getattr(provider, method_name)

        assert decide(provider, WaterButlerPath('/source.txt'), None) is False


class TestFileSizeSource:
    """I-4: ``file_size`` comes from ``S3FileMetadataHeaders.size``, which has to survive both
    spellings of the length header.  ``osfstorage`` feeds the result straight into ``int()``
    (providers/osfstorage/provider.py), so a ``None`` here is a TypeError there.
    """

    @pytest.mark.parametrize('raw', [
        {'ContentLength': 9001},      # botocore HeadObject response
        {'ContentLength': '9001'},
        {'Content-Length': '9001'},   # aiohttp HEAD response headers
    ])
    def test_size_is_usable_as_an_int(self, raw):
        size = S3FileMetadataHeaders('test-path', raw).size

        assert size is not None
        assert int(size) == 9001

    @pytest.mark.parametrize('raw', [
        {'ContentLength': 9001},
        {'Content-Length': '9001'},
    ])
    def test_size_as_int_is_an_int(self, raw):
        size_as_int = S3FileMetadataHeaders('test-path', raw).size_as_int

        assert isinstance(size_as_int, int)
        assert size_as_int == 9001

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_reports_size_from_head_response(self, provider,
                                                                 file_header_metadata,
                                                                 mock_time):
        """The real path: HEAD answers with ``Content-Length``, and the size survives to the
        metadata object that ``can_intra_copy`` is handed.
        """
        path = WaterButlerPath('/my-image.jpg')
        url = 'https://that-kerning.s3.amazonaws.com/my-image.jpg'
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata,
                                  match_querystring=False)

        result = await provider.metadata(path)

        assert int(result.size) == 9001
        assert result.size_as_int == 9001
        assert provider.can_intra_copy(provider, path=path,
                                       file_size=result.size_as_int) is True


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


# A presigned SigV4 URL carries the access key id in ``X-Amz-Credential`` and the signature in
# ``X-Amz-Signature``.  Neither may reach a response body or a log line.
SIGNED_URL = (
    'https://that-kerning.s3.amazonaws.com/my-subfolder/thefile.txt'
    '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
    '&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20160205%2Fus-east-1%2Fs3%2Faws4_request'
    '&X-Amz-Signature=deadbeefcafebabe0123456789abcdef0123456789abcdef0123456789abcdef'
)

SECRET_MARKERS = ('X-Amz-Signature', 'X-Amz-Credential', 'AKIAIOSFODNN7EXAMPLE')


def s3_client_error(code, status, operation='HeadObject'):
    """A botocore ``ClientError`` shaped like the one aiobotocore raises for ``code``."""
    return botocore.exceptions.ClientError(
        {
            'Error': {'Code': code, 'Message': 'S3 prose naming the bucket and the request'},
            'ResponseMetadata': {'HTTPStatusCode': status,
                                 'RequestId': 'REQ123', 'HostId': 'HOST456'},
        },
        operation,
    )


def assert_no_secrets(exc):
    blob = '{!r} {!s} {}'.format(exc, exc, getattr(exc, 'message', ''))
    leaked = [marker for marker in SECRET_MARKERS if marker in blob]
    assert leaked == [], 'exception exposes {}'.format(leaked)


def raw_provider(auth, credentials, settings):
    """A provider with only the region lookup stubbed, so that the real
    ``generate_generic_presigned_url`` and ``check_key_existence`` run."""
    prov = S3Provider(auth, credentials, settings)
    prov._check_region = MockCoroutine()
    prov.region = 'us-east-1'
    return prov


class commit_server:
    """An ``aiohttp.web`` server that accepts a single commit.

    Ported from ``tests/providers/s3compatsigv4/test_provider.py`` (PR #98).
    ``aiohttpretty`` injects responses *above* ``ClientSession._request``, so the redirect
    following that happens *inside* that call cannot be reproduced with it, and pinning it
    needs a real socket.

    Startup and teardown are owned here.  With ``runner.setup()`` through URL assembly left
    outside the ``finally``, a failure after the server started would carry a listening
    socket and the provider's sessions into the next test.
    """

    def __init__(self, provider, app):
        self.provider = provider
        self.app = app
        self.runner = web.AppRunner(app)
        self.url = None

    async def __aenter__(self):
        await self.runner.setup()
        try:
            site = web.TCPSite(self.runner, '127.0.0.1', 0)
            await site.start()
            # aiohttp 3.6.2 exposes the bound port only here.  If this private attribute
            # disappears the AttributeError is deliberate: a test that visibly breaks beats
            # one that quietly skips.
            sockets = site._server.sockets
            assert sockets, 'the test server bound no socket'
            self.url = 'http://127.0.0.1:{}/first'.format(sockets[0].getsockname()[1])
        except Exception:
            # ``__aexit__`` is not called when ``__aenter__`` raises.
            await self.runner.cleanup()
            raise
        return self

    async def __aexit__(self, *exc_info):
        first = None
        try:
            # One failing close must not strand the rest: letting the loop raise would leave
            # every later session open and carry it into the next test.
            for session in self.provider.session_list:
                try:
                    await session.close()
                except Exception as err:
                    first = first if first is not None else err
        finally:
            # The listening socket comes down even if a session close fails.
            await self.runner.cleanup()
        if first is not None:
            raise first
        return False


class TestErrorReporting:
    """K-2 / K-7 / K-8 / K-9: what the six aiobotocore call sites do with a failure."""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_check_key_existence_does_not_expose_the_presigned_url(self, auth, credentials,
                                                                        settings, mock_time):
        """K-8/K-9: core builds its message out of the request URL
        (``exceptions.DEFAULT_ERROR_MSG``), and ``waterbutler.server.api.v1.core.write_error``
        hands ``exc.message`` straight to the client.  Re-wrapping that message verbatim puts the
        signature and the access key id in a 404 body."""
        provider = raw_provider(auth, credentials, settings)
        patcher, _ = patch_aiobotocore_client(
            generate_presigned_url=MockCoroutine(return_value=SIGNED_URL))
        aiohttpretty.register_uri('HEAD', SIGNED_URL, status=403)

        with patcher:
            with pytest.raises(exceptions.NotFoundError) as e:
                await provider.check_key_existence('my-subfolder/thefile.txt')

        assert_no_secrets(e.value)
        assert 'my-subfolder/thefile.txt' in e.value.message

    @pytest.mark.asyncio
    async def test_generate_presigned_url_reports_the_code_not_s3_prose(self, auth, credentials,
                                                                       settings, mock_time):
        """K-2: name the failure by type and S3 error code.  botocore's own message quotes S3's
        prose, which carries the request id and the host id."""
        provider = raw_provider(auth, credentials, settings)
        patcher, _ = patch_aiobotocore_client(
            generate_presigned_url=MockCoroutine(
                side_effect=s3_client_error('AccessDenied', 403)))

        with patcher:
            with pytest.raises(exceptions.NotFoundError) as e:
                await provider.generate_generic_presigned_url('/my-subfolder/thefile.txt')

        assert 'AccessDenied' in e.value.message
        assert 'REQ123' not in e.value.message
        assert 'HOST456' not in e.value.message

    @pytest.mark.asyncio
    async def test_get_bucket_location_converts_a_client_error(self, auth, credentials, settings,
                                                               mock_time):
        """K-2: this site has no handler at all, so a botocore ``ClientError`` escapes the
        provider as itself and the API layer can only answer 500 with no code."""
        provider = raw_provider(auth, credentials, settings)
        patcher, _ = patch_aiobotocore_client(
            generate_presigned_url=MockCoroutine(
                side_effect=s3_client_error('AccessDenied', 403, 'GetBucketLocation')))

        with patcher:
            with pytest.raises(exceptions.MetadataError) as e:
                await provider.get_s3_bucket_object_location()

        assert e.value.code == 403
        assert 'AccessDenied' in e.value.message

    @pytest.mark.asyncio
    async def test_delete_objects_reports_the_code_not_s3_prose(self, auth, credentials, settings,
                                                                mock_time):
        """K-2: keep S3's status rather than flattening every refusal to 500."""
        provider = raw_provider(auth, credentials, settings)
        patcher, _ = patch_aiobotocore_client(
            delete_objects=MockCoroutine(
                side_effect=s3_client_error('AccessDenied', 403, 'DeleteObjects')))

        with patcher:
            with pytest.raises(exceptions.DeleteError) as e:
                await provider.delete_objects_in_chunks(
                    '/my-subfolder/', [{'Key': 'a', 'VersionId': 'v'}])

        assert e.value.code == 403
        assert 'AccessDenied' in e.value.message
        assert 'REQ123' not in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize('site,method,call', [
        ('generate_generic_presigned_url', 'generate_presigned_url',
         lambda p: p.generate_generic_presigned_url('/my-subfolder/thefile.txt')),
        ('delete_objects_in_chunks', 'delete_objects',
         lambda p: p.delete_objects_in_chunks('/my-subfolder/',
                                              [{'Key': 'a', 'VersionId': 'v'}])),
    ])
    async def test_cancellation_is_not_swallowed(self, auth, credentials, settings, mock_time,
                                                 site, method, call):
        """K-7: Python 3.6 derives ``asyncio.CancelledError`` from ``Exception``, so the broad
        ``except Exception`` around each of these calls catches it.  Reporting a cancelled request
        as a provider failure stops the cancellation from propagating, and the task never ends."""
        provider = raw_provider(auth, credentials, settings)
        patcher, _ = patch_aiobotocore_client(
            **{method: MockCoroutine(side_effect=asyncio.CancelledError())})

        with patcher:
            with pytest.raises(asyncio.CancelledError):
                await call(provider)

    @pytest.mark.asyncio
    async def test_chunked_upload_cancellation_is_not_swallowed(self, auth, credentials, settings,
                                                                mock_time):
        """K-7: same for the multi-part upload's handler, which additionally fires off an abort."""
        provider = raw_provider(auth, credentials, settings)
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(side_effect=asyncio.CancelledError())
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(asyncio.CancelledError):
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

    @pytest.mark.asyncio
    async def test_chunked_upload_does_not_log_the_presigned_url(self, auth, credentials, settings,
                                                                 mock_time, caplog):
        """K-8/K-9: the handler logs ``repr()`` of whatever was raised.  Everything raised out of
        ``make_request`` reprs to the request URL, so the signature and the access key id land in
        the log of every failed multi-part upload."""
        provider = raw_provider(auth, credentials, settings)
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(return_value=[])
        provider._complete_multipart_upload = MockCoroutine(side_effect=exceptions.UploadError(
            'An error occurred while making a POST request to {}'.format(SIGNED_URL), code=403))
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.UploadError):
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        logged = ' '.join(record.getMessage() for record in caplog.records)
        leaked = [marker for marker in SECRET_MARKERS if marker in logged]
        assert leaked == [], 'log exposes {}'.format(leaked)
        assert 'UploadError' in logged
        assert 'SESSION' in logged

    @pytest.mark.asyncio
    async def test_intra_copy_error_reporting_is_unchanged(self, auth, credentials, settings,
                                                           mock_time):
        """I-2 folded into the shared helper: same message, same status."""
        provider = raw_provider(auth, credentials, settings)
        dest = raw_provider(auth, credentials, settings)
        dest.exists = MockCoroutine(return_value=False)
        dest.metadata = MockCoroutine(return_value='META')
        patcher, _ = patch_aiobotocore_client(
            copy_object=MockCoroutine(
                side_effect=s3_client_error('InternalError', 200, 'CopyObject')))

        with patcher:
            with pytest.raises(exceptions.IntraCopyError) as e:
                await provider.intra_copy(dest, WaterButlerPath('/a.txt'),
                                          WaterButlerPath('/b.txt'))

        assert e.value.message == 'CopyObject failed: ClientError InternalError'
        assert e.value.code == 500


class TestResponseParsing:
    """K-10: what each XML shape the provider can be handed turns into."""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_rejects_an_unrecognised_root_element(self, provider, mock_time):
        """K-10: ``doc.get('ListBucketResult', {})`` answers ``{}`` for any body whose root
        element is not spelled exactly that -- a namespace-prefixed one, say -- and an empty
        listing is indistinguishable from an empty folder.  Fail closed instead."""
        install_query_encoding_presigned_url(provider)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<s3:ListBucketResult xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/">'
                '<s3:IsTruncated>false</s3:IsTruncated>'
                '<s3:Contents><s3:Key>my-subfolder/thefile.txt</s3:Key></s3:Contents>'
                '</s3:ListBucketResult>').encode('utf-8')
        aiohttpretty.register_uri('GET', objects_url(Prefix='my-subfolder/'), body=body,
                                  status=200)

        with pytest.raises(exceptions.DownloadError):
            await provider.get_folder_metadata('my-subfolder/', {'Prefix': 'my-subfolder/'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_listing_rejects_an_unrecognised_root_element(self, provider, mock_time):
        """K-10: the same shape on the versions listing decides what a delete purges.  An empty
        list means "nothing to delete", so a delete would report success having removed nothing."""
        install_query_encoding_presigned_url(provider)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<s3:ListVersionsResult xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/">'
                '<s3:IsTruncated>false</s3:IsTruncated>'
                '</s3:ListVersionsResult>').encode('utf-8')
        aiohttpretty.register_uri('GET', versions_url(Bucket='that-kerning',
                                                      Prefix='my-image.jpg'),
                                  body=body, status=200)

        with pytest.raises(exceptions.DownloadError):
            await provider.get_object_versions({'Prefix': 'my-image.jpg'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_accepts_a_whitespace_formatted_body(self, provider, mock_time):
        """K-10: indentation between the elements must not change the result."""
        install_query_encoding_presigned_url(provider)
        body = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
                '  <IsTruncated>false</IsTruncated>\n'
                '  <Contents>\n    <Key>my-subfolder/thefile.txt</Key>\n  </Contents>\n'
                '</ListBucketResult>\n').encode('utf-8')
        aiohttpretty.register_uri('GET', objects_url(Prefix='my-subfolder/'), body=body,
                                  status=200)

        contents, prefixes, token = await provider.get_folder_metadata(
            'my-subfolder/', {'Prefix': 'my-subfolder/'})

        assert [item['Key'] for item in contents] == ['my-subfolder/thefile.txt']
        assert token == ''

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_accepts_a_single_contents_element(self, provider, mock_time):
        """K-10: xmltodict collapses a lone repeated element to a dict rather than a
        one-element list."""
        install_query_encoding_presigned_url(provider)
        aiohttpretty.register_uri(
            'GET', objects_url(Prefix='my-subfolder/'),
            body=list_objects_v2_response(['my-subfolder/thefile.txt']), status=200)

        contents, prefixes, token = await provider.get_folder_metadata(
            'my-subfolder/', {'Prefix': 'my-subfolder/'})

        assert [item['Key'] for item in contents] == ['my-subfolder/thefile.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_rejects_an_empty_body(self, provider, mock_time):
        """K-10: an empty 200 must not read as an empty folder."""
        install_query_encoding_presigned_url(provider)
        aiohttpretty.register_uri('GET', objects_url(Prefix='my-subfolder/'), body=b'', status=200)

        with pytest.raises(exceptions.DownloadError):
            await provider.get_folder_metadata('my-subfolder/', {'Prefix': 'my-subfolder/'})


class TestCompleteMultipartUpload:
    """K-1 / K-3: committing a multi-part upload."""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_complete_rejects_a_200_carrying_an_error(self, auth, credentials, settings,
                                                            mock_time):
        """K-3: S3 answers CompleteMultipartUpload with 200 and an ``<Error>`` body when the
        assembly fails part way through, because the status line is already on the wire by then.
        ``expects=(200, 201)`` reads that as a completed upload."""
        provider = raw_provider(auth, credentials, settings)
        provider.generate_generic_presigned_url = MockCoroutine(return_value=SIGNED_URL)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<Error><Code>InternalError</Code>'
                '<Message>We encountered an internal error. Please try again.</Message>'
                '</Error>').encode('utf-8')
        aiohttpretty.register_uri('POST', SIGNED_URL, body=body, status=200)

        with pytest.raises(exceptions.UploadError) as e:
            await provider._complete_multipart_upload(
                WaterButlerPath('/my-subfolder/thefile.txt'), 'SESSION', [{'ETAG': 'abc'}])

        assert 'InternalError' in e.value.message
        assert_no_secrets(e.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_complete_accepts_a_200_carrying_a_result(self, auth, credentials, settings,
                                                            mock_time):
        """K-3: the success body must still be accepted."""
        provider = raw_provider(auth, credentials, settings)
        provider.generate_generic_presigned_url = MockCoroutine(return_value=SIGNED_URL)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<CompleteMultipartUploadResult>'
                '<Location>https://that-kerning.s3.amazonaws.com/my-subfolder/thefile.txt</Location>'
                '<Bucket>that-kerning</Bucket><Key>my-subfolder/thefile.txt</Key>'
                '<ETag>&quot;abc&quot;</ETag>'
                '</CompleteMultipartUploadResult>').encode('utf-8')
        aiohttpretty.register_uri('POST', SIGNED_URL, body=body, status=200)

        await provider._complete_multipart_upload(
            WaterButlerPath('/my-subfolder/thefile.txt'), 'SESSION', [{'ETAG': 'abc'}])

    @pytest.mark.asyncio
    async def test_chunked_upload_says_so_when_the_abort_succeeded(self, auth, credentials,
                                                                   settings, mock_time):
        """K-1: pin the ``if not aborted:`` branch.  The two messages differ in whether the user
        is told to go and clean up the leftover parts by hand."""
        provider = raw_provider(auth, credentials, settings)
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(return_value=[])
        provider._complete_multipart_upload = MockCoroutine(
            side_effect=exceptions.UploadError('nope', code=500))
        provider._abort_chunked_upload = MockCoroutine(return_value=True)

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert 'The upload is aborted.' in e.value.message
        assert 'manually remove them' not in e.value.message

    @pytest.mark.asyncio
    async def test_chunked_upload_says_so_when_the_abort_failed(self, auth, credentials, settings,
                                                                mock_time):
        """K-1: the other side of the same branch."""
        provider = raw_provider(auth, credentials, settings)
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(return_value=[])
        provider._complete_multipart_upload = MockCoroutine(
            side_effect=exceptions.UploadError('nope', code=500))
        provider._abort_chunked_upload = MockCoroutine(return_value=False)

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert 'manually remove them' in e.value.message
        assert 'The upload is aborted.' not in e.value.message


class TestCommitPreconditions:
    """K-5 / 決定-12: the commit has to be sent exactly once.

    CompleteMultipartUpload is not idempotent.  A re-send after the first attempt succeeded
    meets a consumed ``UploadId`` and comes back ``NoSuchUpload``, so whatever code is
    observed belongs to the *last* attempt and says nothing about the upload.  Two different
    mechanisms can re-send it, and they need separate stops: ``retry=0`` for WaterButler's
    own loop in ``make_request``, ``allow_redirects=False`` for aiohttp following a 307/308.
    """

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('status', [408, 502, 503, 504])
    async def test_commit_is_sent_exactly_once(self, auth, credentials, settings, mock_time,
                                               status):
        """Counting the POSTs pins that ``retry=0`` takes effect.  Inspecting the caller only
        pins that it is written down."""
        provider = raw_provider(auth, credentials, settings)
        provider.generate_generic_presigned_url = MockCoroutine(return_value=SIGNED_URL)
        error_body = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<Error><Code>SlowDown</Code>'
                      '<Message>Please reduce your request rate.</Message></Error>')
        aiohttpretty.register_uri('POST', SIGNED_URL, status=status,
                                  body=error_body.encode('utf-8'))

        with pytest.raises(exceptions.UploadError):
            await provider._complete_multipart_upload(
                WaterButlerPath('/my-subfolder/thefile.txt'), 'SESSION', [{'ETAG': 'abc'}])

        # Pin the retried statuses too, so that widening core's ``retry_on`` reports this
        # parameter set as no longer covering it.
        assert provider._retry_on == {408, 502, 503, 504}
        assert status in provider._retry_on
        assert len(aiohttpretty.calls) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize('redirect_status', [307, 308])
    async def test_commit_does_not_follow_a_redirect(self, auth, credentials, settings,
                                                     mock_time, redirect_status):
        """``retry=0`` stops only core's own retry loop.  A 307/308 says "resend with the
        method and body intact", and aiohttp follows it itself under the default
        ``allow_redirects=True``, so two commit POSTs go out without spending any of core's
        retry budget.

        ``aiohttpretty`` cannot pin this; see ``commit_server``."""
        calls = []

        async def first(request):
            await request.read()
            calls.append(request.path)
            raise web.HTTPTemporaryRedirect(location='/second') \
                if redirect_status == 307 else web.HTTPPermanentRedirect(location='/second')

        async def second(request):
            # Reached only if the redirect were followed.  It answers with a definitive
            # rejection code, so that following the redirect fails towards the dangerous
            # verdict rather than a harmless one.
            await request.read()
            calls.append(request.path)
            return web.Response(
                status=403, content_type='application/xml',
                text='<?xml version="1.0" encoding="UTF-8"?><Error>'
                     '<Code>SignatureDoesNotMatch</Code>'
                     '<Message>The request signature we calculated does not match.</Message>'
                     '</Error>')

        app = web.Application()
        app.router.add_post('/first', first)
        app.router.add_post('/second', second)

        provider = raw_provider(auth, credentials, settings)
        async with commit_server(provider, app) as server:
            provider.generate_generic_presigned_url = MockCoroutine(return_value=server.url)
            with pytest.raises(exceptions.UploadError):
                await provider._complete_multipart_upload(
                    WaterButlerPath('/my-subfolder/thefile.txt'), 'SESSION', [{'ETAG': 'abc'}])

        # Exactly one commit POST.  A second one records ``/second``, so a failure here shows
        # how far the request got.
        assert calls == ['/first']

    @pytest.mark.asyncio
    async def test_commit_request_states_both_preconditions(self, auth, credentials, settings,
                                                            mock_time):
        """NOTE_SEMANTICS_DESIGN v2.2 §4-2b: watch the preconditions directly, not only
        through their effect.  The two counting tests above go through aiohttp, so a future
        change that keeps the observable single-send by accident -- core dropping the retry
        loop, say -- would leave them green while the commit stopped declaring what it needs.
        """
        provider = raw_provider(auth, credentials, settings)
        provider.generate_generic_presigned_url = MockCoroutine(return_value=SIGNED_URL)
        provider.make_request = MockCoroutine(
            side_effect=exceptions.UploadError('nope', code=500))

        with pytest.raises(exceptions.UploadError):
            await provider._complete_multipart_upload(
                WaterButlerPath('/my-subfolder/thefile.txt'), 'SESSION', [{'ETAG': 'abc'}])

        _, kwargs = provider.make_request.call_args
        assert kwargs.get('retry') == 0
        assert kwargs.get('allow_redirects') is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize('method_name', ['_create_upload_session', '_upload_part',
                                             '_abort_chunked_upload'])
    async def test_the_other_upload_requests_keep_the_defaults(self, auth, credentials,
                                                               settings, method_name):
        """決定-12 scopes the two keywords to the commit.  Part transfers and session
        creation are idempotent enough that a re-send changes nothing the notice depends on,
        and turning core's retry off for them would trade a recoverable blip for a failed
        upload."""
        source = inspect.getsource(getattr(S3Provider, method_name))
        assert 'retry=' not in source
        assert 'allow_redirects' not in source
