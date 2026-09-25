import os
import io
import xml
import json
import time
import base64
import asyncio
import hashlib
import inspect
import logging
import aiohttp
import datetime
import traceback
import aiohttpretty
import botocore.auth
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
from waterbutler.core.utils import make_disposition
from waterbutler.core import streams, metadata, exceptions
from waterbutler.providers.s3 import settings as pd_settings
from waterbutler.providers.s3 import provider as pd_provider
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


@pytest.fixture(autouse=True)
def pinned_signing_clock():
    """Pin the clock botocore signs with, for every test in this module.

    T-1 / CX1-11: a test that answers the *real* presigner has to name the URL the presigner
    produces, and the only thing that moves between two otherwise identical signings is
    ``X-Amz-Date`` and the signature derived from it.  Freezing it here rather than at each call
    site means a test can go back to the real presigner by deleting the stub, without also
    having to re-indent its body into a ``with`` block.  Nothing else about signing is touched:
    parameter validation, serialisation and the HMAC all still run.
    """
    with frozen_signing_clock():
        yield


@pytest.fixture
def provider(auth, credentials, settings):
    """The shared provider, with only the region lookup stubbed.

    T-1 / CX1-11: this fixture used to install a hand-written ``generate_generic_presigned_url``
    and ``check_key_existence`` -- the first returned ``https://<bucket>.s3.amazonaws.com/<key>``
    from the path alone, ignoring the operation and the parameters entirely; the second re-made
    the same string.  Every test reached through it therefore asserted against a URL the test
    suite had invented, and the real presigner never ran.  Three ROUND1 majors lived in exactly
    that gap (CX1-1/2/3).  The stubs are gone; ``region`` is pinned so the endpoint the presigner
    signs against is stable, and :func:`pinned_signing_clock` pins the clock, which is what lets
    a test name the signed URL in advance.

    Identical to :func:`raw_provider`, which the tests that build a second provider -- a copy
    destination, a differently configured bucket -- call directly.
    """
    return raw_provider(auth, credentials, settings)


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


def folder_listing_params(path, max_keys=None, continuation_token=None):
    """The ListObjectsV2 parameters ``_metadata_folder`` signs for ``path``.

    T-1 / CX1-11: these are the parameters the provider passes, in the casing botocore wants --
    ``MaxKeys`` an int, because botocore validates parameter types before it signs (CX1-1).
    They go to the signing call, not to an assertion about the query string: the query string
    is now whatever botocore signed, and the test matches it by naming the whole URL.
    """
    params = {'Bucket': 'that-kerning', 'Prefix': path.path, 'Delimiter': '/'}
    if max_keys is not None:
        params['MaxKeys'] = max_keys
    if continuation_token:
        params['ContinuationToken'] = continuation_token
    return params


def list_objects_v2_response(keys, is_truncated=False, next_continuation_token=None,
                             common_prefixes=(), encoding_type='url'):
    """Build a ListObjectsV2 response body listing ``keys``.

    ``encoding_type`` defaults to ``'url'`` because that is what S3 answers: botocore puts
    ``encoding-type=url`` on every listing it signs, and S3 echoes the element back to say the
    key names in the body are percent-encoded.  Pass ``None`` for the bucket that was listed
    without it.
    """
    body = '<?xml version="1.0" encoding="UTF-8"?>'
    body += '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    body += '<Name>that-kerning</Name>'
    body += '<MaxKeys>1000</MaxKeys>'
    if encoding_type is not None:
        body += f'<EncodingType>{encoding_type}</EncodingType>'
    body += '<IsTruncated>{}</IsTruncated>'.format('true' if is_truncated else 'false')
    if next_continuation_token is not None:
        body += f'<NextContinuationToken>{next_continuation_token}</NextContinuationToken>'
    for prefix in common_prefixes:
        body += f'<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>'
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
                           next_key_marker=None, next_version_id_marker=None,
                           encoding_type='url'):
    """Build a ListObjectVersions response body.

    ``versions`` and ``delete_markers`` are iterables of ``(key, version_id)`` pairs.
    ``encoding_type`` defaults to ``'url'`` -- see :func:`list_objects_v2_response`.
    """
    body = '<?xml version="1.0" encoding="UTF-8"?>'
    body += '<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    body += '<Name>that-kerning</Name>'
    if encoding_type is not None:
        body += f'<EncodingType>{encoding_type}</EncodingType>'
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


class _OverriddenClientCtx:
    """The real ``create_client()`` context manager, with ``methods`` bound over the client it
    yields.  ``mock.AsyncMock`` needs Python 3.8+, hence the hand-written protocol."""

    def __init__(self, inner, methods):
        self._inner = inner
        self._methods = methods

    async def __aenter__(self):
        client = await self._inner.__aenter__()
        # botocore builds the API methods onto the client's class, so an instance attribute
        # shadows the one being replaced and leaves the rest of the client alone.
        for name, coroutine in self._methods.items():
            setattr(client, name, coroutine)
        return client

    async def __aexit__(self, *args):
        return await self._inner.__aexit__(*args)


def patch_aiobotocore_client(**methods):
    """Let the provider build a *real* aiobotocore client, then replace only ``methods`` on it.

    T-1 / CX1-11: client creation is not stubbed and ``generate_presigned_url`` is left alone,
    so a provider method that signs a URL on its way to the call under test still signs it with
    the real presigner -- the earlier version of this helper handed back a bare ``mock.Mock``,
    which made the presigner return a ``Mock`` too and forced every caller to stub it as well.
    What is replaced is the single API call whose arguments the test is asserting on.

    :return: ``(patcher, handle)`` -- use the patcher as a context manager; ``handle`` carries
        the same coroutine objects that were bound onto the client, so
        ``handle.delete_objects.assert_called_once_with(...)`` reads the real call
    """
    handle = mock.Mock()
    for name, coroutine in methods.items():
        setattr(handle, name, coroutine)

    def _get_session():
        session = aiobotocore_session.get_session()
        real_create_client = session.create_client
        session.create_client = lambda *a, **kw: _OverriddenClientCtx(
            real_create_client(*a, **kw), methods)
        return session

    patcher = mock.patch('waterbutler.providers.s3.provider.get_session', _get_session)
    return patcher, handle


def raw_provider(auth, credentials, settings):
    """A provider with only the region lookup stubbed, so that the real
    ``generate_generic_presigned_url`` and ``check_key_existence`` run."""
    prov = S3Provider(auth, credentials, settings)
    prov._check_region = MockCoroutine()
    prov.region = 'us-east-1'
    return prov


class _FrozenSigningClock(datetime.datetime):
    """``datetime.datetime`` whose ``utcnow()`` does not move."""

    @classmethod
    def utcnow(cls):
        return cls(2016, 2, 5, 14, 28, 50)


def frozen_signing_clock():
    """Pin the clock botocore signs with, so a presigned URL is reproducible.

    T-1 / CX1-11: the real presigner has to run -- it is what rejects a wrongly typed
    parameter, adds ``encoding-type=url`` and turns the parameters into the query string that
    actually goes on the wire.  Three ROUND1 majors hid behind a hand-written stand-in for it.
    Answering the real URL with ``aiohttpretty`` means the test has to name that URL, and the
    only thing that differs between two otherwise identical signings is ``X-Amz-Date`` (one
    second of resolution) and the signature derived from it.  This replaces the clock and
    nothing else: parameter validation, serialisation and the HMAC all still happen for real.
    """
    shim = mock.Mock()
    shim.datetime = _FrozenSigningClock
    return mock.patch.object(botocore.auth, 'datetime', shim)


async def register_presigned(provider, http_method, s3_method, path='', query_parameters=None,
                             default_params=False, **response):
    """Sign ``s3_method`` with the real presigner and answer that exact URL with ``response``.

    Call inside :func:`frozen_signing_clock` so that the URL signed here and the one the
    provider signs a moment later are the same string -- ``aiohttpretty`` matches on the whole
    query, signature included.

    :return: the presigned URL that was registered
    """
    url = await provider.generate_generic_presigned_url(
        path, s3_method, query_parameters=query_parameters, default_params=default_params)
    aiohttpretty.register_uri(http_method, url, **response)
    return url


def redirect_presigned_origin(provider, origin):
    """Send the provider's presigned requests to ``origin`` instead of to S3.

    T-1 / CX1-11: this is not a stand-in for the presigner.  The real
    ``generate_generic_presigned_url`` runs and its output is kept whole -- path, query,
    signature -- with only the scheme and host rewritten.  The endpoint the provider signs
    against is hard-coded to ``s3[.<region>].amazonaws.com``, and a test cannot listen there;
    redirecting the origin is the only way to put the presigner's own query on a real socket.
    Nothing that uses this checks the signature, which the rewritten host would invalidate.
    """
    real = provider.generate_generic_presigned_url
    prefix = parse.urlsplit(origin)[:2]

    async def _redirected(path, method='head_object', query_parameters=None,
                          default_params=True):
        url = await real(path, method, query_parameters=query_parameters,
                         default_params=default_params)
        return parse.urlunsplit(prefix + parse.urlsplit(url)[2:])

    provider.generate_generic_presigned_url = _redirected


def record_request_urls(provider):
    """Record the URL of every request ``provider`` makes, without changing any of them.

    T-1 / CX1-11: this is a spy, not a stand-in.  The real ``make_request`` runs, so the request
    still goes out over the session that ``aiohttpretty`` is standing in for; the wrapper only
    keeps a copy of the URL the real presigner produced, which is the only way to read the query
    that actually went on the wire byte for byte (``aiohttpretty`` hands back a ``furl`` whose
    arguments are already decoded).

    :return: the list of requested URLs, in order
    """
    requested = []
    real_make_request = provider.make_request

    async def _recording(method, url, *args, **kwargs):
        requested.append(url)
        return await real_make_request(method, url, *args, **kwargs)

    provider.make_request = _recording
    return requested


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

        await register_presigned(
            provider, 'GET', 'list_objects_v2', path='/my-subfolder/',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': '/my-subfolder/',
                              'Delimiter': '/', 'MaxKeys': 1},
            body=b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><Prefix>my-subfolder/</Prefix><IsTruncated>false</IsTruncated></ListBucketResult>',
            headers={'Content-Type': 'application/xml'},
        )
        await register_presigned(
            provider, 'HEAD', 'head_object', path=f'my-subfolder/{file_path}',
            default_params=True, headers=file_header_metadata,
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

        await register_presigned(
            provider, 'GET', 'list_objects_v2', path='/my-subfolder/',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': '/my-subfolder/',
                              'Delimiter': '/', 'MaxKeys': 1},
            body=b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><Prefix>my-subfolder/</Prefix><IsTruncated>false</IsTruncated></ListBucketResult>',
            headers={'Content-Type': 'application/xml'},
        )
        await register_presigned(
            provider, 'HEAD', 'head_object', path=f'my-subfolder{file_path}',
            default_params=True, headers=file_header_metadata,
        )

        assert WaterButlerPath('/my-subfolder/') == await provider.validate_v1_path('/')
        wb_path_v1 = await provider.validate_v1_path(file_path)
        wb_path_v0 = await provider.validate_path(file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata, mock_time):
        folder_path = '/Photos'

        await register_presigned(
            provider, 'GET', 'list_objects_v2', path=f'/my-subfolder{folder_path}/',
            query_parameters={'Bucket': 'that-kerning',
                              'Prefix': f'/my-subfolder{folder_path}/',
                              'Delimiter': '/', 'MaxKeys': 1},
            body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'},
        )
        await register_presigned(
            provider, 'HEAD', 'head_object', path=f'my-subfolder{folder_path}',
            default_params=True, status=404,
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
        await register_presigned(
            provider, 'GET', 'get_object', path=path.path, default_params=True,
            query_parameters={'ResponseContentDisposition': make_disposition(path.name)},
            body=b'delicious', auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = await register_presigned(
            provider, 'GET', 'get_object', path=path.path, default_params=True,
            query_parameters={'ResponseContentDisposition': make_disposition(path.name)},
            body=b'de', auto_length=True, status=206)

        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        assert content == b'de'
        assert aiohttpretty.has_call(method='GET', uri=url, headers={'Range': 'bytes=0-1'})

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_version(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        await register_presigned(
            provider, 'GET', 'get_object', path=path.path, default_params=True,
            query_parameters={'VersionId': 'someversion',
                              'ResponseContentDisposition': make_disposition(path.name)},
            body=b'delicious', auto_length=True)

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
        # The disposition is signed into the URL, so naming the expected one here is what makes
        # this test about which name S3 is asked to hand back.
        url = await register_presigned(
            provider, 'GET', 'get_object', path=path.path, default_params=True,
            query_parameters={'ResponseContentDisposition': make_disposition(expected_name)},
            body=b'delicious', auto_length=True)

        result = await provider.download(path, display_name=display_name_arg)
        content = await result.read()

        assert content == b'delicious'
        assert aiohttpretty.has_call(method='GET', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        await register_presigned(
            provider, 'GET', 'get_object', path=path.path, default_params=True,
            query_parameters={'ResponseContentDisposition': make_disposition(path.name)},
            status=404)

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

        # PUT and HEAD are signed separately -- the HTTP method is part of the canonical request,
        # so these are two different URLs even though they name the same key.
        metadata_url = await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            headers=file_header_metadata)
        url = await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True,
            status=201, headers={'ETag': f'"{content_md5}"'})

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
        metadata_url = await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            headers=file_header_metadata)
        url = await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True,
            status=201, headers={'ETag': f'"{content_md5}"'})

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
        metadata_url = await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )
        # `encrypt_uploads` puts `ServerSideEncryption` into the signed parameters as well as
        # into the header, so the encrypted upload is a different URL from the plain one.
        url = await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True,
            query_parameters={'ServerSideEncryption': 'AES256'},
            status=200, headers={'ETag': f'"{content_md5}"'})

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
        init_url = await register_presigned(
            provider, 'POST', 'create_multipart_upload', path=path.path, default_params=True,
            body=create_session_resp, status=200)

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
        # `ServerSideEncryption` is a header parameter, so botocore signs it into
        # ``X-Amz-SignedHeaders`` rather than into the query -- which is why the encrypted
        # session is a different signature from the plain one above.
        init_url = await register_presigned(
            provider, 'POST', 'create_multipart_upload', path=path.path, default_params=True,
            query_parameters={'ServerSideEncryption': 'AES256'},
            body=create_session_resp, status=200)

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
    async def test_chunked_upload_upload_part(self, auth, credentials, settings, file_stream,
                                              upload_parts_headers_list):
        """T-1 / CX1-2: the part goes to the URL the real presigner signed, and to nothing
        else.  ``PartNumber`` and ``UploadId`` are already in that URL, so the request must
        not name them again."""
        provider = raw_provider(auth, credentials, settings)
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah')
        chunk_number = 1
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        # aiohttp resp headers use upper case
        part_headers = json.loads(upload_parts_headers_list).get('headers_list')[0]
        part_headers = {k.upper(): v for k, v in part_headers.items()}

        with frozen_signing_clock():
            upload_part_url = await register_presigned(
                provider, 'PUT', 'upload_part', path=path.path, default_params=True,
                query_parameters={'ContentLength': provider.CHUNK_SIZE,
                                  'PartNumber': chunk_number, 'UploadId': upload_id},
                status=200, headers=part_headers)
            part_metadata = await provider._upload_part(file_stream, path, upload_id,
                                                        chunk_number, provider.CHUNK_SIZE)

        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url)
        assert part_headers == part_metadata

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

        complete_url = await register_presigned(
            provider, 'POST', 'complete_multipart_upload', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            status=200, body=complete_upload_resp)

        await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_session_deleted(self, provider, generic_http_404_resp,
                                                        mock_time):
        path = WaterButlerPath('/foobah')
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = await register_presigned(
            provider, 'DELETE', 'abort_multipart_upload', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id}, status=204)
        await register_presigned(
            provider, 'GET', 'list_parts', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            body=generic_http_404_resp, status=404)

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
        abort_url = await register_presigned(
            provider, 'DELETE', 'abort_multipart_upload', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id}, status=204)
        list_url = await register_presigned(
            provider, 'GET', 'list_parts', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            body=list_parts_resp_empty, status=200)

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
        abort_url = await register_presigned(
            provider, 'DELETE', 'abort_multipart_upload', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id}, status=204)
        await register_presigned(
            provider, 'GET', 'list_parts', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            body=list_parts_resp_not_empty, status=200)

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
        list_url = await register_presigned(
            provider, 'GET', 'list_parts', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            body=generic_http_404_resp, status=404)

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
        list_url = await register_presigned(
            provider, 'GET', 'list_parts', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            body=list_parts_resp_empty, status=200)

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
        list_url = await register_presigned(
            provider, 'GET', 'list_parts', path=path.path, default_params=True,
            query_parameters={'UploadId': upload_id},
            body=list_parts_resp_not_empty, status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-file'},
                body=list_versions_response(
                    versions=[('some-file', 'version-two'), ('some-file', 'version-one')],
                    delete_markers=[('some-file', 'marker-one')],
                ),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-file'},
                body=list_versions_response(
                    versions=[('some-file', 'version-one'), ('some-file.bak', 'version-bak')],
                ),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-file'},
                body=list_versions_response(versions=[('some-file', 'null')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-file'},
                body=list_versions_response(),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-file'},
                body=list_versions_response(
                    versions=[('some-file', 'version-two'), ('some-file', 'version-one')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-file'},
                body=b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code>'
                     b'<Message>Access Denied</Message></Error>',
            status=status)

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
                                                                mock_time):
        """V-5: transport failures are not WaterButlerErrors and would otherwise escape
        delete() unconverted."""
        path = WaterButlerPath('/some-file')

        async def _fail(*args, **kwargs):
            raise transport_error()

        # Restore inside the test body, not at teardown.  ``aiohttpretty`` has already
        # replaced ``ClientSession._request`` by the time this runs, so whatever saves the
        # attribute here saves *its* fake.  ``monkeypatch`` undoes at teardown, and the
        # conftest hook that deactivates aiohttpretty runs first -- so the undo would put
        # the fake back after the real method had been restored, and every later test that
        # needs a real socket would be answered by a deactivated aiohttpretty.
        with mock.patch.object(aiohttp.ClientSession, '_request', _fail):
            with pytest.raises(exceptions.DeleteError) as exc_info:
                await provider.delete(path)

        assert transport_error.__name__ in exc_info.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_confirm_delete(self, provider, mock_time):
        path = WaterButlerPath('/')

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': ''},
                body=list_versions_response(
                    versions=[('some-folder/', 'v1'), ('some-folder/file.txt', 'v2')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'folder-to-delete/'},
                body=list_versions_response(
                    versions=[('folder-to-delete/file1.txt', '111'),
                              ('folder-to-delete/file1.txt', '222')],
                    delete_markers=[('folder-to-delete/file2.txt', '333')],
                ),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'single-thing-folder/'},
                body=list_versions_response(versions=[('single-thing-folder/item', 'v1')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'empty-folder/'},
                body=list_versions_response(versions=[('empty-folder/', 'v1')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'not-found-folder/'},
                body=list_versions_response(),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'tombstone-folder/'},
                body=list_versions_response(
                    delete_markers=[('tombstone-folder/file1.txt', '111')]),
            status=200)

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

        keys = [f'some-folder/file-{index:05d}' for index in range(1001)]
        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'some-folder/'},
                body=list_versions_response(versions=[(key, 'v1') for key in keys]),
            status=200)

        patcher, s3_client = patch_aiobotocore_client(
            delete_objects=MockCoroutine(return_value={'Deleted': [], 'Errors': []}))
        with patcher:
            await provider.delete(path)

        batches = [call[1]['Delete']['Objects'] for call in s3_client.delete_objects.call_args_list]
        assert [len(batch) for batch in batches] == [1000, 1]
        assert [entry['Key'] for batch in batches for entry in batch] == keys

    @pytest.mark.asyncio
    async def test_a_refusal_in_a_later_batch_is_reported(self, auth, credentials, settings,
                                                          monkeypatch, mock_time):
        """CX2-2 / V-4: every batch's body is read, not just the first one's.

        DeleteObjects answers 200 and lists the refusals inside the body, so the check belongs
        to each call rather than to the loop's outcome.  The partial-failure tests above use a
        single batch, where "the last response" and "every response" cannot be told apart --
        code that checked only the first, or only the last, or that broke out of the loop after
        the first success would pass them all.  This sends 1001 objects so that the loop runs
        twice and puts the refusal in the *second* answer.

        The responses are injected at ``before-send``, so botocore serialises the 1001-object
        request, signs it and parses the XML back into the ``Errors`` list the provider reads.
        """
        provider = raw_provider(auth, credentials, settings)
        delete_requests = [{'Key': 'some-folder/file-{:05d}'.format(index), 'VersionId': 'v1'}
                           for index in range(1001)]
        sent = patch_session_with_before_send(
            monkeypatch,
            serve_in_order(
                _FakeHTTPResponse(200, delete_objects_response(
                    deleted=[(entry['Key'], 'v1') for entry in delete_requests[:1000]])),
                _FakeHTTPResponse(200, delete_objects_response(
                    errors=[('some-folder/file-01000', 'v1', 'AccessDenied')])),
            ),
            operation='DeleteObjects')

        with pytest.raises(exceptions.DeleteError) as exc_info:
            await provider.delete_objects_in_chunks('/some-folder/', delete_requests)

        assert len(sent) == 2
        assert 'some-folder/file-01000' in exc_info.value.message
        assert 'AccessDenied' in exc_info.value.message
        # The count names the batch, not the whole listing: "1 of 1" says the survivor is the
        # only object in the second batch.
        assert '1 of 1 objects' in exc_info.value.message

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_truncated_response(self, provider, mock_time):
        """V-6: a folder holding more than one page of versions must be listed to the end
        before any of it is deleted, otherwise the tail of the folder silently survives.
        ListObjectVersions resumes from the last key *and* version id, not a continuation
        token."""
        path = WaterButlerPath('/large-folder/')

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'large-folder/'},
            body=list_versions_response(versions=[('large-folder/file1.txt', '111')],
                                        is_truncated=True,
                                        next_key_marker='large-folder/file2.txt',
                                        next_version_id_marker='222'),
            status=200)
        page_two_url = await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'large-folder/',
                              'KeyMarker': 'large-folder/file2.txt',
                              'VersionIdMarker': '222'},
            body=list_versions_response(versions=[('large-folder/file2.txt', '222')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'error-folder/'},
                body=list_versions_response(versions=[('error-folder/file1.txt', '111'),
                                                      ('error-folder/file2.txt', '222')]),
            status=200)

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

        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'error-folder/'},
                body=list_versions_response(versions=[('error-folder/file1.txt', '111')]),
            status=200)

        patcher, _ = patch_aiobotocore_client(
            delete_objects=MockCoroutine(side_effect=Exception('AccessDenied')))
        with patcher:
            with pytest.raises(exceptions.DeleteError):
                await provider.delete(path)

    # CX1-13: a folder delete whose listing fails is asserted by
    # `TestErrorReporting.test_delete_folder_reports_a_failed_listing_as_a_delete_failure`,
    # which expects the `DeleteError` this now raises rather than the `DownloadError` that used
    # to reach the caller, and runs through the real presigner (T-1 / CX1-11).

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('revision,display_name,expected_name', [
        (None, None, 'my-image'),
        ('latest', 'meow.txt', 'meow.txt'),
        ('someversion', None, 'my-image'),
    ])
    async def test_download_accept_url_answers_with_the_signed_url(
            self, auth, credentials, settings, mock_time, revision, display_name, expected_name):
        """G-10 / 決定-19: ``accept_url=True`` hands back the presigned URL to redirect to.

        This is the default on every download the API serves --
        ``waterbutler.server.api.v1.provider.metadata`` passes ``'direct' not in query`` -- and
        it redirects whenever ``download`` answers with a ``str``.  develop returns the URL here,
        so the file goes from S3 to the browser directly; losing it routed every byte of every
        download through WaterButler instead, which is a change to GRDM's default behaviour and
        not one anybody asked for.

        The URL is compared against what the presigner produces for the same call rather than
        against a pattern, because what makes it usable is that it is signed over exactly the
        parameters the streaming path would have used: the version being asked for and the
        ``Content-Disposition`` that gives the download its filename.

        T-1 / CX1-11: the real presigner produces both sides of the comparison.
        """
        provider = raw_provider(auth, credentials, settings)
        path = WaterButlerPath('/my-subfolder/my-image')
        query_parameters = {'ResponseContentDisposition': make_disposition(expected_name)}
        if revision == 'someversion':
            query_parameters['VersionId'] = revision

        with frozen_signing_clock():
            expected = await provider.generate_generic_presigned_url(
                path.path, 'get_object', query_parameters=query_parameters)
            url = await provider.download(path, accept_url=True, revision=revision,
                                          display_name=display_name)

        assert isinstance(url, str), 'download streamed instead of answering with a URL'
        # Compared field by field rather than as a string: the query is a mapping, and the order
        # the parameters happen to be written in is not part of what S3 is being asked for.  The
        # signature is in there and is checked like any other field, so a URL signed over
        # different parameters still fails.
        assert parse.urlsplit(url)[:3] == parse.urlsplit(expected)[:3]
        assert (parse.parse_qs(parse.urlsplit(url).query)
                == parse.parse_qs(parse.urlsplit(expected).query))
        # Nothing was fetched: the point of the redirect is that WaterButler does not carry the
        # bytes.  A request here would mean the file was downloaded once to be handed over.
        assert aiohttpretty.calls == []

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_without_accept_url_still_streams(self, provider, mock_time):
        """G-10: ``?direct`` -- the one case where the server asks for the bytes -- is unchanged."""
        path = WaterButlerPath('/my-image')
        await register_presigned(
            provider, 'GET', 'get_object', path=path.path, default_params=True,
            query_parameters={'ResponseContentDisposition': make_disposition(path.name)},
            body=b'content', auto_length=True)

        result = await provider.download(path, accept_url=False)
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
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path),
            body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'})

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
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path, max_keys=1000),
            body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'})

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
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path, max_keys=1000),
            body=folder_metadata if isinstance(folder_metadata, bytes) else folder_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'})

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
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path),
            body=folder_and_contents if isinstance(folder_and_contents, bytes) else folder_and_contents.encode('utf-8'))

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result:
            assert fobj.name != path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata_folder_item(self, provider, folder_item_metadata, mock_time):
        path = WaterButlerPath('/')
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path),
            body=folder_item_metadata if isinstance(folder_item_metadata, bytes) else folder_item_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_metadata_folder(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/this-is-not-the-root/')
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path),
            body=folder_empty_metadata if isinstance(folder_empty_metadata, bytes) else folder_empty_metadata.encode('utf-8'),
            headers={'Content-Type': 'application/xml'})
        # An empty listing sends `_metadata_folder` on to `check_key_existence`, to tell a folder
        # that exists only as a trailing-slash key from one that is not there at all.
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            headers=file_header_metadata)

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
        # ``Latest`` is normalised away before the signing call, so this is the plain
        # ``head_object`` -- no ``VersionId`` in the signed parameters.
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            headers=file_header_metadata)

        result = await provider.metadata(path, revision='Latest')

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        """A HEAD that answers 404 reaches the caller as ``NotFoundError``.

        T-1 / CX1-11: this used to assert ``MetadataError``, which is what the *stub*
        ``check_key_existence`` raised -- it called ``make_request(..., throws=MetadataError)``
        and stopped there.  The real one wraps that call in the ``except`` that re-raises
        through ``_raise_from_client_error`` as ``NotFoundError``, because ``BaseProvider.exists``
        reads a ``NotFoundError`` of any status as "no" and every caller arrives through it.
        So the exception the provider actually produces is the one named here.
        """
        path = WaterButlerPath('/notfound.txt')
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True, status=404)

        with pytest.raises(exceptions.NotFoundError):
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
        # PUT and HEAD are signed separately -- the HTTP method is part of the canonical
        # request, so these are two different URLs even though they name the same key.
        metadata_url = await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ])
        url = await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True,
            status=200, headers={'ETag': f'"{content_md5}"'})

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
        metadata_url = await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ])
        url = await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True,
            status=200, headers={'ETag': '"bad hash"'})

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

    T-1 / CX1-11: every request here is signed by the real presigner.  The earlier version of
    this class handed the provider a stand-in that accepted any parameter and pasted it into a
    query string, which is why ``MaxKeys='1000'`` -- a value botocore refuses outright -- read
    as covered (CX1-1).  Nothing below substitutes the presigner: the injection is at the HTTP
    boundary, and the URL registered there is the one the presigner produced.
    """

    PREFIX = 'darp/'

    def _params(self, **extra):
        params = {'Bucket': 'that-kerning', 'Prefix': self.PREFIX, 'Delimiter': '/'}
        params.update(extra)
        return params

    async def _register_page(self, provider, keys, is_truncated=False,
                             next_continuation_token=None, common_prefixes=(),
                             encoding_type='url', **extra):
        """Answer the ListObjectsV2 page selected by ``extra`` with a listing of ``keys``."""
        return await register_presigned(
            provider, 'GET', 'list_objects_v2', query_parameters=self._params(**extra),
            body=list_objects_v2_response(keys, is_truncated=is_truncated,
                                          next_continuation_token=next_continuation_token,
                                          common_prefixes=common_prefixes,
                                          encoding_type=encoding_type),
            headers={'Content-Type': 'application/xml'},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('next_token, sent_token', [
        # The API layer passes `next_token` for every page; `metadata()` turns a `None` into
        # the empty string, so both name the first page.
        (None, None),
        ('', None),
        # S3 hands back an opaque token and the UI hands it straight back.  A real one is
        # base64 and contains characters that have to survive query encoding: this stands in
        # for the worst of them.
        ('t/ok en+/=&?#%', 't/ok en+/=&?#%'),
    ])
    async def test_a_page_request_reaches_s3(self, auth, credentials, settings,
                                             next_token, sent_token):
        """P-1 / P-2 / P-5 / CX1-1: the paging parameters are ones botocore will sign.

        On ``ca65500e`` ``MaxKeys`` is the string ``'1000'``; botocore raises
        ``ParamValidationError`` before anything is signed and the provider converts that into
        a 404, so no request is made at all and every one of these cases fails.
        """
        provider = raw_provider(auth, credentials, settings)
        extra = {'MaxKeys': 1000}
        if sent_token is not None:
            extra['ContinuationToken'] = sent_token

        with frozen_signing_clock():
            await self._register_page(provider, ['darp/a.txt', 'darp/b.txt'],
                                      is_truncated=True,
                                      next_continuation_token='page-2-token', **extra)
            result = await provider.metadata(WaterButlerPath('/darp/'), next_token=next_token)

        assert [item.name for item in result[:-1]] == ['a.txt', 'b.txt']
        assert result[-1] == 'page-2-token'
        # One page means one request -- the provider must not drain the listing here.
        assert len(aiohttpretty.calls) == 1
        sent = aiohttpretty.calls[0]['uri'].params
        assert sent['max-keys'] == '1000'
        assert sent.get('continuation-token') == sent_token

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_last_page_has_no_trailing_token(self, auth, credentials, settings):
        """P-4: ``IsTruncated`` false means the caller must not see a str at the end."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_page(provider, ['darp/a.txt', 'darp/b.txt'], MaxKeys=1000)
            result = await provider.metadata(WaterButlerPath('/darp/'), next_token='')

        assert not isinstance(result[-1], str)
        assert [item.name for item in result] == ['a.txt', 'b.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_listing_without_next_token_returns_every_page(self, auth, credentials,
                                                                 settings):
        """Regression guard: ``metadata(path)`` with no ``next_token`` keyword must return the
        complete listing and nothing but metadata objects.

        ``BaseProvider._folder_file_op`` reads ``item.name`` off every element and
        ``ZipStreamGenerator`` feeds every element to ``path_from_metadata``; a str token among
        them raises ``AttributeError`` mid-copy or mid-download.

        No ``MaxKeys`` goes on these requests, which is what makes this the control case for
        CX1-1: the drain path signs cleanly on ``ca65500e`` too.
        """
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_page(provider, ['darp/a.txt'], is_truncated=True,
                                      next_continuation_token='page-2-token')
            await self._register_page(provider, ['darp/b.txt'],
                                      ContinuationToken='page-2-token')
            result = await provider.metadata(WaterButlerPath('/darp/'))

        assert [item.name for item in result] == ['a.txt', 'b.txt']
        assert not any(isinstance(item, str) for item in result)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_encoded_names_are_decoded(self, auth, credentials, settings):
        """CX1-3: keys and common prefixes are percent-encoded when the listing says so."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_page(provider, ['darp/a%2Bb.txt', 'darp/x%20y.txt'],
                                      common_prefixes=['darp%2Fsub%20dir%2F'], MaxKeys=1000)
            result = await provider.metadata(WaterButlerPath('/darp/'), next_token='')

        assert sorted(item.name for item in result) == ['a+b.txt', 'sub dir', 'x y.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_a_listing_that_is_not_encoded_is_read_verbatim(self, auth, credentials,
                                                                  settings):
        """CX1-3: ``key.replace('+', ' ')`` is the form-encoding rule, and S3 does not use it.

        Under ``encoding-type=url`` a literal ``+`` arrives as ``%2B`` and a space as ``%20``,
        so that replace can only ever fire on a name that was *not* encoded -- where it
        renames the object.  A user who uploads ``a+b.txt`` then cannot download it.
        """
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_page(provider, ['darp/a+b.txt'], encoding_type=None,
                                      MaxKeys=1000)
            result = await provider.metadata(WaterButlerPath('/darp/'), next_token='')

        assert [item.name for item in result] == ['a+b.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_handle_data_leaves_a_single_file_alone(self, auth, credentials, settings,
                                                          file_header_metadata):
        """P-3: a file's metadata is not a listing, so nothing may be popped off it."""
        provider = raw_provider(auth, credentials, settings)
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')

        with frozen_signing_clock():
            await register_presigned(provider, 'HEAD', 'head_object', path=path.path,
                                     default_params=True, headers=file_header_metadata)
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
        body = folder_metadata if isinstance(folder_metadata, bytes) \
            else folder_metadata.encode('utf-8')
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path), body=body,
            headers={'Content-Type': 'application/xml'})

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
        empty_xml = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><IsTruncated>false</IsTruncated></ListBucketResult>'
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path), status=200, body=empty_xml,
            headers={'Content-Type': 'application/xml'})
        # The empty listing sends the precheck on to `check_key_existence`; 404 there is what
        # makes `exists` answer "no" and lets the creation proceed to the PUT.
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True, status=404)
        await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True, status=403)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out_metadata(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path), status=403)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/')
        empty_xml = b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>that-kerning</Name><IsTruncated>false</IsTruncated></ListBucketResult>'
        await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=folder_listing_params(path), status=200, body=empty_xml,
            headers={'Content-Type': 'application/xml'})
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True, status=404)
        await register_presigned(
            provider, 'PUT', 'put_object', path=path.path, default_params=True, status=200)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/doesntalreadyexists/'


class TestOperations:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_adds_bucket_to_presigned_params(self, provider, mock_time):
        """The caller passes only a ``Prefix``.  ``Bucket`` has to be filled in here because
        ListObjectVersions is signed over its parameters -- a missing bucket is not a default
        botocore supplies later, the call fails to sign.

        T-1 / CX1-11: asserted against the URL the real presigner produced and the request that
        was actually made with it, rather than against the arguments it was called with.  In a
        signed URL the bucket is the path and the prefix is a query parameter, so a test that
        only inspects the call arguments cannot tell a bucket that was signed in from one that
        was dropped on the way.
        """
        url = await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': provider.bucket_name, 'Prefix': 'my-image.jpg',
                              'Delimiter': '/'},
            body=list_versions_response(), status=200)

        await provider.get_object_versions({'Prefix': 'my-image.jpg', 'Delimiter': '/'})

        assert aiohttpretty.has_call(method='GET', uri=url)
        split = parse.urlsplit(url)
        assert split.path == '/{}'.format(provider.bucket_name)
        assert 'versions' in split.query
        query = parse.parse_qs(split.query)
        assert query['prefix'] == ['my-image.jpg']
        assert query['delimiter'] == ['/']

    @pytest.mark.asyncio
    async def test_intra_copy(self, provider, file_metadata_object, mock_time):
        source_path = WaterButlerPath('/source')
        dest_path = WaterButlerPath('/dest')

        # The destination is a second provider, not the object under test; ``exists=True`` is what
        # makes ``intra_copy`` report ``created`` False.
        dest_provider = mock.Mock()
        dest_provider.exists = MockCoroutine(return_value=True)
        dest_provider.metadata = MockCoroutine(return_value=file_metadata_object)
        dest_provider.bucket_name = provider.bucket_name
        # 決定-20: the client is built from the destination's credentials and region.
        dest_provider.aws_access_key_id = provider.aws_access_key_id
        dest_provider.aws_secret_access_key = provider.aws_secret_access_key
        dest_provider.region = provider.region
        dest_provider._check_region = MockCoroutine()

        # T-1 / CX1-11: this used to hand `get_session` a bare `mock.Mock()`, so no aiobotocore
        # client was ever built and the CopySource the provider assembles was checked against a
        # client that would have accepted anything.  `patch_aiobotocore_client` builds the real
        # client and shadows only `copy_object`, so the arguments asserted below are the ones a
        # real client received.
        patcher, client = patch_aiobotocore_client(copy_object=MockCoroutine(return_value={}))

        with patcher:
            metadata, exists = await provider.intra_copy(dest_provider, source_path, dest_path)

        assert metadata.kind == 'file'
        assert not exists
        provider._check_region.assert_called()
        client.copy_object.assert_called_once_with(
            Bucket=provider.bucket_name,
            Key=dest_path.path,
            CopySource={'Bucket': provider.bucket_name, 'Key': source_path.path},
        )

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_metadata(self, provider, version_metadata, mock_time):
        path = WaterButlerPath('/my-image.jpg')
        body = version_metadata if isinstance(version_metadata, bytes) \
            else version_metadata.encode('utf-8')
        url = await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': path.path, 'Delimiter': '/'},
            status=200, body=body)

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
        body = single_version_metadata if isinstance(single_version_metadata, bytes) \
            else single_version_metadata.encode('utf-8')
        url = await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': path.path, 'Delimiter': '/'},
            status=200, body=body)

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


def delete_objects_response(deleted=(), errors=()):
    """Build a DeleteObjects response body.

    ``deleted`` is an iterable of ``(key, version_id)``; ``errors`` one of
    ``(key, version_id, code)``.  ``Quiet`` is false on every call the provider makes, so a
    successful delete is reported element by element rather than by an empty body.
    """
    body = '<?xml version="1.0" encoding="UTF-8"?>'
    body += '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    for key, version_id in deleted:
        body += ('<Deleted><Key>{}</Key><VersionId>{}</VersionId>'
                 '</Deleted>'.format(key, version_id))
    for key, version_id, code in errors:
        body += ('<Error><Key>{}</Key><VersionId>{}</VersionId><Code>{}</Code>'
                 '<Message>The operation was refused.</Message>'
                 '</Error>'.format(key, version_id, code))
    body += '</DeleteResult>'
    return body.encode('utf-8')


def serve_in_order(*responses):
    """A ``before-send`` factory that answers with each of ``responses`` in turn.

    The factory takes no arguments, so a test that needs the second call to differ from the
    first has nowhere else to put the difference.  Running out is an error rather than a repeat
    of the last response: a provider that sent one batch too many would otherwise be answered
    as if it had not.
    """
    remaining = list(responses)

    def factory():
        assert remaining, 'more requests were sent than this test has answers for'
        return remaining.pop(0)

    return factory


class TestIntraCopy:
    """I-2〜I-5: the ``intra_copy`` contract and how it reports provider failures."""

    def _dest_provider(self, provider, file_metadata_object, exists):
        dest_provider = mock.Mock()
        dest_provider.exists = MockCoroutine(return_value=exists)
        dest_provider.metadata = MockCoroutine(return_value=file_metadata_object)
        dest_provider.bucket_name = provider.bucket_name
        # 決定-20: the copy is signed by the destination, so these are read for real now.  The
        # same values as the source keep these tests measuring error reporting rather than
        # credentials -- which is asserted separately, against two distinct real providers, by
        # `test_intra_copy_is_signed_with_the_destination_credentials`.
        dest_provider.aws_access_key_id = provider.aws_access_key_id
        dest_provider.aws_secret_access_key = provider.aws_secret_access_key
        dest_provider.region = provider.region
        dest_provider._check_region = MockCoroutine()
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
    @pytest.mark.parametrize('error', [
        botocore.exceptions.EndpointConnectionError(endpoint_url='https://s3.amazonaws.com'),
        botocore.exceptions.ReadTimeoutError(endpoint_url='https://s3.amazonaws.com'),
        botocore.exceptions.ParamValidationError(report='Key must be a string'),
        aiohttp.ClientPayloadError('the response body ended early'),
    ])
    async def test_intra_copy_converts_every_failure_botocore_can_raise(
            self, provider, file_metadata_object, mock_time, error):
        """CX1-8 / K-7 / K-8: ``ClientError`` is the one failure that means "S3 answered".  The
        endpoint being unreachable, the read timing out, a parameter failing validation before
        anything is sent and the body ending early are the ordinary rest, and each one used to
        travel out of ``intra_copy`` unconverted -- reaching the API layer as a bare 500 whose
        message is botocore's own, which names the endpoint.

        The other five aiobotocore call sites already catch ``Exception``; this is the sixth.
        """
        dest_provider = self._dest_provider(provider, file_metadata_object, exists=False)
        patcher, client = patch_aiobotocore_client(
            copy_object=MockCoroutine(side_effect=error))

        with patcher:
            with pytest.raises(exceptions.IntraCopyError) as exc_info:
                await provider.intra_copy(dest_provider, WaterButlerPath('/source'),
                                          WaterButlerPath('/dest'))

        assert type(error).__name__ in exc_info.value.message
        assert_no_secrets(exc_info.value)
        assert_context_suppressed(exc_info.value)

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
    async def test_intra_copy_is_signed_with_the_destination_credentials(
            self, auth, credentials, settings, file_metadata_object, monkeypatch, mock_time):
        """決定-20 / CX1-7: CopyObject is signed by ``dest_provider``, as the docstring says.

        ``intra_copy``'s own contract -- "the credentials specified in `dest_provider` must have
        read access to `source.bucket`" -- is the one develop implements: it signs the PUT with
        the destination's key.  This provider was signing with the *source's* instead, so the
        stated requirement bought the user nothing: granting the destination read access to the
        source did not make the copy work, and the copy that did work was the one where the
        source could write to the destination -- the opposite permission, and one no
        documentation asks anybody to grant.  A copy between two S3 addons with different keys
        therefore failed with an AccessDenied the operator had no way to read.

        The region goes with the credentials.  SigV4 signs the region into the scope and the
        host into the request, and CopyObject is a write *to the destination bucket*, so both
        have to be the destination's; signing a destination in ap-northeast-1 with a us-east-1
        scope is rejected before the object is ever read.

        Injected at the transport boundary (``before-send``), so the request examined here is
        the one botocore actually signed.
        """
        source = raw_provider(auth, credentials, settings)
        dest = raw_provider(
            auth,
            {'access_key': 'DESTACCESSKEY', 'secret_key': 'dest-secret-key'},
            {'id': 'other-kerning:/', 'bucket': 'other-kerning', 'encrypt_uploads': False})
        dest.region = 'ap-northeast-1'
        # The destination's own lookups are not what is being measured; the copy request is.
        dest.exists = MockCoroutine(return_value=False)
        dest.metadata = MockCoroutine(return_value=file_metadata_object)

        sent = patch_session_with_before_send(
            monkeypatch, lambda: _FakeHTTPResponse(200, COPY_OBJECT_SUCCESS_BODY))

        await source.intra_copy(dest, WaterButlerPath('/source.txt'),
                                WaterButlerPath('/dest.txt'))

        assert len(sent) == 1
        authorization = sent[0].headers['Authorization'].decode('utf-8')
        assert 'Credential=DESTACCESSKEY/' in authorization
        assert '/ap-northeast-1/s3/aws4_request' in authorization
        assert 'Credential={}/'.format(source.aws_access_key_id) not in authorization
        assert parse.urlsplit(sent[0].url).netloc == 's3.ap-northeast-1.amazonaws.com'
        # The object still comes *from* the source bucket: this is about who signs, not what is
        # copied.
        copy_source = parse.unquote(sent[0].headers['x-amz-copy-source'].decode('utf-8'))
        assert source.bucket_name in copy_source
        assert 'source.txt' in copy_source
        assert parse.urlsplit(sent[0].url).path == '/other-kerning/dest.txt'

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

    async def _register_copy_traffic(self, provider, file_content, file_header_metadata):
        """Answer the three requests a stream copy makes, each on its own signed URL.

        T-1 / CX1-11: the download, the destination's existence check and the upload are three
        different operations on two different keys, so the real presigner gives three distinct
        URLs.  The old stub collapsed them to two strings derived from the path alone, which is
        why ``match_querystring=False`` used to be needed here.
        """
        headers = dict(file_header_metadata)
        headers['Content-Length'] = str(len(file_content))

        src_url = await register_presigned(
            provider, 'GET', 'get_object', path='source.txt', default_params=True,
            query_parameters={'ResponseContentDisposition': make_disposition('source.txt')},
            body=file_content, headers={'Content-Length': str(len(file_content))}, status=200)
        await register_presigned(
            provider, 'HEAD', 'head_object', path='dest.txt', default_params=True,
            responses=[{'status': 404}, {'headers': headers}])
        dest_url = await register_presigned(
            provider, 'PUT', 'put_object', path='dest.txt', default_params=True, status=200,
            headers={'ETag': '"{}"'.format(hashlib.md5(file_content).hexdigest())})
        return src_url, dest_url

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_copy_over_limit_falls_back_to_stream_copy(self, provider, file_content,
                                                             file_header_metadata, mock_time):
        calls = self._spy_on_intra(provider)
        src_url, dest_url = await self._register_copy_traffic(provider, file_content,
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
        src_url, dest_url = await self._register_copy_traffic(provider, file_content,
                                                              file_header_metadata)
        # The source is deleted after the copy; it has a single version and no delete markers.
        # T-1 / CX1-11: ListObjectVersions carries the prefix in the signed query string, so
        # this is its own URL rather than the bare bucket one the stub used to produce.
        await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters={'Bucket': 'that-kerning', 'Prefix': 'source.txt'},
            body=list_versions_response(versions=[('source.txt', 'v1')]), status=200)
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
        await register_presigned(
            provider, 'HEAD', 'head_object', path=path.path, default_params=True,
            headers=file_header_metadata)

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

    T-1 / CX1-11: every request here is signed by the real presigner, which is also what puts
    ``encoding-type=url`` on the listing.  The stand-in this class used to install did not,
    so the encoding half of the contract was invisible to it (CX1-3).
    """

    async def _register_versions(self, provider, body, **params):
        params.setdefault('Bucket', 'that-kerning')
        return await register_presigned(provider, 'GET', 'list_object_versions',
                                        query_parameters=params, body=body, status=200)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_pages_with_key_marker(self, auth, credentials, settings):
        """A truncated first page must be continued with KeyMarker/VersionIdMarker.

        The first page is registered as a two-element response list rather than as a single
        response on purpose: it caps how many times that page can be served.  Code that cannot
        advance past a truncated page re-requests the very same URL forever, so without the cap
        this test would hang instead of fail.  With the cap, the third request raises
        aiohttpretty's "No responses left." and the test fails in bounded time.
        """
        provider = raw_provider(auth, credentials, settings)
        page_one_body = list_versions_response(versions=[('my-image.jpg', 'version-one')],
                                               is_truncated=True,
                                               next_key_marker='my-image.jpg',
                                               next_version_id_marker='version-one')

        with frozen_signing_clock():
            page_one_url = await register_presigned(
                provider, 'GET', 'list_object_versions',
                query_parameters={'Bucket': 'that-kerning', 'Prefix': 'my-image.jpg'},
                responses=[{'body': page_one_body, 'status': 200},
                           {'body': page_one_body, 'status': 200}],
            )
            page_two_url = await self._register_versions(
                provider,
                list_versions_response(versions=[('my-image.jpg', 'version-two')]),
                Prefix='my-image.jpg', KeyMarker='my-image.jpg',
                VersionIdMarker='version-one')

            versions = await provider.get_object_versions({'Prefix': 'my-image.jpg'})

        assert page_one_url != page_two_url
        assert [item['VersionId'] for item in versions] == ['version-one', 'version-two']
        assert aiohttpretty.has_call(method='GET', uri=page_two_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_encoded_keys_and_markers_are_decoded(self, auth, credentials, settings):
        """V-1 / V-2 / V-6 / CX1-3: read the key names by the rule the response declares.

        botocore puts ``encoding-type=url`` on every listing it signs, so S3 percent-encodes
        the key names and the markers that resume from them, and says so with
        ``<EncodingType>url</EncodingType>``.

        The marker is the sharp edge.  It goes back as a ``KeyMarker`` *parameter*, which the
        signer percent-encodes on the way out; handing it the already-encoded form asks S3 for
        a key literally named ``f%2Fa%2Bb``.  No such key exists, so the second page is a
        listing of something else -- here, a URL nothing answers.

        Equivalence with develop: develop signs with boto2, which sends no ``EncodingType``
        and gets raw key names back.  The decoded set below, ``f/a+b`` and ``f/x y.txt``, is
        exactly what develop's ``get_full_revision`` collects from the same bucket, which is
        the contract this must not change.
        """
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_versions(
                provider,
                list_versions_response(versions=[('f%2Fa%2Bb', 'version-one')],
                                       is_truncated=True, next_key_marker='f%2Fa%2Bb',
                                       next_version_id_marker='version-one'),
                Prefix='f/')
            await self._register_versions(
                provider,
                list_versions_response(versions=[('f%2Fx%20y.txt', 'version-two')]),
                Prefix='f/', KeyMarker='f/a+b', VersionIdMarker='version-one')

            versions = await provider.get_object_versions({'Prefix': 'f/'})

        assert [item['Key'] for item in versions] == ['f/a+b', 'f/x y.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_id_marker_is_resumed_verbatim(self, auth, credentials, settings):
        """CX2-1 / 決定-23: ``EncodingType=url`` covers the key names, not the version ids.

        S3 percent-encodes what it derived from a key -- ``Key``, ``Prefix``, ``Delimiter``,
        ``KeyMarker``/``NextKeyMarker`` -- because those are the values a key name can make
        unreadable.  A version id is an opaque identifier that S3 minted itself, so it comes back
        exactly as it must be sent again; decoding one asks to resume from a different version.
        A ``%`` in it is the case that separates the two rules, and this pins both in a single
        request: the key marker is decoded, the version id marker is not.

        The pre-fix form of the second request is registered as well, so that decoding the
        version id fails on the assertion below -- which names the value that was sent -- rather
        than on ``aiohttpretty``'s "No URLs matching", which would say only that some URL
        differed.
        """
        provider = raw_provider(auth, credentials, settings)
        requested = record_request_urls(provider)
        page_two_body = list_versions_response(versions=[('f%2Fb', 'version-two')])

        with frozen_signing_clock():
            await self._register_versions(
                provider,
                list_versions_response(versions=[('f%2Fa', 'v%2Fid')], is_truncated=True,
                                       next_key_marker='f%2Fa',
                                       next_version_id_marker='v%2Fid'),
                Prefix='f/')
            await self._register_versions(provider, page_two_body, Prefix='f/',
                                          KeyMarker='f/a', VersionIdMarker='v%2Fid')
            await self._register_versions(provider, page_two_body, Prefix='f/',
                                          KeyMarker='f/a', VersionIdMarker='v/id')

            versions = await provider.get_object_versions({'Prefix': 'f/'})

        assert [item['Key'] for item in versions] == ['f/a', 'f/b']
        assert len(requested) == 2
        query = parse.parse_qs(parse.urlsplit(requested[1]).query)
        assert query['key-marker'] == ['f/a']
        assert query['version-id-marker'] == ['v%2Fid']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_a_marker_that_is_not_repeated_is_dropped(self, auth, credentials, settings):
        """CX2-2 / T-3: a ``VersionIdMarker`` belongs to the page that announced it.

        A page boundary can fall in the middle of one key's version history, and then S3 sends
        both markers.  The next boundary need not: once the listing has moved on to whole keys
        again it announces a ``NextKeyMarker`` alone.  Carrying the previous page's version id
        into that request asks to resume from a version of the *earlier* key -- S3 rejects the
        pair outright, and where it does not, the page returned is not the one after this one.

        Three pages is the smallest listing that can show it: the marker has to be set by one
        boundary and then not repeated by the next, so a two-page listing can only show it
        being set.  The stale-marker form of the third request is registered as well, so that
        carrying it forward fails on the assertion below rather than on "No URLs matching".
        """
        provider = raw_provider(auth, credentials, settings)
        requested = record_request_urls(provider)
        page_three_body = list_versions_response(versions=[('k3', 'v3')])

        with frozen_signing_clock():
            await self._register_versions(
                provider,
                list_versions_response(versions=[('k1', 'v1')], is_truncated=True,
                                       next_key_marker='k1', next_version_id_marker='v1'),
                Prefix='k')
            await self._register_versions(
                provider,
                # No NextVersionIdMarker: this boundary falls between two keys.
                list_versions_response(versions=[('k2', 'v2')], is_truncated=True,
                                       next_key_marker='k2'),
                Prefix='k', KeyMarker='k1', VersionIdMarker='v1')
            await self._register_versions(provider, page_three_body, Prefix='k', KeyMarker='k2')
            await self._register_versions(provider, page_three_body, Prefix='k', KeyMarker='k2',
                                          VersionIdMarker='v1')

            versions = await provider.get_object_versions({'Prefix': 'k'})

        assert [item['VersionId'] for item in versions] == ['v1', 'v2', 'v3']
        assert len(requested) == 3
        query = parse.parse_qs(parse.urlsplit(requested[2]).query)
        assert query['key-marker'] == ['k2']
        assert 'version-id-marker' not in query

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_a_listing_that_is_not_encoded_is_read_verbatim(self, auth, credentials,
                                                                  settings):
        """CX1-3: decoding is the response's declaration, not an assumption.

        A bucket listed without ``EncodingType`` answers with the key names as stored, and a
        key may legitimately contain a percent sign.  Decoding one of those would rename the
        object -- and a rename in a listing that drives a delete is how the wrong thing gets
        deleted.
        """
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_versions(
                provider,
                list_versions_response(versions=[('100%2Fdone.txt', 'version-one')],
                                       encoding_type=None),
                Prefix='100')

            versions = await provider.get_object_versions({'Prefix': '100'})

        assert [item['Key'] for item in versions] == ['100%2Fdone.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_collects_delete_markers(self, auth, credentials,
                                                               settings):
        """Delete markers are versions too and must be collectable for a full purge."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_versions(
                provider,
                list_versions_response(versions=[('my-image.jpg', 'version-one')],
                                       delete_markers=[('my-image.jpg', 'marker-one')]),
                Prefix='my-image.jpg')

            versions = await provider.get_object_versions({'Prefix': 'my-image.jpg'},
                                                          include_delete_markers=True)

        assert sorted(item['VersionId'] for item in versions) == ['marker-one', 'version-one']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_object_versions_omits_delete_markers_by_default(self, auth, credentials,
                                                                       settings):
        """revisions() must not grow delete markers as a side effect of the fix."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_versions(
                provider,
                list_versions_response(versions=[('my-image.jpg', 'version-one')],
                                       delete_markers=[('my-image.jpg', 'marker-one')]),
                Prefix='my-image.jpg')

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
    """K-9 / CX1-6: not in the message, and not in the traceback either.

    Converting an exception into a safe one is not enough on its own.  ``raise X`` inside an
    ``except`` leaves the original hanging off ``__context__``, and
    ``waterbutler.server.api.v1.core.log_exception`` records the failure with ``exc_info``, so
    the whole chain is formatted into the log.  Under SigV4 the original's message is the
    presigned request URL -- ``exception_from_response``'s default -- which carries
    ``X-Amz-Credential`` (the access key id) and ``X-Amz-Signature``.  The client response is
    clean; the log is not, and K-9 covers the log.
    """
    blob = '{!r} {!s} {}'.format(exc, exc, getattr(exc, 'message', ''))
    blob += ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    leaked = [marker for marker in SECRET_MARKERS if marker in blob]
    assert leaked == [], 'exception exposes {}'.format(leaked)


class local_server:
    """An ``aiohttp.web`` server bound to a loopback port, tied to ``provider``'s sessions.

    Ported from ``tests/providers/s3compatsigv4/test_provider.py`` (PR #98).
    ``aiohttpretty`` injects responses *above* ``ClientSession._request``, so anything that
    happens *inside* that call -- redirect following, and the merge of a URL's query with
    ``make_request``'s ``params=`` -- cannot be observed with it.  Pinning those needs a
    real socket.

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


# K-4 / 決定-13.  The commit's outcome is one of three things: it succeeded, it definitely
# did not happen, or nobody knows.  The third one is the one that needs saying out loud.
#
# An unknown code and a missing code both fall to UNKNOWN.  That is the fail-safe direction:
# an over-reported notice costs the user a re-check, an under-reported one silently claims
# nothing was stored -- and the user uploads again, at double the storage.
COMMIT_CODE_CASES = [
    ('AccessDenied', False),
    ('InvalidPart', False),
    ('EntityTooSmall', False),
    ('InternalError', True),
    ('SlowDown', True),
    ('RequestTimeout', True),
    ('XVendorMystery', True),
    (None, True),
]

# The classification table, written out independently of the implementation's
# ``DEFINITIVE_REJECTION_CODES``.  Generating it from the implementation would let a deleted
# row delete its own parameter, leaving that row unguarded -- which is exactly how PR #98's
# ``NoSuchUpload`` mistake survived a mutation run.
EXPECTED_DEFINITIVE_REJECTION_CODES = [
    'AccessDenied',
    'InvalidPart',
    'InvalidPartOrder',
    'EntityTooSmall',
    'EntityTooLarge',
    'MalformedXML',
    'SignatureDoesNotMatch',
    'InvalidAccessKeyId',
    'NoSuchBucket',
]

# Transports where the code is observable.
OBSERVED_TRANSPORTS = ['direct_4xx', 'direct_5xx', 'complete_200_error']
# Transports where it is not: whatever the storage meant to say never reaches WaterButler,
# so the verdict is UNKNOWN regardless.
LATENT_TRANSPORTS = ['disconnect', 'broken_xml']


def commit_error_xml(error_code):
    """An S3 error body for CompleteMultipartUpload.

    An ``error_code`` of ``None`` yields a body with no ``<Code>`` element: the "missing
    code" cell, parsable but carrying no verdict.
    """
    if error_code is None:
        return ('<?xml version="1.0" encoding="UTF-8"?>'
                '<Error><Message>boom</Message></Error>')
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<Error><Code>{}</Code><Message>boom</Message></Error>'.format(error_code))


def arrange_chunked_commit(provider, aborted=True):
    """Set up ``_chunked_upload`` so that only the commit fails.

    ``_complete_multipart_upload`` itself is deliberately left real:
    NOTE_SEMANTICS_DESIGN v2.2 §4-2d -- a test that judges the notice must not mock any of
    the code that decides it.  The injection goes to the boundary below (``make_request``).

    T-1 / CX1-11: which is also why the presigner is no longer stood in for here.  The commit
    signs a URL on its way to the ``make_request`` that ``arrange_commit_failure`` replaces, so
    the real presigner runs and its output is simply not read -- a stub bought nothing, and left
    a signing failure invisible to the whole notice matrix.
    """
    provider._create_upload_session = MockCoroutine(return_value='SESSION')
    provider._upload_parts = MockCoroutine(return_value=[{'ETAG': 'abc'}])
    provider._abort_chunked_upload = MockCoroutine(return_value=aborted)


def arrange_commit_failure(provider, transport, error_code):
    """Fail only the commit request, in the shape of ``transport``."""
    if transport == 'direct_4xx':
        provider.make_request = MockCoroutine(side_effect=exceptions.UploadError(
            {'response': commit_error_xml(error_code)}, code=400))
    elif transport == 'direct_5xx':
        # The status class must not decide anything: S3 answers a failed commit with 200 and
        # an ``<Error>`` body, so "5xx" and "the storage was definite" are unrelated.
        provider.make_request = MockCoroutine(side_effect=exceptions.UploadError(
            {'response': commit_error_xml(error_code)}, code=500))
    elif transport == 'complete_200_error':
        resp = mock.Mock()
        resp.status = 200
        resp.read = MockCoroutine(return_value=commit_error_xml(error_code).encode('utf-8'))
        resp.release = MockCoroutine()
        provider.make_request = MockCoroutine(return_value=resp)
    elif transport == 'disconnect':
        # No response arrived, so no code is observable.  The S3 error XML goes into the
        # exception's ``message`` on purpose: an implementation that reads a code from there
        # rather than from a response body has to fail here.
        provider.make_request = MockCoroutine(
            side_effect=aiohttp.ServerDisconnectedError(commit_error_xml(error_code)))
    elif transport == 'broken_xml':
        # The body arrived but is truncated.  The code string is present in it yet cannot be
        # parsed, so it is not observed -- a substring match must never pick it up.
        provider.make_request = MockCoroutine(side_effect=exceptions.UploadError(
            {'response': commit_error_xml(error_code)[:-12]}, code=400))
    else:  # pragma: no cover - a mistyped parameter must not pass silently
        raise AssertionError('unknown transport: {}'.format(transport))


class TestErrorReporting:
    """K-2 / K-7 / K-8 / K-9: what the six aiobotocore call sites do with a failure."""

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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder_reports_a_failed_listing_as_a_delete_failure(
            self, auth, credentials, settings, mock_time):
        """CX1-13 / K-12: a folder delete that cannot list what to delete is a delete failure.

        The listing is made with ``throws=DownloadError``, and nothing between there and the
        caller changes it: the user asked to delete a folder and is told a download went wrong,
        with a status borrowed from an operation they did not ask for.  K-12's "core handles it"
        holds only while the error carries no ``data``; S3 answers this with a readable XML body,
        so ``exception_from_response`` puts its prose -- request id and host id included -- into
        ``message`` and core hands that straight back.

        T-1 / CX1-11: signed by the real presigner, injected at the HTTP boundary.
        """
        provider = raw_provider(auth, credentials, settings)
        path = WaterButlerPath('/doomed-folder/')
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<Error><Code>AccessDenied</Code>'
                '<Message>Access Denied</Message>'
                '<RequestId>REQ123</RequestId><HostId>HOST456</HostId></Error>').encode('utf-8')

        with frozen_signing_clock():
            await register_presigned(
                provider, 'GET', 'list_object_versions',
                query_parameters={'Bucket': 'that-kerning', 'Prefix': 'doomed-folder/'},
                body=body, status=403, headers={'Content-Type': 'application/xml'})

            with pytest.raises(exceptions.DeleteError) as e:
                await provider.delete(path)

        assert e.value.code == 403
        assert 'DownloadError' in e.value.message
        assert 'REQ123' not in e.value.message
        assert 'HOST456' not in e.value.message
        assert_no_secrets(e.value)
        assert_context_suppressed(e.value)


class TestResponseParsing:
    """K-10 / CX1-10: what each XML shape the provider can be handed turns into.

    Every case here is a 200.  That is the point: the body is the only thing that says what
    happened, so a shape the provider cannot read has to become an error rather than an empty
    answer.  An empty folder listing and an unreadable one look identical to the caller, and
    ``_delete_folder`` acts on the difference -- "no versions under this prefix" is how it
    decides there is nothing to purge.

    T-1 / CX1-11: signed by the real presigner, injected at the HTTP boundary.
    """

    FOLDER_PARAMS = {'Bucket': 'that-kerning', 'Prefix': 'my-subfolder/'}
    VERSION_PARAMS = {'Bucket': 'that-kerning', 'Prefix': 'my-image.jpg'}

    async def _register_folder(self, provider, body):
        return await register_presigned(
            provider, 'GET', 'list_objects_v2',
            query_parameters=dict(self.FOLDER_PARAMS), body=body, status=200,
            headers={'Content-Type': 'application/xml'})

    async def _register_versions(self, provider, body):
        return await register_presigned(
            provider, 'GET', 'list_object_versions',
            query_parameters=dict(self.VERSION_PARAMS), body=body, status=200,
            headers={'Content-Type': 'application/xml'})

    async def _register_once(self, provider, s3_method, params, body):
        """Answer the presigned URL exactly once.

        The truncation cases below are about a listing loop that cannot move on.  Registering a
        single answer makes the second, identical request raise out of ``aiohttpretty`` instead
        of being served again, so a loop that fails to terminate shows up as a failure that can
        be measured rather than as a test run that never ends.
        """
        return await register_presigned(
            provider, 'GET', s3_method, query_parameters=dict(params),
            responses=[{'body': body, 'status': 200,
                        'headers': {'Content-Type': 'application/xml'}}])

    async def _list_folder(self, provider):
        return await provider.get_folder_metadata('my-subfolder/',
                                                  dict(self.FOLDER_PARAMS))

    async def _list_versions(self, provider):
        return await provider.get_object_versions(dict(self.VERSION_PARAMS))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_rejects_an_unrecognised_root_element(self, auth, credentials,
                                                                       settings, mock_time):
        """K-10: ``doc.get('ListBucketResult', {})`` answers ``{}`` for any body whose root
        element is not spelled exactly that -- a namespace-prefixed one, say -- and an empty
        listing is indistinguishable from an empty folder.  Fail closed instead."""
        provider = raw_provider(auth, credentials, settings)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<s3:ListBucketResult xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/">'
                '<s3:IsTruncated>false</s3:IsTruncated>'
                '<s3:Contents><s3:Key>my-subfolder/thefile.txt</s3:Key></s3:Contents>'
                '</s3:ListBucketResult>').encode('utf-8')

        with frozen_signing_clock():
            await self._register_folder(provider, body)

            with pytest.raises(exceptions.DownloadError):
                await self._list_folder(provider)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_listing_rejects_an_unrecognised_root_element(self, auth, credentials,
                                                                        settings, mock_time):
        """K-10: the same shape on the versions listing decides what a delete purges.  An empty
        list means "nothing to delete", so a delete would report success having removed nothing."""
        provider = raw_provider(auth, credentials, settings)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<s3:ListVersionsResult xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/">'
                '<s3:IsTruncated>false</s3:IsTruncated>'
                '</s3:ListVersionsResult>').encode('utf-8')

        with frozen_signing_clock():
            await self._register_versions(provider, body)

            with pytest.raises(exceptions.DownloadError):
                await self._list_versions(provider)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_accepts_a_whitespace_formatted_body(self, auth, credentials,
                                                                      settings, mock_time):
        """K-10: indentation between the elements must not change the result."""
        provider = raw_provider(auth, credentials, settings)
        body = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
                '  <IsTruncated>false</IsTruncated>\n'
                '  <Contents>\n    <Key>my-subfolder/thefile.txt</Key>\n  </Contents>\n'
                '</ListBucketResult>\n').encode('utf-8')

        with frozen_signing_clock():
            await self._register_folder(provider, body)
            contents, prefixes, token = await self._list_folder(provider)

        assert [item['Key'] for item in contents] == ['my-subfolder/thefile.txt']
        assert token == ''

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_accepts_a_single_contents_element(self, auth, credentials,
                                                                    settings, mock_time):
        """K-10: xmltodict collapses a lone repeated element to a dict rather than a
        one-element list."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_folder(
                provider, list_objects_v2_response(['my-subfolder/thefile.txt']))
            contents, prefixes, token = await self._list_folder(provider)

        assert [item['Key'] for item in contents] == ['my-subfolder/thefile.txt']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_rejects_an_empty_body(self, auth, credentials, settings,
                                                        mock_time):
        """K-10: an empty 200 must not read as an empty folder."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_folder(provider, b'')

            with pytest.raises(exceptions.DownloadError):
                await self._list_folder(provider)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('element', ['Contents', 'CommonPrefixes'])
    async def test_folder_listing_rejects_a_scalar_where_elements_belong(
            self, auth, credentials, settings, mock_time, element):
        """CX1-10: failing closed on the root element alone stops one step short.  xmltodict
        renders ``<Contents>text</Contents>`` as a string, and iterating a string yields its
        characters -- ``'t'.get('Key')`` is an ``AttributeError``, which escapes the provider
        as a bare 500 saying nothing.  The shape is unreadable for the same reason the root
        element was, so it fails the same way."""
        provider = raw_provider(auth, credentials, settings)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<ListBucketResult><IsTruncated>false</IsTruncated>'
                '<{0}>a stray string</{0}></ListBucketResult>'.format(element)).encode('utf-8')

        with frozen_signing_clock():
            await self._register_folder(provider, body)

            with pytest.raises(exceptions.DownloadError):
                await self._list_folder(provider)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('element', ['Version', 'DeleteMarker'])
    async def test_version_listing_rejects_a_scalar_where_elements_belong(
            self, auth, credentials, settings, mock_time, element):
        """CX1-10: the same on the listing a folder delete is built from."""
        provider = raw_provider(auth, credentials, settings)
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<ListVersionsResult><IsTruncated>false</IsTruncated>'
                '<{0}>a stray string</{0}></ListVersionsResult>'.format(element)).encode('utf-8')

        with frozen_signing_clock():
            await self._register_versions(provider, body)

            with pytest.raises(exceptions.DownloadError):
                await self._list_versions(provider)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_listing_rejects_a_truncation_it_cannot_resume(self, auth, credentials,
                                                                        settings, mock_time):
        """CX1-10: ``IsTruncated`` true with no ``NextContinuationToken`` leaves the loop with
        nothing to change, so it re-sends the identical request for ever.  Neither that nor
        quietly returning the first page is safe -- the caller would take a partial listing for
        the whole folder -- so it fails closed."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_once(
                provider, 'list_objects_v2', self.FOLDER_PARAMS,
                list_objects_v2_response(['my-subfolder/a.txt'], is_truncated=True))

            with pytest.raises(exceptions.DownloadError):
                await self._list_folder(provider)

        assert len(aiohttpretty.calls) == 1

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_listing_rejects_a_truncation_it_cannot_resume(self, auth, credentials,
                                                                         settings, mock_time):
        """CX1-10: the versions listing stopped quietly instead, which is worse than it sounds
        -- ``_delete_folder`` deletes exactly what this returns, so a folder delete would
        report success having purged only the first page and left the rest behind."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self._register_once(
                provider, 'list_object_versions', self.VERSION_PARAMS,
                list_versions_response([('my-image.jpg', 'v1')], is_truncated=True))

            with pytest.raises(exceptions.DownloadError):
                await self._list_versions(provider)

        assert len(aiohttpretty.calls) == 1


COMMIT_PATH = WaterButlerPath('/my-subfolder/thefile.txt')

COMMIT_SUCCESS_BODY = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<CompleteMultipartUploadResult>'
    '<Location>https://that-kerning.s3.amazonaws.com/my-subfolder/thefile.txt</Location>'
    '<Bucket>that-kerning</Bucket><Key>my-subfolder/thefile.txt</Key>'
    '<ETag>&quot;abc&quot;</ETag>'
    '</CompleteMultipartUploadResult>'
).encode('utf-8')


async def register_commit(provider, body, status=200):
    """Answer the real presigned CompleteMultipartUpload URL with ``body``."""
    return await register_presigned(
        provider, 'POST', 'complete_multipart_upload', path=COMMIT_PATH.path,
        query_parameters={'UploadId': 'SESSION'}, default_params=True,
        body=body, status=status)


async def commit(provider):
    await provider._complete_multipart_upload(COMMIT_PATH, 'SESSION', [{'ETAG': 'abc'}])


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
        body = ('<?xml version="1.0" encoding="UTF-8"?>'
                '<Error><Code>InternalError</Code>'
                '<Message>We encountered an internal error. Please try again.</Message>'
                '</Error>').encode('utf-8')

        with frozen_signing_clock():
            await register_commit(provider, body)

            with pytest.raises(exceptions.UploadError) as e:
                await commit(provider)

        assert 'InternalError' in e.value.message
        assert_no_secrets(e.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_complete_accepts_a_200_carrying_a_result(self, auth, credentials, settings,
                                                            mock_time):
        """K-3: the success body must still be accepted."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await register_commit(provider, COMMIT_SUCCESS_BODY)
            await commit(provider)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('label,body', [
        ('an empty Error element',
         b'<?xml version="1.0" encoding="UTF-8"?><Error/>'),
        ('an Error that is not an element',
         b'<?xml version="1.0" encoding="UTF-8"?><Error>failure</Error>'),
        ('an Error with no Code',
         b'<?xml version="1.0" encoding="UTF-8"?><Error><Message>no</Message></Error>'),
        ('a body that is not XML',
         b'<html><body>502 Bad Gateway</body></html>'),
        ('a truncated body',
         b'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Loc'),
        ('an empty body', b''),
        ('a result with no ETag',
         b'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult>'
         b'<Bucket>that-kerning</Bucket></CompleteMultipartUploadResult>'),
        ('an unknown root element',
         b'<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult>'
         b'<UploadId>SESSION</UploadId></InitiateMultipartUploadResult>'),
    ])
    async def test_complete_rejects_a_200_that_does_not_report_success(self, auth, credentials,
                                                                       settings, mock_time,
                                                                       label, body):
        """CX1-4 / K-3: only a ``CompleteMultipartUploadResult`` carrying an ``ETag`` says the
        object was assembled.  Everything else here reached ``isinstance(error, dict)``, found
        no dict, and returned as if the upload had completed -- the user is told the file is
        there and it is not.

        NOTE_SEMANTICS_DESIGN v2.2 §2 wants "2xx *and* a well-formed body" before a commit
        counts as done; a body nobody can read is not evidence either way, so these are UNKNOWN
        and the notice has to be on them.
        """
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await register_commit(provider, body)

            with pytest.raises(exceptions.UploadError) as e:
                await commit(provider)

        assert pd_provider._is_commit_outcome_unknown(e.value), label
        assert provider._commit_outcome_note(e.value), label
        assert_no_secrets(e.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_complete_keeps_classifying_a_rejection_it_can_read(self, auth, credentials,
                                                                       settings, mock_time):
        """CX1-4: tightening the success gate must not turn a readable rejection into UNKNOWN.
        ``EntityTooSmall`` is the one code in DEFINITIVE_REJECTION_CODES that was actually
        observed on a commit, so the commit did not happen and the user must not be told it
        may have."""
        provider = raw_provider(auth, credentials, settings)
        body = (b'<?xml version="1.0" encoding="UTF-8"?>'
                b'<Error><Code>EntityTooSmall</Code></Error>')

        with frozen_signing_clock():
            await register_commit(provider, body)

            with pytest.raises(exceptions.UploadError) as e:
                await commit(provider)

        assert 'EntityTooSmall' in e.value.message
        assert provider._commit_outcome_note(e.value) == ''

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


class TestChunkedUploadWireQuery:
    """R-6 / U-9 / CX1-2: what the part, abort and list-parts requests put on the wire.

    A presigned URL already carries every parameter it was signed over.  Passing the same
    parameters again as ``make_request(params=...)`` does not overwrite them: ``ClientRequest``
    *extends* the URL's query, so the request goes out with each one twice.  S3 answers a
    duplicated query parameter with ``SignatureDoesNotMatch`` -- the signature covers the
    canonical query string, which no longer matches -- so every chunked upload of a file
    larger than one part fails.

    ``aiohttpretty`` cannot show this: it replaces ``ClientSession._request``, which is above
    the merge.  These tests use a real socket and read the query the server received.
    """

    async def _capture(self, provider, method, path, call):
        """Run ``call`` against a loopback server and return the query string it received."""
        seen = {}

        async def handler(request):
            await request.read()
            seen['query'] = request.query_string
            return web.Response(status=200, headers={'ETag': '"d41d8cd98f00b204e9800998ecf8"'})

        app = web.Application()
        app.router.add_route(method, '/{tail:.*}', handler)
        async with local_server(provider, app) as server:
            redirect_presigned_origin(provider, server.url)
            await call(path)

        return parse.parse_qs(seen['query'], keep_blank_values=True)

    @pytest.mark.asyncio
    async def test_part_request_sends_each_parameter_once(self, auth, credentials, settings):
        """CX1-2: ``partNumber`` and ``uploadId`` are in the signed URL, so ``_upload_part``
        must not add them a second time.  On ``ca65500e`` both arrive twice."""
        provider = raw_provider(auth, credentials, settings)
        stream = streams.StringStream(b'abcdefghij')

        async def upload(path):
            await provider._upload_part(stream, path, 'SESSION', 1, 10)

        query = await self._capture(provider, 'PUT', WaterButlerPath('/my-subfolder/f.txt'),
                                    upload)

        assert query['partNumber'] == ['1']
        assert query['uploadId'] == ['SESSION']
        # The signature is signed over the canonical query; a duplicate breaks it even when
        # the two values agree, so the count is the thing to assert, not the value.
        assert len(query['X-Amz-Signature']) == 1

    @pytest.mark.asyncio
    async def test_list_parts_request_sends_each_parameter_once(self, auth, credentials,
                                                                settings):
        """CL m-2: ``_list_uploaded_chunks`` passes ``params=headers`` -- an empty dict that
        reads as a copy-paste of the headers argument.  It adds nothing today; the assertion
        is that the request carries the signed query and only that."""
        provider = raw_provider(auth, credentials, settings)

        async def list_parts(path):
            await provider._list_uploaded_chunks(path, 'SESSION')

        query = await self._capture(provider, 'GET', WaterButlerPath('/my-subfolder/f.txt'),
                                    list_parts)

        assert query['uploadId'] == ['SESSION']
        assert len(query['X-Amz-Signature']) == 1

    @pytest.mark.asyncio
    async def test_uploading_parts_logs_nothing_at_error_level(self, auth, credentials, settings,
                                                                caplog):
        """CL M-2: ``_upload_parts`` opens with ``logger.error('_upload_parts')``.

        Every multi-part upload that goes perfectly emits an ERROR saying only the name of the
        method it is in.  That is what an alert routes on and what an operator reads first, so a
        marker left in from debugging turns the ERROR level into noise and buries the failures
        this provider does report there.
        """
        provider = raw_provider(auth, credentials, settings)
        stream = streams.StringStream(b'abcdefghij')

        async def upload(path):
            with caplog.at_level(logging.INFO, logger='waterbutler.providers.s3.provider'):
                await provider._upload_parts(stream, path, 'SESSION')

        await self._capture(provider, 'PUT', WaterButlerPath('/my-subfolder/f.txt'), upload)

        errors = [record.getMessage() for record in caplog.records
                  if record.levelno >= logging.ERROR]
        assert errors == []


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
        pins that it is written down.

        T-1 / CX1-11: the URL being counted is the one the real presigner produced for this
        commit, not a constant.  ``retry_on`` matches on the status, but core's retry re-sends
        the *same* URL, so answering the real one is what makes "exactly once" a statement
        about the commit request rather than about a string the test chose.
        """
        provider = raw_provider(auth, credentials, settings)
        error_body = ('<?xml version="1.0" encoding="UTF-8"?>'
                      '<Error><Code>SlowDown</Code>'
                      '<Message>Please reduce your request rate.</Message></Error>')
        await register_presigned(
            provider, 'POST', 'complete_multipart_upload',
            path='my-subfolder/thefile.txt', query_parameters={'UploadId': 'SESSION'},
            default_params=True, status=status, body=error_body.encode('utf-8'))

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

        ``aiohttpretty`` cannot pin this; see ``local_server``.

        T-1 / CX1-11: the first route is mounted on the path the real presigner signed, and the
        request reaches it through ``redirect_presigned_origin`` -- only the scheme and host are
        rewritten, because a test cannot listen on ``s3.amazonaws.com``.  So what the redirect is
        offered is the commit's own URL, and a signing change that moved the path would be a
        failure here rather than a test quietly measuring a constant.
        """
        provider = raw_provider(auth, credentials, settings)
        commit_url = await provider.generate_generic_presigned_url(
            'my-subfolder/thefile.txt', method='complete_multipart_upload',
            query_parameters={'UploadId': 'SESSION'})
        commit_path = parse.urlsplit(commit_url).path

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
        app.router.add_post(commit_path, first)
        app.router.add_post('/second', second)

        async with local_server(provider, app) as server:
            redirect_presigned_origin(provider, server.url)
            with pytest.raises(exceptions.UploadError):
                await provider._complete_multipart_upload(
                    WaterButlerPath('/my-subfolder/thefile.txt'), 'SESSION', [{'ETAG': 'abc'}])

        # Exactly one commit POST.  A second one records ``/second``, so a failure here shows
        # how far the request got.
        assert calls == [commit_path]

    @pytest.mark.asyncio
    async def test_commit_request_states_both_preconditions(self, auth, credentials, settings,
                                                            mock_time):
        """NOTE_SEMANTICS_DESIGN v2.2 §4-2b: watch the preconditions directly, not only
        through their effect.  The two counting tests above go through aiohttp, so a future
        change that keeps the observable single-send by accident -- core dropping the retry
        loop, say -- would leave them green while the commit stopped declaring what it needs.

        T-1 / CX1-11: ``make_request`` is the boundary being watched, so the presigner above it
        is left real -- its URL is signed and then simply not sent anywhere.
        """
        provider = raw_provider(auth, credentials, settings)
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


class TestCommitOutcome:
    """K-4 / 決定-13: what the user is told when a multi-part commit fails.

    Ported from PR #98 (``tests/providers/s3compatsigv4/test_provider.py``); the design is
    ``S3CompatSigv4-quota-handling/NOTE_SEMANTICS_DESIGN.md`` v2.2 §2-2 / §3-2〜3-4 / §4-1 /
    §4-2d.  The outcome has three values -- success, NOT_COMMITTED, UNKNOWN -- and the
    difference that matters is the last two: "the file was not saved" tells the user to
    upload again, and saying it when the object is in fact on the storage costs a second
    copy that only an administrator can remove.

    The verdict is taken from the S3 error **code** alone.  The HTTP status class cannot
    carry it: a failed CompleteMultipartUpload arrives as 200 with an ``<Error>`` body, and
    ``_check_for_200_error``-style handling rewrites that to 5xx, so the status says "server
    error" for answers the storage was perfectly definite about.

    Two deviations from #98, both recorded in PHASE_TK2_REPORT:

    * the quota-suppression branch is **not** ported -- K-11 established that the ``s3``
      provider has no quota mechanism at all (absent from ``ADDON_METHOD_PROVIDER`` and from
      ``website/util/quota.py``'s ``PROVIDERS``), so there is no quota response to suppress;
    * K-1's abort-outcome wording is kept and the notice is combined with it, rather than
      replacing it as #98 does.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize('aborted', [True, False])
    @pytest.mark.parametrize('transport', OBSERVED_TRANSPORTS)
    @pytest.mark.parametrize('error_code,expect_notice', COMMIT_CODE_CASES)
    async def test_commit_notice_depends_only_on_the_observed_code(
            self, auth, credentials, settings, mock_time,
            transport, error_code, expect_notice, aborted):
        """The observable cells: the same operation and the same observed code must give the
        same verdict on every transport and whatever the abort did.  Per-transport parameter
        sets cannot expose a contradiction *between* transports, which is why the product is
        taken in one place -- all three previous review rounds missed the contradiction for
        exactly that reason."""
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider, aborted=aborted)
        arrange_commit_failure(provider, transport, error_code)

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert (S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in e.value.message) is expect_notice
        assert_no_secrets(e.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('aborted', [True, False])
    @pytest.mark.parametrize('transport', LATENT_TRANSPORTS)
    @pytest.mark.parametrize('latent_code', [code for code, _ in COMMIT_CODE_CASES])
    async def test_commit_notice_when_the_code_cannot_be_observed(
            self, auth, credentials, settings, mock_time, monkeypatch,
            transport, latent_code, aborted):
        """The latent cells.  On a disconnect or a truncated body no code can be read, so
        whatever the storage meant to say, the verdict falls to UNKNOWN.

        These cells are not vacuous: the code string really is there -- in the exception's
        ``message`` on a disconnect, inside the truncated body on broken XML.  An
        implementation reading it from anywhere but a parsed response body, or by substring,
        drops the notice and fails here.

        "Not observable" is the premise, so the premise is asserted alongside the
        conclusion: an implementation emitting the notice unconditionally would satisfy the
        conclusion on its own."""
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider, aborted=aborted)
        arrange_commit_failure(provider, transport, latent_code)

        observed = []
        real_observed = S3Provider._observed_error_code.__func__

        def spy(cls, err):
            code = real_observed(cls, err)
            observed.append(code)
            return code

        monkeypatch.setattr(S3Provider, '_observed_error_code', classmethod(spy))

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in e.value.message
        assert observed and all(code is None for code in observed)
        assert_no_secrets(e.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('aborted,abort_message', [
        (True, ' The upload is aborted.'),
        (False, 'manually remove them'),
    ])
    async def test_the_notice_is_combined_with_the_abort_outcome(
            self, auth, credentials, settings, mock_time, aborted, abort_message):
        """K-1's two abort messages stay, and the K-4 notice goes *between* the failure
        sentence and them.  The two answer different questions -- "is the object there?" and
        "is there rubbish left behind?" -- and dropping either leaves the user without the
        half they need to act on."""
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider, aborted=aborted)
        arrange_commit_failure(provider, 'direct_5xx', 'InternalError')

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        message = e.value.message
        notice = S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE
        assert notice in message
        assert abort_message in message
        assert message.index(notice) < message.index(abort_message)
        assert message.index('An unexpected error has occurred') < message.index(notice)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('aborted,abort_message', [
        (True, ' The upload is aborted.'),
        (False, 'manually remove them'),
    ])
    async def test_a_definitive_rejection_leaves_the_abort_outcome_alone(
            self, auth, credentials, settings, mock_time, aborted, abort_message):
        """The other half of the combination table: suppressing the notice must not take the
        abort outcome with it."""
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider, aborted=aborted)
        arrange_commit_failure(provider, 'direct_5xx', 'AccessDenied')

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in e.value.message
        assert abort_message in e.value.message

    @pytest.mark.asyncio
    async def test_a_failure_before_the_commit_does_not_claim_one(self, auth, credentials,
                                                                  settings, mock_time):
        """NOTE_SEMANTICS_DESIGN v2.2 §3-3: "sent" begins at the commit ``await``.  A part
        that fails never gets there, so the notice must not appear -- it would send the user
        looking for a file that was never assembled."""
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider)
        provider._upload_parts = MockCoroutine(
            side_effect=exceptions.UploadError({'response': commit_error_xml('InternalError')},
                                               code=500))

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE not in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize('where', ['commit-request', 'commit-read'])
    async def test_a_general_exception_inside_the_commit_claims_one(
            self, auth, credentials, settings, mock_time, where):
        """NOTE_SEMANTICS_DESIGN v2.2 §4-2d: the mark has to be applied to *any* exception
        that escapes the commit, not only to the ones WaterButler recognises.

        Both injection points sit on the boundary -- the request and the response read --
        with the real ``_complete_multipart_upload`` in between.  Replacing that method with
        a mock that pre-marks its exception would pin the exit while leaving the ``except``
        clauses that reach it completely unguarded; PR #98 measured two mutations surviving
        360 tests that way."""
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider)

        if where == 'commit-request':
            provider.make_request = MockCoroutine(side_effect=RuntimeError('boom'))
        else:
            resp = mock.Mock()
            resp.status = 200
            resp.read = MockCoroutine(side_effect=RuntimeError('boom'))
            resp.release = MockCoroutine()
            provider.make_request = MockCoroutine(return_value=resp)

        with pytest.raises(exceptions.UploadError) as e:
            await provider._chunked_upload(None, WaterButlerPath('/my-subfolder/thefile.txt'))

        assert S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in e.value.message

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', [200, 500])
    async def test_a_commit_answered_in_invalid_utf8_claims_one(self, auth, credentials,
                                                                settings, mock_time, status):
        """CX2-2 / NOTE_SEMANTICS_DESIGN v2.2 §4-1: a body nobody can decode is UNKNOWN.

        The matrix above reaches the general-exception cells by injecting a ``RuntimeError``
        into ``make_request``, and the illegible-body cells with a synthetic ``UploadError``.
        Neither goes through a socket, and the decoding happens below the provider -- so the
        one thing that was never measured is the case that produces it: bytes that are not
        UTF-8 arriving over real HTTP.  Both statuses are here because the byte sequence
        surfaces as a different exception on each, and the verdict must not depend on that:

        * **200** -- the body is read as bytes and handed to ``xmltodict``, which cannot parse
          it.  Nothing contradicts success and nothing states it, so the commit's own
          "does not report success" ``UploadError`` is what carries the mark;
        * **500** -- ``exception_from_response`` builds the exception by calling
          ``data.decode('utf-8')`` (``waterbutler/core/exceptions.py``), which raises
          ``UnicodeDecodeError`` *instead of* returning an ``UploadError``.  That escapes
          ``make_request`` as a type WaterButler does not recognise, and it reaches the notice
          only because the commit marks every exception rather than the ones it knows.

        The bytes are invalid UTF-8 in the middle of an otherwise well-formed success body, so
        an implementation that decoded leniently -- or read the ``ETag`` out of the raw bytes --
        would report the upload as completed instead.
        """
        provider = raw_provider(auth, credentials, settings)
        arrange_chunked_commit(provider)
        commit_url = await provider.generate_generic_presigned_url(
            'my-subfolder/thefile.txt', method='complete_multipart_upload',
            query_parameters={'UploadId': 'SESSION'})
        commit_path = parse.urlsplit(commit_url).path

        async def commit(request):
            await request.read()
            return web.Response(
                status=status, content_type='application/xml',
                body=b'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult>'
                     b'<ETag>"\xff\xfe"</ETag></CompleteMultipartUploadResult>')

        app = web.Application()
        app.router.add_post(commit_path, commit)

        async with local_server(provider, app) as server:
            redirect_presigned_origin(provider, server.url)
            with pytest.raises(exceptions.UploadError) as e:
                await provider._chunked_upload(None,
                                               WaterButlerPath('/my-subfolder/thefile.txt'))

        assert S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in e.value.message
        assert_no_secrets(e.value)

    @pytest.mark.parametrize('error_code', EXPECTED_DEFINITIVE_REJECTION_CODES)
    def test_every_definitive_rejection_code_suppresses_the_notice(self, auth, credentials,
                                                                   settings, error_code):
        """One parameter per row of the classification table.  Without this, deleting a row
        also deletes the test that would have caught the deletion -- measured in PR #98,
        where 4 of 6 row-deleting mutations survived."""
        provider = raw_provider(auth, credentials, settings)
        err = pd_provider._mark_commit_outcome_unknown(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=400))

        assert provider._commit_outcome_note(err) == ''

    @pytest.mark.parametrize('error_code', [
        'NoSuchUpload',       # a second commit meets a consumed UploadId -- the first may
                              # well have succeeded, so this is the opposite of definitive
        'InternalError', 'SlowDown', 'RequestTimeout', 'ServiceUnavailable',
        'accessdenied',       # codes are identifiers: case is not folded
        'XAccessDenied',      # and a substring must not pass for the code
        None,
    ])
    def test_codes_outside_the_table_keep_the_notice(self, auth, credentials, settings,
                                                     error_code):
        provider = raw_provider(auth, credentials, settings)
        err = pd_provider._mark_commit_outcome_unknown(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=400))

        assert provider._commit_outcome_note(err) == provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

    @pytest.mark.parametrize('status', [400, 500, 502, 200])
    @pytest.mark.parametrize('error_code,suppressed', [('AccessDenied', True),
                                                       ('InternalError', False)])
    def test_the_note_ignores_the_status_class(self, auth, credentials, settings, status,
                                               error_code, suppressed):
        """決定-13's central claim, isolated from the transports: the status contributes
        nothing.  It cannot -- a failed commit's own status is 200."""
        provider = raw_provider(auth, credentials, settings)
        err = pd_provider._mark_commit_outcome_unknown(
            exceptions.UploadError({'response': commit_error_xml(error_code)}, code=status))

        assert (provider._commit_outcome_note(err) == '') is suppressed

    def test_the_note_does_not_read_a_code_off_a_connection_error(self, auth, credentials,
                                                                  settings):
        """A dropped connection is precisely the case where nothing was observed.  Its
        message is attacker-shaped only by accident here, but the rule is the point: only a
        response body may speak for the storage."""
        provider = raw_provider(auth, credentials, settings)
        err = pd_provider._mark_commit_outcome_unknown(
            aiohttp.ServerDisconnectedError(commit_error_xml('AccessDenied')))

        assert provider._observed_error_code(err) is None
        assert provider._commit_outcome_note(err) == provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE

    def test_the_note_needs_the_mark(self, auth, credentials, settings):
        """Without the mark the failure did not come from the commit, so there is no commit
        whose outcome could be unknown."""
        provider = raw_provider(auth, credentials, settings)
        err = exceptions.UploadError({'response': commit_error_xml('InternalError')}, code=500)

        assert provider._commit_outcome_note(err) == ''

    @pytest.mark.parametrize('error_code,expected', [('AccessDenied', 'AccessDenied'),
                                                     (None, None)])
    def test_observed_error_code_reads_a_botocore_client_error(self, auth, credentials,
                                                               settings, error_code, expected):
        """``generate_generic_presigned_url`` and the other aiobotocore call sites raise
        ``ClientError``, whose code lives in ``response['Error']['Code']`` rather than in a
        body WaterButler read itself."""
        provider = raw_provider(auth, credentials, settings)
        err = pd_provider._mark_commit_outcome_unknown(
            s3_client_error(error_code, 403, operation='CompleteMultipartUpload'))

        assert provider._observed_error_code(err) == expected

    def test_the_table_is_what_the_design_says_it_is(self, auth, credentials, settings):
        """The table is transcribed from MinIO measurements in NOTE_SEMANTICS_DESIGN v2.2
        §2-2 and is not verified against AWS S3 -- TEST_SPEC E-1 reconciles it.  Pinning the
        exact set here means an addition has to be argued for, not slipped in."""
        assert pd_provider.DEFINITIVE_REJECTION_CODES == frozenset(
            EXPECTED_DEFINITIVE_REJECTION_CODES)


def assert_context_suppressed(exc):
    """CX1-6 / K-9: nothing the provider refused to say is reachable through ``__context__``.

    An exception raised inside an ``except`` keeps the original on ``__context__`` unless the
    ``raise`` says ``from None``, and ``traceback.format_exception`` -- which is what
    ``log_exception``'s ``exc_info`` ends up calling -- walks that chain.  Converting a failure
    into one that names only the type and the error code therefore does nothing for the log
    while the chain is still there.

    Asserted as a structural property rather than by scanning for markers: the marker scan can
    only fail on the messages that happen to carry a URL today, whereas the rule is that a
    deliberately-narrowed exception does not drag the wide one along behind it.
    """
    assert exc.__context__ is None or exc.__suppress_context__, (
        'chains {}: {!s}'.format(type(exc.__context__).__name__, exc.__context__))


class TestExceptionChaining:
    """CX1-6 / K-9: the conversion points must not leave the original on the chain."""

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_check_key_existence_does_not_chain_the_signed_url(self, auth, credentials,
                                                                      settings, mock_time):
        """K-8/K-9: the HEAD path is where the original's message really is the presigned URL.
        ``exception_from_response`` has no body to use on a HEAD, so it falls back to
        ``DEFAULT_ERROR_MSG``, which is the request URL -- and under SigV4 that URL carries
        ``X-Amz-Credential`` and ``X-Amz-Signature``.
        ``waterbutler.server.api.v1.core.write_error`` hands ``exc.message`` straight to the
        client and ``log_exception`` records the chain, so both have to be clean."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await register_presigned(provider, 'HEAD', 'head_object',
                                     path='my-subfolder/thefile.txt', default_params=True,
                                     status=403)

            with pytest.raises(exceptions.NotFoundError) as e:
                await provider.check_key_existence('my-subfolder/thefile.txt')

        assert_context_suppressed(e.value)
        assert_no_secrets(e.value)
        assert 'my-subfolder/thefile.txt' in e.value.message

    @pytest.mark.asyncio
    async def test_generate_presigned_url_does_not_chain_s3_prose(self, auth, credentials,
                                                                   settings, mock_time):
        """The other kind of original: botocore's ``ClientError``, whose message quotes S3's
        prose along with the request id and the host id."""
        provider = raw_provider(auth, credentials, settings)
        patcher, _ = patch_aiobotocore_client(
            generate_presigned_url=MockCoroutine(
                side_effect=s3_client_error('AccessDenied', 403)))

        with patcher:
            with pytest.raises(exceptions.NotFoundError) as e:
                await provider.generate_generic_presigned_url('my-subfolder/thefile.txt')

        assert_context_suppressed(e.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('abort_status', [204, 500])
    async def test_the_chunked_upload_exit_does_not_chain_the_commit_failure(
            self, auth, credentials, settings, mock_time, abort_status):
        """``_chunked_upload`` composes its message precisely so that the failure is named
        without the storage's prose.  Raising it from inside the ``except`` puts that prose
        back.  Both abort outcomes are taken: after CX1-5 the abort's own exception is caught
        too, and that handler is a second place a context can be picked up from."""
        provider = raw_provider(auth, credentials, settings)
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': 'abc'}])

        with frozen_signing_clock():
            await register_commit(provider,
                                  commit_error_xml('InternalError').encode('utf-8'))
            await register_presigned(
                provider, 'DELETE', 'abort_multipart_upload', path=COMMIT_PATH.path,
                query_parameters={'UploadId': 'SESSION'}, default_params=True,
                body=b'', status=abort_status)
            await register_presigned(
                provider, 'GET', 'list_parts', path=COMMIT_PATH.path,
                query_parameters={'UploadId': 'SESSION'}, default_params=True,
                body=b'', status=404)

            with pytest.raises(exceptions.UploadError) as e:
                await provider._chunked_upload(None, COMMIT_PATH)

        assert_context_suppressed(e.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_the_commit_error_body_is_not_chained_either(self, auth, credentials,
                                                                settings, mock_time):
        """``_complete_multipart_upload`` raises from inside the ``try`` that read the body, so
        there is no context to suppress -- pin that, because moving the raise into an
        ``except`` would silently reintroduce one."""
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await register_commit(provider,
                                  commit_error_xml('InternalError').encode('utf-8'))

            with pytest.raises(exceptions.UploadError) as e:
                await commit(provider)

        assert_context_suppressed(e.value)


class TestAbortFailureKeepsTheCommitNotice:
    """CX1-5 / K-1 × K-4: the abort raising must not take the commit's verdict with it.

    ``_abort_chunked_upload`` returns ``False`` only when it got answers it could read and
    parts were still there.  Every other way it goes wrong -- the DELETE answering 404, 403 or
    500, the LIST PARTS answering anything outside ``(200, 201, 404)`` -- comes back out as an
    exception from ``make_request``.  Raised where the notice has just been computed and not
    yet used, that exception discards both the failure sentence and the notice, and the user
    is told only that the cleanup failed.

    404 ``NoSuchUpload`` on the abort is the worst cell of the table and a real answer: it is
    what S3 says when the ``UploadId`` is already consumed, which is exactly the case where
    the commit did succeed and the user most needs to be told to go and look.
    """

    @staticmethod
    async def arrange(provider, abort_status, commit_error_code):
        """Fail the commit with ``commit_error_code`` and the abort with ``abort_status``.

        Both requests go out to the URL the real presigner produced, so the abort really does
        travel through ``make_request`` and raise the way it would against S3.
        """
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': 'abc'}])

        await register_commit(provider, commit_error_xml(commit_error_code).encode('utf-8'))
        await register_presigned(
            provider, 'DELETE', 'abort_multipart_upload', path=COMMIT_PATH.path,
            query_parameters={'UploadId': 'SESSION'}, default_params=True,
            body=commit_error_xml('NoSuchUpload').encode('utf-8'), status=abort_status)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize('abort_status', [404, 403, 500])
    @pytest.mark.parametrize('commit_error_code,expect_notice', [
        ('InternalError', True),     # UNKNOWN -- the notice is the whole point
        ('AccessDenied', False),     # NOT_COMMITTED -- and it must stay suppressed
    ])
    async def test_the_upload_failure_survives_an_abort_that_raises(
            self, auth, credentials, settings, mock_time,
            abort_status, commit_error_code, expect_notice):
        provider = raw_provider(auth, credentials, settings)

        with frozen_signing_clock():
            await self.arrange(provider, abort_status, commit_error_code)

            with pytest.raises(exceptions.UploadError) as e:
                await provider._chunked_upload(None, COMMIT_PATH)

        message = e.value.message
        assert 'An unexpected error has occurred' in message
        assert (S3Provider.UPLOAD_MAY_HAVE_COMPLETED_MESSAGE in message) is expect_notice
        # An abort that raised cleaned nothing up, so the user is told to do it by hand.
        assert 'manually remove them' in message
        assert 'The upload is aborted.' not in message
        assert_no_secrets(e.value)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_a_cancellation_from_the_abort_still_propagates(self, auth, credentials,
                                                                   settings, mock_time):
        """Catching the abort's failures must not catch a cancellation with them: swallowing
        it and raising ``UploadError`` instead stops the cancellation propagating and the task
        never ends.  Python 3.6 derives ``CancelledError`` from ``Exception``, so a bare
        ``except Exception`` does catch it."""
        provider = raw_provider(auth, credentials, settings)
        provider._create_upload_session = MockCoroutine(return_value='SESSION')
        provider._upload_parts = MockCoroutine(return_value=[{'ETAG': 'abc'}])
        provider._abort_chunked_upload = MockCoroutine(side_effect=asyncio.CancelledError())

        with frozen_signing_clock():
            await register_commit(provider,
                                  commit_error_xml('InternalError').encode('utf-8'))

            with pytest.raises(asyncio.CancelledError):
                await provider._chunked_upload(None, COMMIT_PATH)
