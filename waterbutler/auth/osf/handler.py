import inspect  # noqa
import logging
import datetime

import jwe
import jwt
import aiohttp
from aiohttp.client_exceptions import ClientError, ContentTypeError

from waterbutler.core import exceptions
from waterbutler.auth.osf import settings
from waterbutler.utils import inspect_info  # noqa
from waterbutler.core.auth import AuthType, BaseAuthHandler
from waterbutler.settings import MFR_IDENTIFYING_HEADER


JWE_KEY = jwe.kdf(settings.JWE_SECRET.encode(), settings.JWE_SALT.encode())
EXPORT_DATA_FAKE_NODE_ID = 'export_location'

logger = logging.getLogger(__name__)


class OsfAuthHandler(BaseAuthHandler):
    """Identity lookup via the Open Science Framework"""
    ACTION_MAP = {
        'put': 'upload',
        'get': 'download',
        'head': 'metadata',
        'delete': 'delete',
    }

    def build_payload(self, bundle, view_only=None, cookie=None):
        query_params = {}

        if cookie:
            bundle['cookie'] = cookie

        if view_only:
            # View only must go outside of the jwt
            query_params['view_only'] = view_only

        raw_payload = jwe.encrypt(jwt.encode({
            'data': bundle,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)
        }, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM), JWE_KEY)

        # Note: `aiohttp3` uses `yarl` which only supports string parameters
        query_params['payload'] = raw_payload.decode("utf-8")

        return query_params

    async def make_request(self, params, headers, cookies):
        try:
            # Note: with simple request whose response is handled right afterwards without "being passed
            #       further along", use the context manager so WB doesn't need to handle the sessions.
            async with aiohttp.request(
                'get',
                settings.API_URL,
                params=params,
                headers=headers,
                cookies=cookies,
            ) as response:
                if response.status != 200:
                    try:
                        data = await response.json()
                    except (ValueError, ContentTypeError):
                        data = await response.read()
                    raise exceptions.AuthError(data, code=response.status)

                try:
                    raw = await response.json()
                    signed_jwt = jwe.decrypt(raw['payload'].encode(), JWE_KEY)
                    data = jwt.decode(signed_jwt, settings.JWT_SECRET,
                                      algorithm=settings.JWT_ALGORITHM,
                                      options={'require_exp': True})
                    return data['data']
                except (jwt.InvalidTokenError, KeyError):
                    raise exceptions.AuthError(data, code=response.status)
        except ClientError:
            raise exceptions.AuthError('Unable to connect to auth sever', code=503)

    async def fetch(self, request, bundle):
        """Used for v0"""
        headers = {'Content-Type': 'application/json'}

        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']

        cookie = request.query_arguments.get('cookie')
        if cookie:
            cookie = cookie[0].decode()

        view_only = request.query_arguments.get('view_only')
        if view_only:
            view_only = view_only[0].decode()

        payload = (await self.make_request(
            self.build_payload(bundle, cookie=cookie, view_only=view_only),
            headers,
            dict(request.cookies)
        ))

        payload['auth']['callback_url'] = payload['callback_url']
        return payload

    async def get(self, resource, provider, request, action=None, auth_type=AuthType.SOURCE,
                  path='', version=None, callback_log=True, location_id=None):
        """Used for v1"""
        method = request.method.lower()

        if method == 'post' and action:
            post_action_map = {
                'copy': 'download' if auth_type is AuthType.SOURCE else 'upload',
                'rename': 'upload',
                'move': 'upload',
            }

            try:
                osf_action = post_action_map[action.lower()]
            except KeyError:
                raise exceptions.UnsupportedActionError(method, supported=post_action_map.keys())
        elif method == 'head' and settings.MFR_ACTION_HEADER in request.headers:
            mfr_action_map = {'render': 'render', 'export': 'export'}
            mfr_action = request.headers[settings.MFR_ACTION_HEADER].lower()
            try:
                osf_action = mfr_action_map[mfr_action]
            except KeyError:
                raise exceptions.UnsupportedActionError(mfr_action, supported=mfr_action_map.keys())
        else:
            try:
                if method == 'get' and 'meta' in request.query_arguments:
                    osf_action = 'metadata'
                elif method == 'get' and 'revisions' in request.query_arguments:
                    osf_action = 'revisions'
                else:
                    osf_action = self.ACTION_MAP[method]
            except KeyError:
                raise exceptions.UnsupportedHTTPMethodError(method, supported=self.ACTION_MAP.keys())

        headers = {'Content-Type': 'application/json'}

        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']

        if MFR_IDENTIFYING_HEADER in request.headers:
            headers[MFR_IDENTIFYING_HEADER] = request.headers[MFR_IDENTIFYING_HEADER]

        cookie = request.query_arguments.get('cookie')
        if cookie:
            cookie = cookie[0].decode()

        view_only = request.query_arguments.get('view_only')
        if view_only:
            # View only must go outside of the jwt
            view_only = view_only[0].decode()

        data = {
            'nid': resource,
            'provider': provider,
            'action': osf_action,
            'path': path,
            'version': version,
            'metrics': {
                'referrer': request.headers.get('Referer'),
                'user_agent': request.headers.get('User-Agent'),
                'origin': request.headers.get('Origin'),
                'uri': request.uri,
            },
            'callback_log': callback_log
        }
        if resource == EXPORT_DATA_FAKE_NODE_ID:
            data['location_id'] = location_id

        payload = await self.make_request(
            self.build_payload(data, cookie=cookie, view_only=view_only),
            headers,
            dict(request.cookies)
        )

        payload['auth']['callback_url'] = payload['callback_url'] if callback_log else ''
        return payload
