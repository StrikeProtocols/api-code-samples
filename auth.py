from datetime import datetime, timezone
import hashlib
import json
from time import time
from uuid import uuid4

import requests


class StrikeAPIClient:
    def __init__(self, url, key_id, key_secret, version):
        self.url = url
        self.key_id = key_id
        self.key_secret = key_secret
        self.version = version

    def get_nonce(self):
        # nonce is chosen to be the current time in microseconds
        return int(time() * 1000000)

    def get_digest(self, timestamp, nonce, request_method, route, params, data, idempotent_id, debug=False):

        route = '/{}/{}'.format(self.version, route)

        params_str = '' if not params else '&'.join('{}={}'.format(k, v) for k, v in params.items())
        json_str = '' if not data else json.dumps(data)

        unencoded_digest = '{}|{}|{}|{}|{}|{}|{}|{}|{}'.format(
            self.key_id, self.key_secret, timestamp, nonce, request_method, route, params_str, json_str, idempotent_id
        )
        if debug:
            print('   The unencoded digest is: {}'.format(unencoded_digest))
        return hashlib.sha256(str.encode(unencoded_digest)).hexdigest()

    def get_headers(self, request_method, route, nonce, params=None, data=None, debug=False):
        idempotent_id = ''
        headers = {'Accept': 'application/json'}
        if request_method == 'POST':
            headers['Content-Type'] = 'application/json'
        if request_method in ('POST', 'PATCH', 'DELETE'):
            idempotent_id = str(uuid4())
            headers['X-Idempotent-ID'] = idempotent_id

        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        headers['Authorization'] = 'HMAC {}|{}|{}|{}'.format(
            self.key_id, timestamp, nonce, self.get_digest(timestamp, nonce, request_method, route, params, data,
                                                           idempotent_id, debug))

        if debug:
            print('   The HMAC header is: {}'.format(headers["Authorization"]))

        return headers

    def get(self, route, params=None):
        return requests.get(self.get_full_url(route),
                            headers=self.get_headers('GET', route, self.get_nonce(), params=params),
                            params=params)

    def post(self, route, data=None):
        return requests.post(self.get_full_url(route),
                             headers=self.get_headers('POST', route, self.get_nonce(), data=data),
                             json=data)

    def patch(self, route, data=None):
        return requests.patch(self.get_full_url(route),
                             headers=self.get_headers('PATCH', route, self.get_nonce(), data=data),
                             json=data)

    def delete(self, route):
        return requests.delete(self.get_full_url(route),
                               headers=self.get_headers('DELETE', route, self.get_nonce()))

    def get_full_url(self, route):
        return '{}/{}/{}'.format(self.url, self.version, route)
