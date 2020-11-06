import requests
import requests.exceptions
import singer
import time
import urllib
import json

LOGGER = singer.get_logger()


def set_query_parameters(url, **params):
    """Given a URL, set or replace a query parameter and return the
    modified URL.
    >>> set_query_parameters('http://example.com?foo=bar&biz=baz', foo='stuff', bat='boots')
    'http://example.com?foo=stuff&biz=baz&bat=boots'
    """
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(query_string)

    new_query_string = ''

    for param_name, param_value in params.items():
        query_params[param_name] = [param_value]
        new_query_string = urllib.parse.urlencode(query_params, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, new_query_string, fragment))


class Client:
    BASE_URL = 'https://api.livechatinc.com'
    MAX_ATTEMPTS = 10

    def __init__(self, config):
        entity_id = config['entity_id']
        access_token = config['access_token']
        self.auth = (entity_id, access_token)
        self.headers = {'X-API-Version': '2'}

    def get(self, url, params=None):

        if not url.startswith('https://'):
            url = f'{self.BASE_URL}/{url}'

        if params:
            url = set_query_parameters(url, query=json.dumps(params))

        LOGGER.info(f'URL: {url}')

        for num_retries in range(self.MAX_ATTEMPTS):
            will_retry = num_retries < self.MAX_ATTEMPTS - 1
            try:
                resp = requests.get(url, auth=self.auth, headers=self.headers)
            # Catch the base exception from requests
            except requests.exceptions.RequestException:
                resp = None
                if will_retry:
                    LOGGER.info('Livechat: unable to get response, will retry', exc_info=True)
                else:
                    LOGGER.error(f'Livechat: unable to get response, exceeded retries', exc_info=True)
            if will_retry:
                if resp and resp.status_code >= 500:
                    LOGGER.info('livechat request with 5xx response, retrying', extra={
                        'url': resp.url,
                        'reason': resp.reason,
                        'code': resp.status_code
                    })
                # resp will be None if there was a `ConnectionError`
                elif resp is not None:
                    break  # No retry needed
                time.sleep(60)

        resp.raise_for_status()
        return resp.json()

    def paging_get(self, url, results_key='chats', **get_args):
        """Iterates through all pages and yields results, one object at a time"""
        next_page = 1
        total_returned = 0

        get_args = {k: v for k, v in get_args.items() if v is not None}
        while next_page:
            get_args['page'] = next_page
            # params = {'page': next_page}
            data = self.get(set_query_parameters(url, **get_args))

            if next_page == 1:
                total_size = data.get('total')
                pages = data.get('pages')

            LOGGER.info(f'total_size: {total_size}')
            LOGGER.info(f'pages: {pages}')

            LOGGER.info('Livechat paging request', extra={
                'total_size': total_size,
                'page': next_page,
                'pages': pages,
                'url': url,
                'total_returned': total_returned
            })

            for record in data[results_key]:
                total_returned += 1
                yield record

            LOGGER.info(f'total returned: {total_returned}')

            next_page = next_page + 1 if total_returned < total_size else None
