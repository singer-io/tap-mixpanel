import base64
import io

import backoff
import jsonlines
import requests
import singer
from requests.exceptions import ConnectionError, Timeout
from requests.models import ProtocolError
from singer import metrics

LOGGER = singer.get_logger()

BACKOFF_MAX_TRIES_REQUEST = 7
REQUEST_TIMEOUT = 300


class ReadTimeoutError(Exception):
    pass


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class MixpanelError(Exception):
    pass


class MixpanelBadRequestError(MixpanelError):
    pass


class MixpanelUnauthorizedError(MixpanelError):
    pass


class MixpanelPaymentRequiredError(MixpanelError):
    pass


class MixpanelNotFoundError(MixpanelError):
    pass


class MixpanelForbiddenError(MixpanelError):
    pass


class MixpanelInternalServiceError(Server5xxError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": MixpanelBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": MixpanelUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    402: {
        "raise_exception": MixpanelPaymentRequiredError,
        "message": "Your current plan does not allow API calls. Payment is required to complete the operation."
    },
    403: {
        "raise_exception": MixpanelForbiddenError,
        "message": "User does not have permission to access the resource."
    },
    404: {
        "raise_exception": MixpanelNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    429: {
        "raise_exception": Server429Error,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": MixpanelInternalServiceError,
        "message": "Server encountered an unexpected condition that prevented it from fulfilling the request."
    }
}

def raise_for_error(response):
    LOGGER.error('ERROR %s: %s, REASON: %s', response.status_code,
                                             response.text, 
                                             response.reason)
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    error_code = response.status_code
    error_message = response_json.get(
        "error", response_json.get(
            "message", ERROR_CODE_EXCEPTION_MAPPING.get(
                error_code, {}).get(
                    "message", "Unknown Error")))

    # if response text contains something unusual error of to_date then provide helper message of timezone mismatch
    # E.g error: to_date cannot be later than today
    if error_code == 400:
        if "to_date" in response.text:
            error_message += " Please validate the timezone with the MixPanel UI under project settings."
        else:
            error_message = '{}(Please verify your credentials.)'.format(error_message)

    message = "HTTP-error-code: {}, Error: {}".format(error_code, error_message)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(
        error_code, {}).get("raise_exception", MixpanelError)
    raise exc(message) from None


class MixpanelClient(object):
    def __init__(self,
                 api_secret,
                 api_domain,
                 request_timeout,
                 user_agent=None):
        self.__api_secret = api_secret
        self.__api_domain = api_domain
        self.__request_timeout = request_timeout
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__verified = False
        self.disable_engage_endpoint = False

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, Server429Error, ReadTimeoutError, ConnectionError, Timeout, ProtocolError),
                          max_tries=5,
                          factor=2)
    def check_access(self):
        if self.__api_secret is None:
            raise Exception('Error: Missing api_secret in tap config.json.')
        headers = {}
        # Endpoint: simple API call to return a single record (org settings) to test access
        url = 'https://{}/api/2.0/engage'.format(self.__api_domain)
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'
        headers['Authorization'] = 'Basic {}'.format(
            str(base64.urlsafe_b64encode(self.__api_secret.encode("utf-8")), "utf-8"))

        try:
            response = self.__session.get(
                url=url,
                timeout=self.__request_timeout, # Request timeout parameter
                headers=headers)
        except requests.exceptions.Timeout as err:
            LOGGER.error('TIMEOUT ERROR: %s',str(err))
            raise ReadTimeoutError

        if response.status_code == 402:
            # 402 Payment Requirement does not indicate a permissions or authentication error
            self.disable_engage_endpoint = True
            LOGGER.warning('Mixpanel returned a 402 from the Engage API. Engage stream will be skipped.')
            return True
        elif response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            return True

    @backoff.on_exception(
        backoff.expo,
        (Server5xxError, Server429Error, ReadTimeoutError, ConnectionError, Timeout, ProtocolError),
        max_tries=BACKOFF_MAX_TRIES_REQUEST,
        factor=3,
        logger=LOGGER)
    def perform_request(self,
                        method,
                        url=None,
                        params=None,
                        json=None,
                        stream=False,
                        **kwargs):
        try:
            response = self.__session.request(method=method,
                                              url=url,
                                              params=params,
                                              json=json,
                                              stream=stream,
                                              timeout=self.__request_timeout, # Request timeout parameter
                                              **kwargs)

            if response.status_code > 500:
                raise Server5xxError()

            if response.status_code != 200:
                raise_for_error(response)
            return response
        except requests.exceptions.Timeout as err:
            LOGGER.error('TIMEOUT ERROR: %s',str(err))
            raise ReadTimeoutError(err)

    def request(self, method, url=None, path=None, params=None, json=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if url and path:
            url = '{}/{}'.format(url, path)
        elif path and not url:
            url = 'https://{}/api/2.0/{}'.format(self.__api_domain, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        kwargs['headers']['Authorization'] = 'Basic {}'.format(
            str(base64.urlsafe_b64encode(self.__api_secret.encode("utf-8")), "utf-8"))
        with metrics.http_request_timer(endpoint) as timer:
            response = self.perform_request(method=method,
                                            url=url,
                                            params=params,
                                            json=json,
                                            **kwargs)

            timer.tags[metrics.Tag.http_status_code] = response.status_code

        response_json = response.json()
        return response_json

    def request_export(self, method, url=None, path=None, params=None, json=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if url and path:
            url = '{}/{}'.format(url, path)
        elif path and not url:
            url = 'https://{}/api/2.0/{}'.format(self.__api_domain, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = 'export'

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        kwargs['headers']['Authorization'] = 'Basic {}'.format(
            str(base64.urlsafe_b64encode(self.__api_secret.encode("utf-8")), "utf-8"))
        with metrics.http_request_timer(endpoint) as timer:
            response = self.perform_request(method=method,
                                            url=url,
                                            params=params,
                                            json=json,
                                            stream=True,
                                            **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

            # export endpoint returns jsonl results;
            #  other endpoints return json with array of results
            #  jsonlines reference: https://jsonlines.readthedocs.io/en/latest/
            reader = jsonlines.Reader(response.iter_lines())
            for record in reader.iter(allow_none=True, skip_empty=True):
                yield record
