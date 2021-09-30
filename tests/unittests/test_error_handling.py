import unittest
from unittest import mock
import requests
from tap_mixpanel import client

# mock responce


class Mockresponse:
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        return self.text


class TestMixpanelErrorHandling(unittest.TestCase):

    def mock_send_400(*args, **kwargs):
        return Mockresponse("", 400, raise_error=True)

    def mock_send_401(*args, **kwargs):
        return Mockresponse("", 401, raise_error=True)

    def mock_send_402(*args, **kwargs):
        return Mockresponse("", 402, raise_error=True)

    def mock_send_403(*args, **kwargs):
        return Mockresponse("", 403, raise_error=True)

    def mock_send_404(*args, **kwargs):
        return Mockresponse("", 404, raise_error=True)

    def mock_send_429(*args, **kwargs):
        return Mockresponse("", 429, raise_error=True)

    def mock_send_500(*args, **kwargs):
        return Mockresponse("", 500, raise_error=True)

    def mock_send_501(*args, **kwargs):
        return Mockresponse("", 501, raise_error=True)

    @mock.patch("requests.Session.request", side_effect=mock_send_400)
    def test_request_with_handling_for_400_exception_handling(self, mock_send_400):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred.(Please verify your credentials.)"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request")
    def test_request_with_handling_for_400_timeout_error_handling(self, mock_request):
        error = {"request": "/api/2.0/engage/revenue?from_date=2020-02-01&to_date=2020-03-01", "error": "Timeout Error."}
        mock_request.return_value = Mockresponse("", 400, raise_error=True, text=error)
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: Timeout Error.(Please verify your credentials.)"
            # Verifying the message formed for the timeout error
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_401)
    def test_request_with_handling_for_401_exception_handling(self, mock_send_401):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.MixpanelUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_402)
    def test_request_with_handling_for_402_exception_handling(self, mock_send_402):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.MixpanelRequestFailedError as e:
            expected_error_message = "HTTP-error-code: 402, Error: Request can not be processed."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_403)
    def test_request_with_handling_for_403_exception_handling(self, mock_send_403):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.MixpanelForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_404)
    def test_request_with_handling_for_404_exception_handling(self, mock_send_404):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_429)
    def test_request_with_handling_for_429_exception_handling(self, mock_send_429):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')
        except client.Server429Error as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_500)
    def test_request_with_handling_for_500_exception_handling(self, mock_send_500):
        with self.assertRaises(client.MixpanelInternalServiceError):
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')

    @mock.patch("requests.Session.request", side_effect=mock_send_501)
    def test_request_with_handling_for_501_exception_handling(self, mock_send_501):
        with self.assertRaises(client.Server5xxError):
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.perform_request('GET')

    @mock.patch("requests.Session.get", side_effect=mock_send_400)
    def test_check_access_with_handling_for_400_exception_handling(self, mock_send_400):
        try:
            tap_stream_id = "tap_mixpanel"
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred.(Please verify your credentials.)"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.get")
    def test_check_access_with_handling_for_400_timeout_error_handling(self, mock_request):
        error = {"request": "/api/2.0/engage/revenue?from_date=2020-02-01&to_date=2020-03-01", "error": "Timeout Error."}
        mock_request.return_value = Mockresponse("", 400, raise_error=True, text=error)
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: Timeout Error.(Please verify your credentials.)"
            # Verifying the message formed for the timeout error
            self.assertEqual(str(e), expected_error_message)
            
    @mock.patch("requests.Session.request", side_effect=mock_send_401)
    def test_check_access_with_handling_for_401_exception_handling(self, mock_send_401):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_403)
    def test_check_access_with_handling_for_403_exception_handling(self, mock_send_403):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_404)
    def test_check_access_with_handling_for_404_exception_handling(self, mock_send_404):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_429)
    def test_check_access_with_handling_for_429_exception_handling(self, mock_send_429):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.Server429Error as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_500)
    def test_check_access_with_handling_for_500_exception_handling(self, mock_send_500):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelInternalServiceError as e:
            expected_error_message = "HTTP-error-code: 500, Error: Server encountered an unexpected condition that prevented it from fulfilling the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_501)
    def test_check_access_with_handling_for_501_exception_handling(self, mock_send_501):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain")
            mock_client.check_access()
        except client.MixpanelError as e:
            expected_error_message = "HTTP-error-code: 501, Error: Unknown Error"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
