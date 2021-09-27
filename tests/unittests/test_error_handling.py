import unittest
from unittest import mock
import requests
from tap_mixpanel import client

# Mock response
def get_mock_http_response(content, status_code):
    contents = content
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    return response

class TestMixpanelErrorHandling(unittest.TestCase):

    def mock_send_400(*args, **kwargs):
        return get_mock_http_response("", 400)

    def mock_400_different_timezone(*args, **kwargs):
        content = " to_date cannot be later than today"
        return get_mock_http_response(content, 400)

    def mock_send_401(*args, **kwargs):
        return get_mock_http_response("", 401)

    def mock_send_402(*args, **kwargs):
        return get_mock_http_response("", 402)

    def mock_send_403(*args, **kwargs):
        return get_mock_http_response("", 403)

    def mock_send_404(*args, **kwargs):
        return get_mock_http_response("", 404)

    def mock_send_429(*args, **kwargs):
        return get_mock_http_response("", 429)

    def mock_send_500(*args, **kwargs):
        return get_mock_http_response("", 500)

    def mock_send_501(*args, **kwargs):
        return get_mock_http_response("", 501)

    @mock.patch("requests.Session.request", side_effect=mock_send_400)
    def test_request_with_handling_for_400_exception_handling(self, mock_send_400):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_400_different_timezone)
    def test_request_with_handling_for_400_for_different_timezone(self, mock_400_different_timezone):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred. Please validate the timezone with the MixPanel UI under project settings."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_401)
    def test_request_with_handling_for_401_exception_handling(self, mock_send_401):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.MixpanelUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_402)
    def test_request_with_handling_for_402_exception_handling(self, mock_send_402):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.MixpanelRequestFailedError as e:
            expected_error_message = "HTTP-error-code: 402, Error: Request can not be processed."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_403)
    def test_request_with_handling_for_403_exception_handling(self, mock_send_403):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.MixpanelForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_404)
    def test_request_with_handling_for_404_exception_handling(self, mock_send_404):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=mock_send_429)
    def test_request_with_handling_for_429_exception_handling(self, mock_send_429, mocked_sleep):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')
        except client.Server429Error as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=mock_send_500)
    def test_request_with_handling_for_500_exception_handling(self, mock_send_500, mocked_sleep):
        with self.assertRaises(client.MixpanelInternalServiceError):
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=mock_send_501)
    def test_request_with_handling_for_501_exception_handling(self, mock_send_501, mocked_sleep):
        with self.assertRaises(client.Server5xxError):
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.perform_request('GET')

    @mock.patch("requests.Session.get", side_effect=mock_send_400)
    def test_check_access_with_handling_for_400_exception_handling(self, mock_send_400):
        try:
            tap_stream_id = "tap_mixpanel"
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.get", side_effect=mock_400_different_timezone)
    def test_check_access_with_handling_for_400_for_different_timezone(self, mock_400_different_timezone):
        try:
            tap_stream_id = "tap_mixpanel"
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred. Please validate the timezone with the MixPanel UI under project settings."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_401)
    def test_check_access_with_handling_for_401_exception_handling(self, mock_send_401):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_403)
    def test_check_access_with_handling_for_403_exception_handling(self, mock_send_403):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_404)
    def test_check_access_with_handling_for_404_exception_handling(self, mock_send_404):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=mock_send_429)
    def test_check_access_with_handling_for_429_exception_handling(self, mock_send_429, mocked_sleep):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.Server429Error as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=mock_send_500)
    def test_check_access_with_handling_for_500_exception_handling(self, mock_send_500, mocked_sllep):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelInternalServiceError as e:
            expected_error_message = "HTTP-error-code: 500, Error: Server encountered an unexpected condition that prevented it from fulfilling the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_501)
    def test_check_access_with_handling_for_501_exception_handling(self, mock_send_501):
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret")
            mock_client.check_access()
        except client.MixpanelError as e:
            expected_error_message = "HTTP-error-code: 501, Error: Unknown Error"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
