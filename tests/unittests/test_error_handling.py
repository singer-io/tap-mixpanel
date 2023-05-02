import unittest
from unittest import mock
import requests
from tap_mixpanel import client

# mock responce
REQUEST_TIMEOUT = 300


class Mockresponse:
    """
    Mocked standard HTTPResponse to test error handling.
    """
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

# Mock response for timezone related error messages
def get_mock_http_response(content, status_code):
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = content.encode()
    return response

@mock.patch('time.sleep', return_value=None) # Mock time.sleep to reduce the time
class TestMixpanelErrorHandling(unittest.TestCase):

    def mock_send_400(*args, **kwargs):
        return Mockresponse("", 400, raise_error=True)

    def mock_400_different_timezone(*args, **kwargs):
        content = " to_date cannot be later than today"
        return get_mock_http_response(content, 400)

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

    def mock_send_error(*args, **kwargs):
        content = '{"error": "Resource not found error message from API response field \'error\'."}'
        return get_mock_http_response(content, 404)

    def mock_send_message(*args, **kwargs):
        content = '{"message": "Resource not found error message from API response field \'message\'."}'
        return get_mock_http_response(content, 404)

    @mock.patch("requests.Session.request", side_effect=mock_send_400)
    def test_request_with_handling_for_400_exception_handling(self, mock_send_400, mock_sleep):
        """
        Test that `perform_request` method handle 400 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred.(Please verify your credentials.)"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_400_different_timezone)
    def test_request_with_handling_for_400_for_different_timezone(self, mock_400_different_timezone, mock_sleep):
        """
        Test that `perform_request` method handle 400 error with proper message for different timezone
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred. Please validate the timezone with the MixPanel UI under project settings."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request")
    def test_request_with_handling_for_400_timeout_error_handling(self, mock_request, mock_sleep):
        """
        Test that `perform_request` method handle 400 error with timeout error message in case of `error` field in response
        """
        error = {"request": "/api/2.0/engage/revenue?from_date=2020-02-01&to_date=2020-03-01", "error": "Timeout Error."}
        mock_request.return_value = Mockresponse("", 400, raise_error=True, text=error)
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: Timeout Error.(Please verify your credentials.)"
            # Verifying the message formed for the timeout error
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_401)
    def test_request_with_handling_for_401_exception_handling(self, mock_send_401, mock_sleep):
        """
        Test that `perform_request` method handle 401 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_402)
    def test_request_with_handling_for_402_exception_handling(self, mock_send_402, mock_sleep):
        """
        Test that `perform_request` method handle 402 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelPaymentRequiredError as e:
            expected_error_message = "HTTP-error-code: 402, Error: Your current plan does not allow API calls. Payment is required to complete the operation."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_403)
    def test_request_with_handling_for_403_exception_handling(self, mock_send_403, mock_sleep):
        """
        Test that `perform_request` method handle 403 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User does not have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_404)
    def test_request_with_handling_for_404_exception_handling(self, mock_send_404, mock_sleep):
        """
        Test that `perform_request` method handle 404 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_429)
    def test_request_with_handling_for_429_exception_handling(self, mock_send_429, mock_sleep):
        """
        Test that `perform_request` method handle 429 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.Server429Error as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_500)
    def test_request_with_handling_for_500_exception_handling(self, mock_send_500, mock_sleep):
        """
        Test that `perform_request` method handle 500 error with proper message
        """
        with self.assertRaises(client.MixpanelInternalServiceError):
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')

    @mock.patch("requests.Session.request", side_effect=mock_send_501)
    def test_request_with_handling_for_501_exception_handling(self, mock_send_501, mock_sleep):
        """
        Test that `perform_request` method handle 501 error with proper message
        """
        with self.assertRaises(client.Server5xxError):
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')

    @mock.patch("requests.Session.request", side_effect=mock_send_error)
    def test_request_with_handling_for_404_exception_handling_error(self, mock_send_error, mock_sleep):
        '''
            Verify that if 'error' field is present in API response then it should be used as error message.
        '''
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: Resource not found error message from API response field 'error'."
            # Verifying the message retrived from 'error' field of API response
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_message)
    def test_request_with_handling_for_404_exception_handling_message(self, mock_send_message, mock_sleep):
        '''
            Verify that if 'message' field is present in API response then it should be used as error message.
        '''
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.perform_request('GET')
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: Resource not found error message from API response field 'message'."
            # Verifying the message retrived from 'message' field of API response
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.get", side_effect=mock_send_400)
    def test_check_access_with_handling_for_400_exception_handling(self, mock_send_400, mock_sleep):
        """
        Test that `check_access` method handle 404 error with proper message
        """
        try:
            tap_stream_id = "tap_mixpanel"
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred.(Please verify your credentials.)"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.get", side_effect=mock_400_different_timezone)
    def test_check_access_with_handling_for_400_for_different_timezone(self, mock_400_different_timezone, mock_sleep):
        """
        Test that `check_access` method handle 404 error with proper message for different timezone
        """
        try:
            tap_stream_id = "tap_mixpanel"
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred. Please validate the timezone with the MixPanel UI under project settings."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.get")
    def test_check_access_with_handling_for_400_timeout_error_handling(self, mock_request, mock_sleep):
        """
        Test that `check_access` method handle 404 error with timeout error message in case of `error` field in response
        """
        error = {"request": "/api/2.0/engage/revenue?from_date=2020-02-01&to_date=2020-03-01", "error": "Timeout Error."}
        mock_request.return_value = Mockresponse("", 400, raise_error=True, text=error)
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: Timeout Error.(Please verify your credentials.)"
            # Verifying the message formed for the timeout error
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_401)
    def test_check_access_with_handling_for_401_exception_handling(self, mock_send_401, mock_sleep):
        """
        Test that `check_access` method handle 401 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_403)
    def test_check_access_with_handling_for_403_exception_handling(self, mock_send_403, mock_sleep):
        """
        Test that `check_access` method handle 403 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User does not have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_404)
    def test_check_access_with_handling_for_404_exception_handling(self, mock_send_404, mock_sleep):
        """
        Test that `check_access` method handle 404 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_429)
    def test_check_access_with_handling_for_429_exception_handling(self, mock_send_429, mock_sleep):
        """
        Test that `check_access` method handle 429 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.Server429Error as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_500)
    def test_check_access_with_handling_for_500_exception_handling(self, mock_send_500, mock_sleep):
        """
        Test that `check_access` method handle 500 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelInternalServiceError as e:
            expected_error_message = "HTTP-error-code: 500, Error: Server encountered an unexpected condition that prevented it from fulfilling the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_501)
    def test_check_access_with_handling_for_501_exception_handling(self, mock_send_501, mock_sleep):
        """
        Test that `check_access` method handle 501 error with proper message
        """
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelError as e:
            expected_error_message = "HTTP-error-code: 501, Error: Unknown Error"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)


    @mock.patch("requests.Session.request", side_effect=mock_send_error)
    def test_check_access_with_handling_for_404_exception_handling_error(self, mock_send_error, mock_sleep):
        '''
            Verify that if 'error' field is present in API response then it should be used as error message.
        '''
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: Resource not found error message from API response field 'error'."
            # Verifying the message retrived from 'error' field of API response
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=mock_send_message)
    def test_check_access_with_handling_for_404_exception_handling_message(self, mock_send_message, mock_sleep):
        '''
            Verify that if 'message' field is present in API response then it should be used as error message.
        '''
        try:
            mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
            mock_client.check_access()
        except client.MixpanelNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: Resource not found error message from API response field 'message'."
            # Verifying the message retrived from 'message' field of API response
            self.assertEqual(str(e), expected_error_message)

    @mock.patch("requests.Session.request", side_effect=requests.exceptions.Timeout)
    def test_check_access_handle_timeout_error(self, mock_request, mock_sleep):
        '''
        Check whether the request backoffs properly for `check_access` method for 5 times in case of Timeout error.
        '''
        mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
        with self.assertRaises(client.ReadTimeoutError):
            mock_client.check_access()

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mock_request.call_count, 5)




class TestMixpanelConnectionResetErrorHandling(unittest.TestCase):

    @mock.patch("requests.Session.request", side_effect=requests.models.ProtocolError)
    def test_check_access_handle_timeout_error(self, mock_request):
        '''
        Check whether the request backoffs properly for `check_access` method for 5 times in case of Timeout error.
        '''
        mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
        with self.assertRaises(requests.models.ProtocolError):
            mock_client.check_access()

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mock_request.call_count, 5)
