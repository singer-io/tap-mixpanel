import unittest
import requests

import jsonlines
from unittest import mock
from parameterized import parameterized

from tap_mixpanel import client
from tap_mixpanel import streams

# Mock response
REQUEST_TIMEOUT = 300


class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp = "", content=[""], headers=None, raise_error=True, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text


def get_mock_http_response(content, status_code):
    """Mock response for timezone related error messages.

    Args:
        content (str): Returns the content of the response, in bytes.
        status_code (int): Returns a number that indicates the status.

    Returns:
        request.Response: Custom mock response.
    """
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = content.encode()
    return response


# Mock time.sleep to reduce the time
@mock.patch("time.sleep", return_value=None)
class TestMixpanelErrorHandling(unittest.TestCase):
    """
    Test case to verify the custom error message and
    back off is implemented for mentioned errors in tests.
    """

    timeout_400_error = {
        "request": "/api/2.0/engage/revenue?from_date=2020-02-01&to_date=2020-03-01",
        "error": "Timeout Error.",
    }

    def mock_400_different_timezone(*args, **kwargs):
        """Mock 400 error response with with different timezone.

        Returns:
            requests.Response: Returns mock 400 error response.
        """
        content = " to_date cannot be later than today"
        return get_mock_http_response(content, 400)

    def mock_send_error(*args, **kwargs):
        """Mock error response with description in \'error\' field.

        Returns:
            requests.Response: Returns mock 404 error response.
        """
        content = '{"error": "Resource not found error message from API response field \'error\'."}'
        return get_mock_http_response(content, 404)

    def mock_send_message(*args, **kwargs):
        """Mock error response with description in \'message\' field.

        Returns:
            requests.Response: Returns mock 404 error response.
        """
        content = '{"message": "Resource not found error message from API response field \'message\'."}'
        return get_mock_http_response(content, 404)

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), client.MixpanelBadRequestError, "A validation exception has occurred.(Please verify your credentials.)"],
        ["400 different timezone error", 400, mock_400_different_timezone(), client.MixpanelBadRequestError, "A validation exception has occurred. Please validate the timezone with the MixPanel UI under project settings."],
        ["400 timeout error", 400, MockResponse(400, text=timeout_400_error), client.MixpanelBadRequestError, "Timeout Error.(Please verify your credentials.)"],
        ["401 error", 401, MockResponse(401), client.MixpanelUnauthorizedError, "Invalid authorization credentials."],
        ["402 error", 402, MockResponse(402), client.MixpanelPaymentRequiredError, "Your current plan does not allow API calls. Payment is required to complete the operation."],
        ["403 error", 403, MockResponse(403), client.MixpanelForbiddenError, "User does not have permission to access the resource."],
        ["404 error", 404, MockResponse(404), client.MixpanelNotFoundError, "The resource you have specified cannot be found."],
        ["404 error", 404, mock_send_error(), client.MixpanelNotFoundError, "Resource not found error message from API response field 'error'."],
        ["404 error", 404, mock_send_message(), client.MixpanelNotFoundError, "Resource not found error message from API response field 'message'."],
        ["429 error", 429, MockResponse(429), client.Server429Error, "The API rate limit for your organization/application pairing has been exceeded."],
    ])
    @mock.patch("requests.Session.request")
    def test_perform_request_exception_handling(
        self, test_name, error_code, mock_response, error, error_message, mock_request, mock_sleep,
    ):
        """
        Test that `perform_request` method handle error with proper message.
        """
        mock_request.return_value = mock_response
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=REQUEST_TIMEOUT,
        )
        with self.assertRaises(error) as e:
            mock_client.perform_request("GET")

        expected_error_message = (
            f"HTTP-error-code: {error_code}, Error: {error_message}"
        )

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), client.MixpanelBadRequestError, "A validation exception has occurred.(Please verify your credentials.)"],
        ["400 different timezone error", 400, mock_400_different_timezone(), client.MixpanelBadRequestError, "A validation exception has occurred. Please validate the timezone with the MixPanel UI under project settings."],
        ["400 timeout error", 400, MockResponse(400, text=timeout_400_error), client.MixpanelBadRequestError, "Timeout Error.(Please verify your credentials.)"],
        ["401 error", 401, MockResponse(401), client.MixpanelUnauthorizedError, "Invalid authorization credentials."],
        ["403 error", 403, MockResponse(403), client.MixpanelForbiddenError, "User does not have permission to access the resource."],
        ["404 error", 404, MockResponse(404), client.MixpanelNotFoundError, "The resource you have specified cannot be found."],
        ["404 error", 404, mock_send_error(), client.MixpanelNotFoundError, "Resource not found error message from API response field 'error'."],
        ["404 error", 404, mock_send_message(), client.MixpanelNotFoundError, "Resource not found error message from API response field 'message'."],
        ["429 error", 429, MockResponse(429), client.Server429Error, "The API rate limit for your organization/application pairing has been exceeded."],
        ["500 error", 500, MockResponse(500), client.MixpanelInternalServiceError, "Server encountered an unexpected condition that prevented it from fulfilling the request."],
        ["501 error", 501, MockResponse(501), client.MixpanelError, "Unknown Error"],
    ])
    @mock.patch("requests.Session.get")
    def test_check_access_exception_handling(
        self, test_name, error_code, mock_response, error, error_message, mock_request, mock_sleep,
    ):
        """
        Test that `check_access` method handle error with proper message.
        """
        mock_request.return_value = mock_response
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=REQUEST_TIMEOUT,
        )
        with self.assertRaises(error) as e:
            mock_client.check_access()

        expected_error_message = (
            f"HTTP-error-code: {error_code}, Error: {error_message}"
        )

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand(
        [
            ["500 error", MockResponse(500), client.MixpanelInternalServiceError],
            ["501 error", MockResponse(501), client.Server5xxError],
        ]
    )
    @mock.patch("requests.Session.request")
    def test_request_with_handling_for_5xx_exception_handling(
        self, test_name, mock_response, error, mock_request, mock_sleep
    ):
        """
        Test that `perform_request` method handle 5xx error with proper message.
        """
        mock_request.return_value = mock_response
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=REQUEST_TIMEOUT,
        )
        with self.assertRaises(error):
            mock_client.perform_request("GET")

    @mock.patch("requests.Session.request", side_effect=requests.exceptions.Timeout)
    def test_check_access_handle_timeout_error(self, mock_request, mock_sleep):
        """
        Check whether the request back off properly for `check_access`
        method for 5 times in case of Timeout error.
        """
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=REQUEST_TIMEOUT,
        )
        with self.assertRaises(client.ReadTimeoutError):
            mock_client.check_access()

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("requests.Session.request")
    @mock.patch("tap_mixpanel.client.LOGGER.warning")
    def test_check_access_402_exception_handling(
        self, mock_logger, mock_request, mock_sleep
    ):
        """
        Test that `check_access` method does not throw 402 error and prints
        warning logger instead.
        """
        mock_request.return_value = MockResponse(402)
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=REQUEST_TIMEOUT,
        )

        mock_client.check_access()

        # Verify that for 402 error expected logger is printed.
        mock_logger.assert_called_with(
            "Mixpanel returned a 402 from the Engage API. Engage stream will be skipped."
        )


# Mock time.sleep to reduce the time
@mock.patch("time.sleep", return_value=None)
class TestMixpanelConnectionResetErrorHandling(unittest.TestCase):

    @mock.patch("requests.Session.request", side_effect=requests.models.ProtocolError)
    def test_check_access_handle_timeout_error(self, mock_request, mock_time):
        """
        Check whether the request backoffs properly for `check_access` method for 5 times in case of Timeout error.
        """
        mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
        with self.assertRaises(requests.models.ProtocolError):
            mock_client.check_access()

        # Verify that requests.Session.request is called 5 times
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("jsonlines.jsonlines.Reader.iter", side_effect=requests.exceptions.ChunkedEncodingError)
    def test_ChunkedEncodingError(self, mock_jsonlines, mock_time):
        """
        Check whether the request backoffs properly for `check_access` method for 5 times in case of Timeout error.
        """
        mock_client = client.MixpanelClient(api_secret="mock_api_secret", api_domain="mock_api_domain", request_timeout=REQUEST_TIMEOUT)
        mock_client._MixpanelClient__verified = True

        fake_response = MockResponse(500)
        fake_response.iter_lines = lambda : []
        mock_client.perform_request = lambda *args, **kwargs: fake_response

        stream = streams.Export(mock_client)

        with self.assertRaises(requests.exceptions.ChunkedEncodingError) as error:
            stream.get_and_transform_records(
                querystring={},
                project_timezone=None,
                max_bookmark_value=None,
                state=None,
                config=None,
                catalog=None,
                selected_streams=None,
                last_datetime=None,
                endpoint_total=None,
                limit=None,
                total_records=None,
                parent_total=None,
                record_count=None,
                page=None,
                offset=None,
                parent_record=None,
                date_total=None,
            )

        self.assertEqual(mock_jsonlines.call_count, 5)
