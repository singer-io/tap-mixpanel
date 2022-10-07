import unittest
from unittest import mock

from parameterized import parameterized

from tap_mixpanel.__init__ import main

CONFIG = {
    "api_secret": "dummy_secret",
    "date_window_size": "30",
    "attribution_window": "5",
    "project_timezone": "Europe/Amsterdam",
    "select_properties_by_default": "true",
    "start_date": "2020-02-01T00:00:00Z",
    "user_agent": "tap-mixpanel <api_user_email@your_company.com>",
    "eu_residency": False,
    "end_date": "2020-03-02T00:00:00Z",
}

REQUEST_TIMEOUT_DEFAULT = 300
TIMEOUT_FLOAT = 200.0
TIMEOUT_INT = 200
NULL_STRING = ""
ZERO_INT = 0
ZERO_STRING = "0"
STRING_INT = "200"


class MockParseArgs:
    """Mocked MockParseArgs class with custom state, discover, config
    attributes to pass unit test cases."""

    def __init__(self, state, discover, config):
        self.state = state
        self.discover = discover
        self.config = config


class Mockresponse:
    """Mocked standard HTTPResponse."""

    def __init__(self, resp, status_code):
        self.json_data = resp
        self.status_code = status_code


HEADER = {
    "User-Agent": "tap-mixpanel <api_user_email@your_company.com>",
    "Accept": "application/json",
    "Authorization": "Basic ZHVtbXlfc2VjcmV0",
}


@mock.patch("requests.Session.request", return_value=Mockresponse("", status_code=200))
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_mixpanel.__init__.do_discover", return_value="")
class TestMixpanelRequestTimeoutParameterValue(unittest.TestCase):
    """
    Test that tap handles different type of request_timeout parameter
    values.
    """

    def test_request_timeout_for_none_param_value(
        self, mock_discover, mock_parse_args, mock_request
    ):
        """Test that tap handles none value of request_timeout parameter."""
        config = CONFIG.copy()
        mock_parse_args.return_value = MockParseArgs(
            state={}, discover=True, config=config
        )
        main()

        # Verify that request method called with expected parameter value when"request_timeout" is None
        mock_request.assert_called_with(
            "GET",
            "https://mixpanel.com/api/2.0/engage",
            allow_redirects=True,
            headers=HEADER,
            timeout=REQUEST_TIMEOUT_DEFAULT,
        )

    @parameterized.expand(
        [
            ["empty value", NULL_STRING, REQUEST_TIMEOUT_DEFAULT],
            ["string value", STRING_INT, TIMEOUT_FLOAT],
            ["integer value", TIMEOUT_INT, TIMEOUT_FLOAT],
            ["float value", TIMEOUT_FLOAT, TIMEOUT_FLOAT],
            ["zero value", ZERO_INT, REQUEST_TIMEOUT_DEFAULT],
            ["zero(string) value", ZERO_STRING, REQUEST_TIMEOUT_DEFAULT],
        ]
    )
    def test_request_timeout(
        self, mock_discover, mock_parse_args, mock_request, test_name, input_value, expected_value
    ):
        """Test that tap handles various request timeout values."""
        config = CONFIG.copy()
        config["request_timeout"] = input_value
        mock_parse_args.return_value = MockParseArgs(
            state={}, discover=True, config=config
        )
        main()

        # Verify that request method called with expected parameter value
        mock_request.assert_called_with(
            "GET",
            "https://mixpanel.com/api/2.0/engage",
            allow_redirects=True,
            headers=HEADER,
            timeout=expected_value,
        )
