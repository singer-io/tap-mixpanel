import unittest
from unittest import mock
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
    "end_date": "2020-03-02T00:00:00Z"
}

REQUEST_TIMEOUT = 300
REQUEST_TIMEOUT_FLOAT = 300.0

class MockParseArgs():
    """
    Mocked MockParseArgs class with custom state, discover, config attributes to pass unit test cases.
    """
    def __init__(self, state, discover, config):
        self.state = state
        self.discover = discover
        self.config = config

class Mockresponse:
    """
    Mocked standard HTTPResponse.
    """
    def __init__(self, resp, status_code):
        self.json_data = resp
        self.status_code = status_code

HEADER = {
        'User-Agent': 'tap-mixpanel <api_user_email@your_company.com>',
        'Accept': 'application/json',
        'Authorization': 'Basic ZHVtbXlfc2VjcmV0'
    }
   
@mock.patch("requests.Session.request", return_value = Mockresponse("", status_code=200))
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_mixpanel.__init__.do_discover", return_value = '')
class TestMixpanelRequestTimeoutParameterValue(unittest.TestCase):
    """Test that tap handles different type of request_timeout parameter values"""
    def test_request_timeout_for_none_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles none value of request_timeout parameter"""
        config = CONFIG.copy()
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is None
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)
    
    def test_request_timeout_for_empty_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles empty value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = ""
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is empty string
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)
    
    def test_request_timeout_for_string_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles string value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = "100"
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is string
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=100.0)
    
    def test_request_timeout_for_int_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles int value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = 200
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is int
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=200.0)
        
    def test_request_timeout_for_float_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles float value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = REQUEST_TIMEOUT_FLOAT
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is float
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT_FLOAT)
        
    def test_request_timeout_for_zero_int_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles int 0 value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = 0
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is int 0
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)
  
    def test_request_timeout_for_zero_string_param_value(self, mock_discover, mock_parse_args, mock_request):
        """Test that tap handles string 0 value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = "0"
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is string 0
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)

