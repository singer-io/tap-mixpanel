import unittest
from unittest import mock
from tap_mixpanel.streams import  Revenue
from tap_mixpanel.client import MixpanelClient
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

REQUEST_TIMEOUT = 300.0

class MockCatalog():
    def __init__(self, name):
        self.name = name

    def get_selected_streams(self, state):
        return [MockStream(self.name)]

class MockParseArgs():
    def __init__(self, state, discover, config):
        self.state = state
        self.discover = discover
        self.config = config

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

    def json(self):
        return self.text

CLIENT = MixpanelClient('','','')
REVENUE_OBJ = Revenue(CLIENT)
CATALOG = MockCatalog('revenue')
HEADER = {
        'User-Agent': 'tap-mixpanel <api_user_email@your_company.com>',
        'Accept': 'application/json',
        'Authorization': 'Basic ZHVtbXlfc2VjcmV0'
    }

@mock.patch("tap_mixpanel.client.MixpanelClient.request", return_value = {})
@mock.patch("tap_mixpanel.streams.MixPanel.write_bookmark", return_value = '')
@mock.patch("tap_mixpanel.streams.MixPanel.write_schema", return_value = '')
class TestMixpanelRequestTimeoutParameterValueInSync(unittest.TestCase):
    """Test that discover handles different type of value in sync"""
    def test_request_timeout_for_none_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles none value of request_timeout parameter"""
        config = CONFIG.copy()
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" does not passed
        mock_request.assert_called_with(method='GET', request_timeout=REQUEST_TIMEOUT, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

    def test_request_timeout_for_empty_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles empty value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = ""
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" value is empty
        mock_request.assert_called_with(method='GET', request_timeout=REQUEST_TIMEOUT, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

    def test_request_timeout_for_string_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles string value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = "100"
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" value is string
        mock_request.assert_called_with(method='GET', request_timeout=100.0, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

    def test_request_timeout_for_int_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles int value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = 200
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" value is int
        mock_request.assert_called_with(method='GET', request_timeout=200.0, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

    def test_request_timeout_for_float_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles float value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = REQUEST_TIMEOUT
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" value is float
        mock_request.assert_called_with(method='GET', request_timeout=REQUEST_TIMEOUT, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

    def test_request_timeout_for_zero_string_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles "0" value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = "0"
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" value is "0"
        mock_request.assert_called_with(method='GET', request_timeout=REQUEST_TIMEOUT, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

    def test_request_timeout_for_zero_int_param_value_in_sync(self, mock_write_schema, mock_write_bookmark, mock_request):
        """Test that sync handles int 0 value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = 0
        REVENUE_OBJ.sync(catalog=CATALOG, state={}, config=config, start_date="2020-02-01T00:00:00Z")

        # Verify that request called with expected parameter value when "request_timeout" value is 0
        mock_request.assert_called_with(method='GET', request_timeout=REQUEST_TIMEOUT, url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

@mock.patch("requests.Session.request", return_value = Mockresponse("", status_code=200))
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_mixpanel.__init__.do_discover", return_value = '')
class TestMixpanelRequestTimeoutParameterValueInDiscover(unittest.TestCase):
    """Test that discover handles different type of value in discover mode"""
    def test_request_timeout_for_none_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles none value of request_timeout parameter"""
        config = CONFIG.copy()
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is None
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)

    def test_request_timeout_for_empty_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles empty value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = ""
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is empty string
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)

    def test_request_timeout_for_string_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles string value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = "100"
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is string
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=100.0)

    def test_request_timeout_for_int_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles int value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = 200
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is int
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=200.0)
        
    def test_request_timeout_for_float_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles float value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = REQUEST_TIMEOUT
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is float
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)
        
    def test_request_timeout_for_zero_int_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles int 0 value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = 0
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is int 0
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)
        
    def test_request_timeout_for_zero_string_param_value_in_discover(self, mock_discover, mock_parse_args, mock_request):
        """Test that discover handles string 0 value of request_timeout parameter"""
        config = CONFIG.copy()
        config['request_timeout'] = "0"
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        r = main()

        # Verify that request method called with expected parameter value when"request_timeout" is string 0
        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=HEADER, timeout=REQUEST_TIMEOUT)
    
    