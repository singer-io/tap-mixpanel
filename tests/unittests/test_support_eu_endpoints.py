import unittest
from unittest import mock
from tap_mixpanel.streams import  Revenue, Export
from tap_mixpanel.client import MixpanelClient
from tap_mixpanel.__init__ import main

EU_CONFIG = {
  "api_secret": "dummy_secret",
  "date_window_size": "30",
  "attribution_window": "5",
  "project_timezone": "Europe/Amsterdam",
  "select_properties_by_default": "true",
  "start_date": "2020-02-01T00:00:00Z",
  "user_agent": "tap-mixpanel <api_user_email@your_company.com>",
  "eu_residency": True,
  "end_date": "2020-03-02T00:00:00Z"
}
STANDARD_CONFIG = {
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

class MockStream():
    def __init__(self, stream):
        self.stream = stream

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

class TestMixpanelSupportEuEndpoints(unittest.TestCase):

    @mock.patch("tap_mixpanel.client.MixpanelClient.request")
    @mock.patch("tap_mixpanel.streams.MixPanel.write_bookmark")
    @mock.patch("tap_mixpanel.streams.MixPanel.write_schema")
    def test_support_eu_endpoints_except_export(self, mock_write_schema, mock_write_bookmark, mock_request):
        mock_request.return_value = {}
        mock_write_schema.return_value = ''
        mock_write_bookmark.return_value = ''

        state = {}
        catalog = MockCatalog('revenue')

        client = MixpanelClient('','','')
        revenue_obj = Revenue(client)
        revenue_obj.sync(catalog=catalog, state=state, config=EU_CONFIG, start_date="2020-02-01T00:00:00Z")

        mock_request.assert_called_with(method='GET', url='https://eu.mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')

        revenue_obj = Revenue(client)
        revenue_obj.sync(catalog=catalog,state=state, config=STANDARD_CONFIG, start_date="2020-02-01T00:00:00Z")

        mock_request.assert_called_with(method='GET', url='https://mixpanel.com/api/2.0', path='engage/revenue',
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')


    @mock.patch("tap_mixpanel.client.MixpanelClient.request_export")
    @mock.patch("tap_mixpanel.streams.MixPanel.write_bookmark")
    @mock.patch("tap_mixpanel.streams.MixPanel.write_schema")
    def test_support_export_eu_endpoint(self, mock_write_schema, mock_write_bookmark, mock_request_export):
        mock_request_export.return_value = {}
        mock_write_schema.return_value = ''
        mock_write_bookmark.return_value = ''

        state = {}
        catalog = MockCatalog('export')

        client = MixpanelClient('','','')
        export_obj = Export(client)
        export_obj.sync(catalog=catalog,state=state, config=EU_CONFIG, start_date="2020-02-01T00:00:00Z")

        mock_request_export.assert_called_with(method='GET', url='https://data-eu.mixpanel.com/api/2.0', path='export',
                                        params='from_date=2020-02-01&to_date=2020-03-02', endpoint='export')

        export_obj = Export(client)
        export_obj.sync(catalog=catalog,state=state, config=STANDARD_CONFIG, start_date="2020-02-01T00:00:00Z")

        mock_request_export.assert_called_with(method='GET', url='https://data.mixpanel.com/api/2.0', path='export',
                                        params='from_date=2020-02-01&to_date=2020-03-02', endpoint='export')


    @mock.patch("requests.Session.request")
    @mock.patch("singer.utils.parse_args")
    @mock.patch("tap_mixpanel.__init__.do_discover")
    def test_support_eu_endpoint_in_discover(self, mock_discover, mock_parse_args, mock_request):

        mock_request.return_value = Mockresponse("", status_code=200)
        mock_discover.return_value = ''
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=EU_CONFIG)
        r = main()

        header = {
            'User-Agent': 'tap-mixpanel <api_user_email@your_company.com>',
            'Accept': 'application/json',
            'Authorization': 'Basic ZHVtbXlfc2VjcmV0'
        }
        mock_request.assert_called_with('GET','https://eu.mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=header, timeout=300)

        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=STANDARD_CONFIG)
        r = main()

        mock_request.assert_called_with('GET','https://mixpanel.com/api/2.0/engage', allow_redirects=True,
                                        headers=header, timeout=300)
