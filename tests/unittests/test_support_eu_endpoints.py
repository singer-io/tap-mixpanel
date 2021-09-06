import unittest
from unittest import mock
from tap_mixpanel.streams import  Revenue, Export
from tap_mixpanel.client import MixpanelClient

CONFIG = {
  "api_secret": "dummy_secret",
  "date_window_size": "30",
  "attribution_window": "5",
  "project_timezone": "Europe/Amsterdam",
  "select_properties_by_default": "true",
  "start_date": "2020-02-01T00:00:00Z",
  "user_agent": "tap-mixpanel <api_user_email@your_company.com>",
  "eu_residency_server": True,
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
        revenue_obj.sync(catalog=catalog,state=state,
                         config={"eu_residency_server": True, "start_date": "2020-02-01T00:00:00Z", "end_date": "2020-03-02T00:00:00Z"})
        
        mock_request.assert_called_with(method='GET', url='https://eu.mixpanel.com/api/2.0', path='engage/revenue', 
                                        params='unit=day&from_date=2020-02-01&to_date=2020-03-02', endpoint='revenue')
        
        revenue_obj = Revenue(client)
        revenue_obj.sync(catalog=catalog,state=state,
                         config={"eu_residency_server": False, "start_date": "2020-02-01T00:00:00Z", "end_date": "2020-03-02T00:00:00Z"})
        
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
        export_obj.sync(catalog=catalog,state=state,
                         config={"eu_residency_server": True, "start_date": "2020-02-01T00:00:00Z", "end_date": "2020-03-02T00:00:00Z"})
        
        mock_request_export.assert_called_with(method='GET', url='https://data-eu.mixpanel.com/api/2.0', path='export', 
                                        params='from_date=2020-02-01&to_date=2020-03-02', endpoint='export')
        
        export_obj = Export(client)
        export_obj.sync(catalog=catalog,state=state,
                         config={"eu_residency_server": False, "start_date": "2020-02-01T00:00:00Z", "end_date": "2020-03-02T00:00:00Z"})
        
        mock_request_export.assert_called_with(method='GET', url='https://data.mixpanel.com/api/2.0', path='export', 
                                        params='from_date=2020-02-01&to_date=2020-03-02', endpoint='export')