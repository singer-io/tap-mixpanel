import unittest
from unittest import mock

from tap_mixpanel import client


class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""
    status_code = 200

    def iter_lines(self):
        """Mock generator(list) to return in response."""
        return [
            b'{"event": "Page View", "properties": {"time": 1583044147, "distinct_id": "test_id_1"}}',
            b'{"event": "Page View", "properties": {"time": 1583225657, "distinct_id": "test_id_2"}}'
        ]

    def json(self):
        """JSON formatted response"""
        return {}


class TestClientMethods(unittest.TestCase):
    """
    Test case to validate working of client methods.
    """

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    @mock.patch("tap_mixpanel.client.MixpanelClient.perform_request")
    def test_request_with_url(self, mock_perform_request, mock_check_access):
        """
        Test `request` method for success response with URL and Path.
        """

        mock_check_access.return_value = True
        mock_perform_request.return_value = MockResponse()
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=300,
            user_agent="USER_AGENT"
        )

        response = mock_client.request(method="GET", url="https://sample_url", path="sample_path",
                                       endpoint={})

        # Verify that check_access is called.
        self.assertTrue(mock_check_access.called)

        # Verify that returned response is expected.
        self.assertEqual(response, {})

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    @mock.patch("tap_mixpanel.client.MixpanelClient.perform_request")
    def test_request_without_url(self, mock_perform_request, mock_check_access):
        """
        Test `request` method for success response without URL.
        """

        mock_check_access.return_value = True
        mock_perform_request.return_value = MockResponse()
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=300,
            user_agent="USER_AGENT"
        )

        response = mock_client.request(method="GET", path="sample_path", endpoint={})

        # Verify that check_access is called.
        self.assertTrue(mock_check_access.called)

        # Verify that returned response is expected.
        self.assertEqual(response, {})

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    @mock.patch("tap_mixpanel.client.MixpanelClient.perform_request")
    def test_request_export_with_url(self, mock_perform_request, mock_check_access):
        """
        Test `request_export` with passing base URL for success response.
        """

        mock_perform_request.return_value = MockResponse()
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=300,
            user_agent="USER_AGENT"
        )

        response = list(mock_client.request_export(
            method="POST", url="https://sample_url", path="sample_path", endpoint={}))

        expected_data = [
            {"event": "Page View",
             "properties": {
                 "time": 1583044147,
                 "distinct_id": "test_id_1"}},
            {"event": "Page View",
             "properties": {
                 "time": 1583225657,
                 "distinct_id": "test_id_2"}}
        ]

        # Verify that check_access is called.
        self.assertTrue(mock_check_access.called)

        # Verify that returned response is expected.
        self.assertEqual(list(response), expected_data)

    @mock.patch("tap_mixpanel.client.MixpanelClient.check_access")
    @mock.patch("tap_mixpanel.client.MixpanelClient.perform_request")
    def test_request_export_without_url(self, mock_perform_request, mock_check_access):
        """
        Test `request_export` without passing base URL for success response.
        """

        mock_perform_request.return_value = MockResponse()
        mock_client = client.MixpanelClient(
            api_secret="mock_api_secret",
            api_domain="mock_api_domain",
            request_timeout=300,
            user_agent="USER_AGENT"
        )

        response = list(mock_client.request_export(method="POST", path="sample_path", endpoint={}))

        expected_data = [
            {"event": "Page View",
             "properties": {
                 "time": 1583044147,
                 "distinct_id": "test_id_1"}},
            {"event": "Page View",
             "properties": {
                 "time": 1583225657,
                 "distinct_id": "test_id_2"}}
        ]

        # Verify that check_access method is called.
        self.assertTrue(mock_check_access.called)

        # Verify that returned response is expected.
        self.assertEqual(list(response), expected_data)
