from collections.abc import Generator
import unittest
from unittest import mock

import requests
import requests_mock
from pytest import raises

from tap_mixpanel import client
from tap_mixpanel.client import (
    MixpanelInternalServiceError,
    ReadTimeoutError,
    Server5xxError,
    Server429Error,
)
from tests.configuration.fixtures import mixpanel_client


class TestMediumClient(unittest.TestCase):
    @mock.patch("time.sleep", return_value=None)
    def test_request_export_backoff_on_timeout(self, mock_sleep, mixpanel_client):
        """
        Test that request_export method of the client backoff max times
        (time.sleep called 'Max-1' times) if timeout error occur.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request(
                "GET",
                "http://test.com",
                exc=requests.exceptions.Timeout("Timeout on request"),
            )

            with raises(ReadTimeoutError):
                for record in mixpanel_client.request_export("GET", url="http://test.com"):
                    pass

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, client.BACKOFF_MAX_TRIES_REQUEST - 1)

    @mock.patch("time.sleep", return_value=None)
    def test_request_export_backoff_on_remote_timeout(self, mock_sleep, mixpanel_client):
        """
        Test that request_export method of the client backoff max times
        (time.sleep called 'Max-1' times) if 504 error occur.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request("GET", "http://test.com", text=None, status_code=504)

            with raises(Server5xxError):
                for _ in mixpanel_client.request_export("GET", url="http://test.com"):
                    pass

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, client.BACKOFF_MAX_TRIES_REQUEST - 1)

    @mock.patch("time.sleep", return_value=None)
    def test_perform_request_backoff_on_remote_timeout_429(self, mock_sleep, mixpanel_client):
        """
        Test that perform_request method of the client backoff max times
        (time.sleep called 'Max-1' times) if 429 error occur.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request(
                "GET", "http://test.com", text=None, content=b"error", status_code=429
            )

            with raises(Server429Error):
                mixpanel_client.perform_request("GET", url="http://test.com")

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, client.BACKOFF_MAX_TRIES_REQUEST - 1)

    @mock.patch("time.sleep", return_value=None)
    def test_perform_request_backoff_on_remote_timeout_500(self, mock_sleep, mixpanel_client):
        """
        Test that perform_request method of the client backoff max times
        (time.sleep called 'Max-1' times) if 500 error occur.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request("GET", "http://test.com", text=None, status_code=500)

            with raises(MixpanelInternalServiceError):
                mixpanel_client.perform_request("GET", url="http://test.com")

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, client.BACKOFF_MAX_TRIES_REQUEST - 1)

    @mock.patch("time.sleep", return_value=None)
    def test_check_access_backoff_on_remote_timeout_429(self, mock_sleep, mixpanel_client):
        """
        Test that check_access method of the client backoff 5 times (time.sleep called 4 times)
        if 429 error occur.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request(
                "GET",
                "https://mixpanel.com/api/2.0/engage",
                content=b"error",
                text=None,
                status_code=429,
            )

            with raises(Server429Error):
                mixpanel_client.check_access()

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, 4)

    @mock.patch("time.sleep", return_value=None)
    def test_check_access_backoff_on_remote_timeout_500(self, mock_sleep, mixpanel_client):
        """
        Test that check_access method of the client backoff 5 times (time.sleep called 4 times)
        if 500 error occur.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request(
                "GET",
                "https://mixpanel.com/api/2.0/engage",
                content=b"error",
                text=None,
                status_code=500,
            )

            with raises(MixpanelInternalServiceError):
                mixpanel_client.check_access()

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, 4)

    @mock.patch("time.sleep", return_value=None)
    def test_request_backoff_on_timeout(self, mock_sleep, mixpanel_client):
        """
        Test that for the `request` method of the client back max times for the timeout.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request(
                "GET",
                "http://test.com",
                exc=requests.exceptions.Timeout("Timeout on request"),
            )

            with raises(ReadTimeoutError):
                mixpanel_client.request("GET", url="http://test.com")

            # Assert backoff retry count as expected
            self.assertEqual(mock_sleep.call_count, client.BACKOFF_MAX_TRIES_REQUEST - 1)

    def test_request_returns_json(self, mixpanel_client):
        """
        Test that request method of the client returns json object.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request("GET", "http://test.com", json={"a": "b"})
            result = mixpanel_client.request("GET", url="http://test.com")

            # Verify that returned object is expected JSON.
            self.assertEqual(result, {"a": "b"})

    def test_request_export_returns_generator(self, mixpanel_client):
        """
        Test that request method of the client returns a generator object.
        """
        with requests_mock.Mocker() as mocker:
            mocker.request("GET", "http://test.com", json={"a": "b"})
            result = mixpanel_client.request_export("GET", url="http://test.com")

            # Verify that returned object is a generator object.
            self.assertEqual(type(result), Generator)
