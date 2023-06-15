import unittest
from unittest import mock
from parameterized import parameterized
from singer.catalog import Catalog
from tap_mixpanel.discover import discover
from tap_mixpanel.schema import get_schema, get_schemas
from tap_mixpanel.client import MixpanelPaymentRequiredError

@mock.patch("tap_mixpanel.schema.get_schema")
class TestDiscover(unittest.TestCase):
    """Test `discover` function."""

    def test_discover(self, mock_schema):
        """
        Test discover function generates a catalog object.
        """
        mock_schema.return_value = dict()
        client = mock.Mock()
        return_catalog = discover(client=client, properties_flag=False)

        # Verify discover function returns `Catalog` type object.
        self.assertIsInstance(return_catalog, Catalog)


class TestGetSchema(unittest.TestCase):
    """
    Test get_schema returns schema for dynamic and standard schemas.
    """

    engage_schema_response = {
        "status": "ok",
        "results": {
            "$last_seen": {"type": "datetime"},
            "country": {"type": "string"}
        }
    }

    export_schema_response = {
        "$last_seen": {},
        "country": {}
    }

    @parameterized.expand([
        ["engage", "engage/properties", engage_schema_response],
        ["export", "events/properties/top", export_schema_response],
    ])
    def test_dynamic_schema(self, stream_name, stream_path, schema_response):
        """
        Test that streams with dynamic schema function makes http call
        and writes fields as per the response.
        """
        client = mock.Mock()
        client.request.return_value = schema_response
        schema = get_schema(client=client, properties_flag=True, stream_name=stream_name)

        # Verify that request method is called with the path to get stream schema
        self.assertTrue(client.request.called)
        _, args = client.request.call_args
        self.assertEqual(stream_path, args["path"])

        # Verify that field without '$' is written as it is
        self.assertIn("country", schema["properties"])

        # Verify that field with '$' is written with 'mp_reserved_'
        self.assertIn("mp_reserved_last_seen", schema["properties"])

    def test_other_schema(self):
        """
        Test for standard stream function is not making http call to get schema.
        """
        client = mock.Mock()
        client.request.return_value = mock.Mock()
        schema = get_schema(client=client, properties_flag=True, stream_name="revenue")

        # Verify that request method is not called
        self.assertFalse(client.request.called)

        # Verify that schema type is dictionary
        self.assertEqual(type(schema), dict)

    @mock.patch("tap_mixpanel.schema.LOGGER.warning")
    def test_402_handling(self, mock_logger):
        """
        Test that while generating dynamic schema if 402 error occurs,
        tap does not throw the error and instead prints an warning logger.
        """
        client = mock.Mock()
        client.request.side_effect = MixpanelPaymentRequiredError

        schemas, field_metadata = get_schemas(client, True)

        # Verify that expected warning logger is called
        mock_logger.assert_any_call(
            "Mixpanel returned a 402 indicating the Engage endpoint and stream is unavailable. Skipping."
        )
        mock_logger.assert_any_call(
            "Mixpanel returned a 402 from the %s API so %s stream will be skipped.",
                "export",
                "export",
        )

        # Verify that dynamic schema stream is not written in catalog.
        self.assertNotIn("export", schemas)
        self.assertNotIn("engage", schemas)
