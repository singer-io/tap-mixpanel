import unittest
from unittest import mock
from parameterized import parameterized
from singer.catalog import Catalog
from tap_mixpanel.discover import discover, _apply_access_checks, _prune_inaccessible_children
from tap_mixpanel.schema import get_schema, get_schemas
from tap_mixpanel.client import MixpanelForbiddenError, MixpanelPaymentRequiredError
from tap_mixpanel.streams import STREAMS

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


class TestCheckAccess(unittest.TestCase):
    """Test check_access method on stream classes."""

    def test_check_access_success(self):
        """Test check_access returns True when the API call succeeds."""
        client = mock.Mock()
        client.request.return_value = {}

        stream_cls = STREAMS["annotations"]
        stream = stream_cls(client=client)
        result = stream.check_access()

        self.assertTrue(result)
        self.assertTrue(client.request.called)

    def test_check_access_forbidden(self):
        """Test check_access returns False when a 403 error is raised."""
        client = mock.Mock()
        client.request.side_effect = MixpanelForbiddenError("403 Forbidden")

        stream_cls = STREAMS["annotations"]
        stream = stream_cls(client=client)
        result = stream.check_access()

        self.assertFalse(result)

    def test_check_access_child_stream_always_true(self):
        """Test that child streams always return True without making a request."""
        client = mock.Mock()

        stream_cls = STREAMS["cohort_members"]
        stream = stream_cls(client=client)
        result = stream.check_access()

        self.assertTrue(result)
        # Should not make any request for child streams
        self.assertFalse(client.request.called)


class TestApplyAccessChecks(unittest.TestCase):
    """Test _apply_access_checks function."""

    @mock.patch("tap_mixpanel.discover.STREAMS")
    def test_all_streams_accessible(self, mock_streams):
        """Test that no streams are removed when all are accessible."""
        mock_stream_cls = mock.Mock()
        mock_stream_cls.parent = None
        mock_stream_cls.return_value.check_access.return_value = True

        mock_streams.items.return_value = [("stream_a", mock_stream_cls)]

        schemas = {"stream_a": {"type": "object"}}
        field_metadata = {"stream_a": []}

        _apply_access_checks(mock.Mock(), schemas, field_metadata)

        self.assertIn("stream_a", schemas)
        self.assertIn("stream_a", field_metadata)

    @mock.patch("tap_mixpanel.discover.STREAMS")
    def test_partial_access(self, mock_streams):
        """Test that inaccessible streams are removed but accessible ones remain."""
        accessible_cls = mock.Mock()
        accessible_cls.parent = None
        accessible_cls.return_value.check_access.return_value = True

        inaccessible_cls = mock.Mock()
        inaccessible_cls.parent = None
        inaccessible_cls.return_value.check_access.return_value = False

        mock_streams.items.return_value = [
            ("stream_a", accessible_cls),
            ("stream_b", inaccessible_cls),
        ]

        schemas = {"stream_a": {"type": "object"}, "stream_b": {"type": "object"}}
        field_metadata = {"stream_a": [], "stream_b": []}

        _apply_access_checks(mock.Mock(), schemas, field_metadata)

        self.assertIn("stream_a", schemas)
        self.assertNotIn("stream_b", schemas)
        self.assertIn("stream_a", field_metadata)
        self.assertNotIn("stream_b", field_metadata)

    @mock.patch("tap_mixpanel.discover.STREAMS")
    def test_all_streams_inaccessible_raises(self, mock_streams):
        """Test that MixpanelForbiddenError is raised when no streams are accessible."""
        inaccessible_cls = mock.Mock()
        inaccessible_cls.parent = None
        inaccessible_cls.return_value.check_access.return_value = False

        mock_streams.items.return_value = [
            ("stream_a", inaccessible_cls),
            ("stream_b", inaccessible_cls),
        ]

        schemas = {"stream_a": {"type": "object"}, "stream_b": {"type": "object"}}
        field_metadata = {"stream_a": [], "stream_b": []}

        with self.assertRaises(MixpanelForbiddenError):
            _apply_access_checks(mock.Mock(), schemas, field_metadata)


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Test _prune_inaccessible_children function."""

    def test_child_removed_when_parent_excluded(self):
        """Test that child stream is removed when its parent is not in schemas."""
        schemas = {"cohort_members": {"type": "object"}}
        field_metadata = {"cohort_members": []}

        _prune_inaccessible_children(schemas, field_metadata)

        # cohort_members has parent="cohorts" which is not in schemas
        self.assertNotIn("cohort_members", schemas)
        self.assertNotIn("cohort_members", field_metadata)

    def test_child_kept_when_parent_present(self):
        """Test that child stream is kept when its parent is in schemas."""
        schemas = {"cohorts": {"type": "object"}, "cohort_members": {"type": "object"}}
        field_metadata = {"cohorts": [], "cohort_members": []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn("cohort_members", schemas)
        self.assertIn("cohort_members", field_metadata)


class TestDiscoverWithAccessChecks(unittest.TestCase):
    """Integration test for discover with access checks."""

    @mock.patch("tap_mixpanel.discover._apply_access_checks")
    @mock.patch("tap_mixpanel.discover.get_schemas")
    def test_discover_calls_access_checks(self, mock_get_schemas, mock_access_checks):
        """Test that discover calls _apply_access_checks."""
        mock_get_schemas.return_value = (
            {"annotations": {"type": "object", "properties": {}}},
            {"annotations": []},
        )
        client = mock.Mock()

        catalog = discover(client=client, properties_flag=False)

        mock_access_checks.assert_called_once()
        self.assertIsInstance(catalog, Catalog)

    @mock.patch("tap_mixpanel.schema.get_schema")
    def test_discover_excludes_forbidden_streams(self, mock_schema):
        """Test that discover excludes streams that return 403."""
        mock_schema.return_value = {"type": "object", "properties": {}}

        client = mock.Mock()
        # Make the client.request raise MixpanelForbiddenError for annotations only
        def side_effect(*args, **kwargs):
            endpoint = kwargs.get("endpoint", "")
            if endpoint == "annotations":
                raise MixpanelForbiddenError("403 Forbidden")
            return {}

        client.request.side_effect = side_effect
        client.disable_engage_endpoint = False

        catalog = discover(client=client, properties_flag=False)
        stream_names = [stream.stream for stream in catalog.streams]

        self.assertNotIn("annotations", stream_names)
        # Other non-child streams should remain
        self.assertIn("export", stream_names)
