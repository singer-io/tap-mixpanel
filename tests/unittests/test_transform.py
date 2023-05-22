import unittest

from parameterized import parameterized
from tap_mixpanel.transform import transform_record


class TestTransformRecord(unittest.TestCase):
    """
    Test case for transforming record.
    """

    test_dict_1 = {
        "$distinct_id": "1234",
        "$properties": {
            "$key1": "value1",
            "$key2": "value2"
        }
    }
    exp_dict_1 = {
        "distinct_id": "1234",
        "mp_reserved_key1": "value1",
        "mp_reserved_key2": "value2"
    }
    test_dict_2 = {
        "time": "1665052647",
        "properties": {
            "$key1": "value1",
            "$key2": "value2",
            "key3": "value3"
        }
    }
    exp_dict_2 = {
        "mp_reserved_key1": "value1",
        "mp_reserved_key2": "value2",
        "key3": "value3",
        "time": "2022-10-06T10:37:27.000000Z"
    }
    test_dict_3 = {
        "key1": "value1",
        "key2": "value2"
    }
    exp_dict_3 = {
        "key1": "value1",
        "key2": "value2",
        "parent_key1": "parent_value1"
    }
    test_dict_4 = {
        "$distinct_id": "1234",
    }
    exp_dict_4 = {
        "cohort_id": "5678",
        "distinct_id": "1234"
    }
    test_dict_5 = {
        "key1": "value1",
        "key2": "value2"
    }

    @parameterized.expand([
        ["engage", test_dict_1, exp_dict_1],
        ["export", test_dict_2, exp_dict_2],
        ["funnels", test_dict_3, exp_dict_3],
        ["cohort_members", test_dict_4, exp_dict_4],
        ["cohort", {**test_dict_5}, test_dict_5],
    ])
    def test_transform_record(self, stream, test_dict_1, expected_dict):
        """
        Test that `transform_record` function transforms records respected by streams.
        """

        parent_record = None
        project_timezone = "UTC"
        if stream == "funnels":
            parent_record = {"parent_key1": "parent_value1"}
        elif stream == "cohort_members":
            parent_record = {"id": "5678"}
        transformed_dict = transform_record(test_dict_1, stream, project_timezone, parent_record)

        # Verify that returned record is expected
        self.assertEqual(transformed_dict, expected_dict)
