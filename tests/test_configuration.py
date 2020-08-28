config = {
    "test_name": "tap_mixpanel_combined_test",
    "tap_name": "tap-mixpanel",
    "type": "platform.mixpanel",
    "properties": {
        "date_window_size": "TAP_MIXPANEL_DATE_WINDOW_SIZE",
        "attribution_window": "TAP_MIXPANEL_ATTRIBUTION_WINDOW",
        "project_timezone": "TAP_MIXPANEL_PROJECT_TIMEZONE",
        "select_properties_by_default": "TAP_MIXPANEL_SELECT_PROPERTIES_BY_DEFAULT"
    },
    "credentials": {
        "api_secret": "TAP_MIXPANEL_API_SECRET"
    },
    "streams" : {
        "annotations": {"date"},
        "cohort_members": {"cohort_id", "distinct_id"},
        "cohorts": {"id"},
        "engage": {"distinct_id"},
        "export": {"event", "time", "distinct_id"},
        "revenue": {"date"}
    },
    "exclude_streams": [
        "annotations",
        "cohort_members",
        "cohorts",
        "engage",
        "funnels"
    ]
}
