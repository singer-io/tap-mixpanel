# Changelog

## 1.5.0
  * Adds `export_events` as optional param to filter the data for export stream based on event names [#56](https://github.com/singer-io/tap-mixpanel/pull/56)

## 1.4.3
  * Code Refactoring [#55](https://github.com/singer-io/tap-mixpanel/pull/55) 
    * Reduced record duplication.
    * Optimized parent-child stream, by making common parent API calls.
    * Handled 402 error (Payment require) in discover mode for a free plan Mixpanel account.

## 1.4.2
  * Adds ProtocolError to backoff handling [#58](https://github.com/singer-io/tap-mixpanel/pull/58)

## 1.4.1
  * Implement Request Timeout

## 1.4.0
  * Add a proper error message for timezone issue [#35](https://github.com/singer-io/tap-mixpanel/pull/35)
  * Switch from using multipleof to singer.decimal [#38]( https://github.com/singer-io/tap-mixpanel/pull/38)
  * Fix date time conversion [#40](https://github.com/singer-io/tap-mixpanel/pull/40)

## 1.3.0
  * Add support for EU residency mixpanel data via the `eu_residency` config parameter, which when `'true'` will have the tap extract data from the EU residency endpoint [#39](https://github.com/singer-io/tap-mixpanel/pull/39)

## 1.2.2
  * Add a timeout on requests [#20](https://github.com/singer-io/tap-mixpanel/pull/20)

## 1.2.1
  * Fix the query params for the `engage` and `cohort_members` streams to fetch all records [#24](https://github.com/singer-io/tap-mixpanel/pull/24)

## 1.2.0
  * Make the `engage` stream's schema be an `anyOf` the possible types [#18](https://github.com/singer-io/tap-mixpanel/pull/18)

## 1.1.1
  * Remove page_size param to shorten processing time [#15](https://github.com/singer-io/tap-mixpanel/pull/15)

## 1.1.0
  * Make `date_window_size` not a required config field. Defaults to 30 [#13](https://github.com/singer-io/tap-mixpanel/pull/13)

## 1.0.2
  * Allows the tap to proceed when the Engage endpoint is unavailable due to a HTTP 402 payment required [#12](https://github.com/singer-io/tap-mixpanel/pull/12)

## 1.0.1
  * Bumping patch version to avoid conflict with old pypi uploaded version 1.0.0 in May 2017

## 1.0.0
  * Major version bump intended to release changes from 0.0.6
  * Plus [PR #6 to change export stream to be append-only with no primary key](https://github.com/singer-io/tap-mixpanel/pull/6)

## 0.0.6
  * Adjust backoff and timeout handling

## 0.0.5
  * Fix a memory-leak for the export stream [#3](https://github.com/singer-io/tap-mixpanel/pull/3)

## 0.0.4
  * Change `sync.py` date windowing start/end dates to provide dates in local `project_timezone` to eliminate error of requesting data for future dates.

## 0.0.3
  * Adjust `client.py` to add better error handling for read timeouts.

## 0.0.2
  * Change `export` to streaming. Required changes to client, sync, and transform.

## 0.0.1
  * Initial commit
