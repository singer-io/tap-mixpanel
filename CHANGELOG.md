# Changelog

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
