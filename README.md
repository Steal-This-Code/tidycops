<!-- README.md is generated from README.Rmd. Please edit that file -->

# tidycops

<!-- badges: start -->
<!-- badges: end -->

`tidycops` is an incident-focused R package for pulling public police
data across cities with one interface.

It supports two core modes:

- Comparable cross-city output (`std_*` schema)
- Full city-native output (all exposed source fields)

## Currently Supported Cities

As of April 30, 2026, incident adapters are available for:

- Boston
- Chicago
- Cincinnati
- Cleveland
- Dallas
- Denver
- Detroit
- Fort Lauderdale
- Gainesville
- Grand Rapids
- Hartford
- Houston
- Indianapolis
- Kansas City
- Minneapolis
- Naperville
- New Orleans
- New York City
- Pittsburgh
- Providence
- Rochester
- San Antonio
- San Francisco
- Seattle
- Washington, DC

Run `list_supported_incident_cities()` for the latest in-package list.

## Installation

You can install the development version of tidycops like so:

``` r
# install.packages("remotes")
remotes::install_github("Steal-This-Code/tidycops")
```

## Incident Workflow

``` r
library(dplyr)

# Which cities are wired?
list_supported_incident_cities()

# Which fields are most comparable across cities?
list_common_incident_fields(min_cities = 4)
```

### 1) Comparable Cross-City Output

Use this when you want harmonized fields for city-to-city analysis.

``` r
chicago_compare <- get_incidents(
  city = "chicago",
  view = "comparable",
  start_date = "2026-04-01",
  end_date = "2026-04-07",
  neighborhood = "Mission",
  as_sf = TRUE,
  limit = 500
)
```

### 2) Full City Schema Output

Use this when you want all city-native fields plus adapter metadata.

``` r
cincy_full <- get_incidents(
  city = "cincy",
  view = "city_full",
  start_date = "2026-04-01",
  end_date = "2026-04-07",
  limit = 1000
)
```

### 3) Raw Payload Output

Use this when you want untouched source payload rows only.

``` r
cincy_raw <- get_incidents(
  city = "cincy",
  view = "city_raw",
  start_date = "2026-04-01",
  end_date = "2026-04-07",
  limit = 1000
)

# Equivalent convenience helper:
cincy_raw_2 <- download_city_incidents_raw(
  city = "cincy",
  start_date = "2026-04-01",
  end_date = "2026-04-07",
  limit = 1000
)
```

### Date Shortcut

Use `last_n_days` when you want a trailing window without manual date
math.

``` r
recent_cincy <- get_incidents(
  city = "cincinnati",
  view = "comparable",
  last_n_days = 14,
  limit = 1000
)
```

## Source Freshness and Bounded Feeds

Some adapters are rolling windows, capped historical feeds, or
scope-limited (for example calls-for-service or Part I only). Check
source metadata before analysis.

``` r
list_incident_sources() |>
  dplyr::filter(source_status %in% c("historical_capped", "rolling_window") | scope_status != "all_incidents")
```

## Build Priorities: Incident Function Shortcuts

Before expanding to arrests/UOF, these are the highest-value additions
for incident workflows:

1. `dedupe` controls
   - Add optional dedupe modes (`none`, `incident_id`, `incident_number`) for
     city feeds that publish multiple offense rows per incident.
2. `fields = "core"` projection
   - Add a quick column subset option for the most cross-city stable fields.
3. Source diagnostics helper
   - Add a helper to report expected coverage, null rates, and key field
     completeness by city/date window.

These keep the package incident-first while making pull/analyze loops
faster.
