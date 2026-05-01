# tidycops 0.2.0

- Rebranded package identity from `opendpd` to `tidycops`.
- Added a standardized multi-city incident wrapper with documented `std_*`
  schema fields.
- Added city/source registry helpers:
  `list_supported_incident_cities()`, `list_incident_sources()`,
  and `list_standard_incident_fields()`.
- Added ArcGIS incident adapters for Denver, Detroit, Indianapolis, and
  Minneapolis, including source coverage notes for rolling-window feeds.
- Added CKAN incident support and new city adapters for Pittsburgh and
  San Antonio.
- Added a quickstart vignette and expanded README guidance.
- Added unit tests for adapter registry and standardized mapping behavior.
