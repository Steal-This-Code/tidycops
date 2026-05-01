#' tidycops: Standardized Police Incident Data Access
#'
#' `tidycops` provides a consistent R interface for incident-level police open
#' data across multiple public sources. It includes:
#'
#' - Legacy Dallas-oriented helpers (`get_incidents()`, `get_arrests()`,
#'   `get_charges()`, `get_uof()`, `get_ois()`).
#' - A city-agnostic standardized incident pipeline
#'   (`get_standardized_incidents()`).
#' - Source and schema introspection helpers
#'   (`list_supported_incident_cities()`, `list_incident_sources()`,
#'   `list_standard_incident_fields()`).
#'
#' @keywords internal
"_PACKAGE"
