# R/get_uof.R

#' Fetch Dallas Police Use of Force / Response to Resistance Data (2017-2020)
#'
#' Retrieves Use of Force (UOF) / Response to Resistance data for a specific year
#' (currently 2017-2020) from the Dallas Open Data portal API.
#'
#' @description
#' This function queries the Socrata Open Data API (SODA) for Dallas Police
#' UOF data for a single specified year. Currently, only data for years 2017 through
#' 2020 is supported by this function. Data is split by year on the portal,
#' and column names/types may vary between years within this range.
#'
#' Filtering by common fields like force type, reason, and service type is supported.
#' Due to inconsistencies across yearly datasets, detailed geographic filters and
#' automatic geographic object conversion are not available in this function.
#' Use the `where` argument for more specific filtering needs.
#'
#' If the `where` argument is provided, it overrides all other filter arguments.
#'
#' @param year Numeric. The specific year for which to retrieve data. Must be
#'   between 2017 and 2020 (inclusive).
#' @param start_date Optional. A character string in 'YYYY-MM-DD' format or a
#'   Date object specifying the minimum incident date (inclusive). Filters on
#'   `occurred_d` (for years 2017-2019) or `occurred_dt` (for year 2020).
#' @param end_date Optional. A character string in 'YYYY-MM-DD' format or a
#'   Date object specifying the maximum incident date (inclusive). Filters on
#'   `occurred_d` or `occurred_dt` based on the year.
#' @param force_type Optional. A character vector of force types to filter by
#'   (e.g., 'Pointing Firearm', 'Taser Display', 'Physical Force'), matching the `forcetype` field.
#' @param reason Optional. A character vector of UOF reasons to filter by
#'   (e.g., 'Resisting Arrest', 'Prevent Escape'), matching the `uof_reason` field.
#' @param service_type Optional. A character vector of service types to filter by
#'   (e.g., 'Warrant Service', 'Disturbance Call'). Filters on `service_ty` (2017-2019)
#'   or `service_type` (2020).
#' @param limit The maximum number of records to return for the specified year.
#'   Defaults to 1000. Use `limit = Inf` to attempt retrieving all records for that year.
#' @param select A character vector specifying which columns to retrieve. Column names
#'   vary by year - check the portal for the specific year's schema. If NULL (default),
#'   all columns for that year are retrieved.
#' @param where An optional character string containing a custom SoQL WHERE clause
#'   (e.g., `"offrace = 'WHITE'"` or `"beat = '114'"` if applicable for that year).
#'   Overrides other filter arguments if provided.
#' @param ... Additional SODA query parameters passed directly to the API URL,
#'   (e.g., `$order = "occurred_dt DESC"` for 2020 or `$order = "occurred_d DESC"` for 2017-2019).
#'
#' @return A `tibble` containing the requested UOF data for the specified year (2017-2020).
#'   **Note:** Column names and data types may differ depending on the `year` requested.
#' @export
#'
#' @importFrom httr GET http_type content stop_for_status modify_url user_agent
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr bind_rows as_tibble tibble filter select mutate all_of arrange relocate last_col
#' @importFrom rlang check_installed is_installed inform .data warn abort list2 := sym
#' @importFrom utils URLencode
#'
#' @examples
#' \dontrun{
#' # Get first 50 UOF records for 2020, order descending by date
#' # Date field is occurred_dt for 2020
#' uof_2020 <- get_uof(year = 2020, limit = 50, `$order` = "occurred_dt DESC")
#' print(names(uof_2020))
#'
#' # Get UOF records for 2019 where Taser Cartridge was used
#' taser_2019 <- get_uof(year = 2019, force_type = "Taser Cartridge")
#' print(taser_2019)
#'
#' # Get UOF records for 2018 related to 'Resisting Arrest' reason
#' resist_2018 <- get_uof(year = 2018, reason = "Resisting Arrest", limit = 100)
#' print(resist_2018)
#'
#' # Get specific columns for 2017 UOF data
#' # Date field is occurred_d for 2017
#' uof_2017_select <- get_uof(
#'   year = 2017,
#'   select = c("uofnum", "occurred_d", "forcetype", "uof_reason"),
#'   limit = 20
#' )
#' print(uof_2017_select)
#'
#' # Example of trying an unsupported year (will produce an error)
#' # try(get_uof(year = 2021))
#' }
get_uof <- function(year, start_date = NULL, end_date = NULL,
                    force_type = NULL,
                    reason = NULL,
                    service_type = NULL,
                    limit = 1000, select = NULL, where = NULL, ...) {

  # --- Input Validation & Dependency Checks ---
  rlang::check_installed("httr", reason = "to fetch data from the API.")
  rlang::check_installed("jsonlite", reason = "to parse JSON data.")
  rlang::check_installed("dplyr", reason = "for data manipulation.")
  if (!is.numeric(year) || length(year) != 1 || year %% 1 != 0) {
    stop("`year` must be a single integer (e.g., 2020).", call. = FALSE)
  }
  if (!is.numeric(limit) || limit <= 0) stop("`limit` must be a positive number or Inf.", call. = FALSE)

  # --- Year to Resource ID Mapping (Restricted to 2017-2020) ---
  # This map may need updating if Dallas PD releases/changes datasets.
  year_resource_map <- list(
    "2020" = "nufk-2iqn",
    "2019" = "46zb-7qgj",
    "2018" = "33un-ry4j",
    "2017" = "tsu5-ca6k"
  )

  year_char <- as.character(year)
  # Validate requested year against supported map
  if (!year_char %in% names(year_resource_map)) {
    stop("Invalid `year`. Currently, only years 2017 through 2020 are supported.", call. = FALSE)
  }
  resource_id <- year_resource_map[[year_char]]

  # --- Base URL and Query Parameters ---
  base_url <- paste0("https://www.dallasopendata.com/resource/", resource_id, ".json")
  ua <- httr::user_agent("tidycops/0.2.0 (https://github.com/Steal-This-Code/tidycops)")
  query_params <- rlang::list2(...)

  # --- Conditional Field Names ---
  # Determine correct API field names based on year (approximated from known schemas)
  if (year >= 2020) { # Logic for 2020 (and potentially later years if map is updated)
    date_field <- "occurred_dt"        # Check API field name if using >2020
    service_type_field <- "service_type" # Check API field name if using >2020
  } else { # Logic for 2017, 2018, 2019
    date_field <- "occurred_d"
    service_type_field <- "service_ty"
  }
  # Assuming 'forcetype' and 'uof_reason' names are consistent based on provided lists

  # --- Filtering Logic ---
  all_where_clauses <- list()

  # Helper function for creating IN clauses (handles numeric types without quotes)
  # Consider defining this once internally (e.g., in utils.R)
  sql_in_clause <- function(field, values) {
    if (is.null(values) || length(values) == 0) return(NULL)
    quoted_values <- sapply(values, function(v) {
      if (is.character(v)) {
        v_escaped <- gsub("'", "''", v); paste0("'", v_escaped, "'")
      } else if (is.numeric(v) || is.logical(v)) {
        as.character(v)
      } else {
        v_escaped <- gsub("'", "''", as.character(v)); paste0("'", v_escaped, "'")
      }
    })
    paste0(field, " IN (", paste(quoted_values, collapse = ", "), ")")
  }

  # Build WHERE clause if 'where' argument is not provided
  if (is.null(where)) {
    # -- Date filtering (using conditional date_field) --
    if (!is.null(start_date)) {
      tryCatch({
        # Format with time component, as source fields are often Timestamps
        start_date_fmt <- format(as.Date(start_date), "%Y-%m-%dT00:00:00")
        all_where_clauses <- c(all_where_clauses, paste0(date_field, " >= '", start_date_fmt, "'"))
      }, error = function(e) stop("`start_date` invalid format.", call. = FALSE))
    }
    if (!is.null(end_date)) {
      tryCatch({
        # Use less than the start of the day *after* the end date for inclusivity
        end_date_exclusive_fmt <- format(as.Date(end_date) + 1, "%Y-%m-%dT00:00:00")
        all_where_clauses <- c(all_where_clauses, paste0(date_field, " < '", end_date_exclusive_fmt, "'"))
      }, error = function(e) stop("`end_date` invalid format.", call. = FALSE))
    }

    # -- UOF Specific Filters --
    if (!is.null(force_type)) { # Field: forcetype (Text)
      if(!is.character(force_type)) stop("`force_type` must be character.", call. = FALSE)
      clause <- sql_in_clause("forcetype", force_type) # Assumes API uses lowercase 'forcetype'
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(reason)) { # Field: uof_reason (Text)
      if(!is.character(reason)) stop("`reason` must be character.", call. = FALSE)
      clause <- sql_in_clause("uof_reason", reason) # Assumes API uses lowercase 'uof_reason'
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(service_type)) { # Field: service_ty or service_type (Text)
      if(!is.character(service_type)) stop("`service_type` must be character.", call. = FALSE)
      # Use the conditional field name determined earlier
      clause <- sql_in_clause(service_type_field, service_type)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }

    # -- Combine clauses --
    if (length(all_where_clauses) > 0) {
      query_params[["$where"]] <- paste(all_where_clauses, collapse = " AND ")
    }
  } else { # User provided 'where' clause
    if (!is.character(where) || length(where) != 1) stop("`where` must be a single string.", call. = FALSE)
    # Check if other filters were also provided and issue a warning
    other_filters_present <- !is.null(start_date) || !is.null(end_date) ||
      !is.null(force_type) || !is.null(reason) ||
      !is.null(service_type)
    if (other_filters_present) {
      rlang::warn("Using 'where'; ignoring other filter arguments: start_date, end_date, force_type, reason, service_type.")
    }
    query_params[["$where"]] <- where
  }

  # --- Column Selection Logic ---
  if (!is.null(select)) {
    if (!is.character(select)) stop("`select` must be a character vector.", call. = FALSE)
    query_params[["$select"]] <- paste(unique(select), collapse = ",")
  }

  # --- Data Retrieval with Pagination ---
  all_data_chunks <- list()
  current_offset <- 0
  records_retrieved <- 0
  api_max_limit_per_req <- 1000
  fetch_limit_this_req <- if (is.infinite(limit)) { api_max_limit_per_req } else { min(limit - records_retrieved, api_max_limit_per_req) }

  if (fetch_limit_this_req > 0) { rlang::inform(paste0("Starting data retrieval for year ", year, "...")) }
  else { rlang::inform("Limit is zero; returning empty dataset."); return(dplyr::tibble()) }

  repeat {
    if (fetch_limit_this_req <= 0) break

    query_params[["$limit"]] <- fetch_limit_this_req
    query_params[["$offset"]] <- current_offset
    request_url <- httr::modify_url(base_url, query = query_params)

    response <- httr::GET(request_url, ua)
    httr::stop_for_status(response, task = paste("fetch data for year", year, ". URL:", request_url))

    if (httr::http_type(response) != "application/json") stop("API did not return JSON.", call. = FALSE)
    content_text <- httr::content(response, "text", encoding = "UTF-8")

    if(nchar(trimws(content_text)) <= 2 || content_text == "[]") {
      data_chunk <- dplyr::tibble()
    } else {
      tryCatch({
        data_chunk <- jsonlite::fromJSON(content_text, flatten = TRUE)
        data_chunk <- dplyr::as_tibble(data_chunk)
      }, error = function(e) {
        stop(paste0("Failed to parse JSON for year ", year,". Error: ", e$message), call. = FALSE)
      })
    }

    chunk_rows <- nrow(data_chunk)
    if (chunk_rows == 0) { rlang::inform(paste0("No more data found for year ", year, ".")); break }

    all_data_chunks[[length(all_data_chunks) + 1]] <- data_chunk
    records_retrieved <- records_retrieved + chunk_rows
    current_offset <- current_offset + chunk_rows

    if (!is.infinite(limit) && records_retrieved >= limit) { rlang::inform(paste("Reached limit of", limit)); break }
    if (chunk_rows < fetch_limit_this_req) { rlang::inform(paste0("Retrieved last page for year ", year, ".")); break }

    fetch_limit_this_req <- if (is.infinite(limit)) { api_max_limit_per_req } else { min(limit - records_retrieved, api_max_limit_per_req) }
  } # End repeat loop

  rlang::inform(paste("Total records retrieved for year", year, ":", records_retrieved))

  # --- Combine and Finalize ---
  if (length(all_data_chunks) == 0) {
    rlang::inform(paste0("Query returned no matching records for year ", year, "."))
    return(dplyr::tibble())
  }
  final_data <- dplyr::bind_rows(all_data_chunks)
  if (!is.infinite(limit) && nrow(final_data) > limit) {
    final_data <- final_data[1:limit, , drop = FALSE]
  }

  # Geographic Conversion Removed previously

  rlang::inform(paste0("Data retrieval for year ", year, " complete."))
  # Returns a tibble, schema may vary by year
  return(final_data)
}
