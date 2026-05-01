# R/get_charges.R

#' Fetch Dallas Police Arrest Charge Data
#'
#' Retrieves arrest charge data from the Dallas Open Data portal API
#' (SODA endpoint 9u3q-af6p).
#'
#' @description
#' This function queries the Socrata Open Data API (SODA) for Dallas Police
#' Arrest Charges. It allows filtering by arrest date range, charge details
#' (description, severity, class, statute), and NIBRS classifications. It supports
#' retrieving large datasets through automatic pagination. Note: Geographic filters
#' and geographic object conversion are not available for this dataset. The primary
#' date field (`arrestdate`) is stored as text in the source data.
#'
#' If the `where` argument is provided, it overrides all other filter arguments.
#'
#' @param start_date Optional. A character string in 'YYYY-MM-DD' format or a
#'   Date object specifying the minimum arrest date (inclusive, based on `arrestdate` text field).
#' @param end_date Optional. A character string in 'YYYY-MM-DD' format or a
#'   Date object specifying the maximum arrest date (inclusive, based on `arrestdate` text field).
#' @param charge_description Optional. A character vector of charge descriptions
#'   to filter by (maps to `chargedesc` field). Case-sensitive.
#' @param severity Optional. A character vector of charge severities to filter by
#'   (e.g., 'FELONY', 'MISDEMEANOR'), matching the `severity` field.
#' @param penalty_class Optional. A character vector of penalty classes to filter by
#'   (e.g., 'FS' - State Jail Felony, 'MA' - Misdemeanor A, 'MC' - Misdemeanor C),
#'   matching the `pclass` field.
#' @param statute Optional. A character vector of statute codes to filter by, matching
#'   the `statute` field.
#' @param nibrs_group Optional. A character vector of NIBRS Group codes (e.g., 'A', 'B')
#'   to filter charges.
#' @param nibrs_code Optional. A character vector of specific NIBRS offense codes
#'   (e.g., '13A', '90Z') to filter charges.
#' @param nibrs_crime_against Optional. A character vector specifying the NIBRS
#'   'Crime Against' category (e.g., 'PERSON', 'PROPERTY', 'SOCIETY'), matching
#'   the `nibrs_crimeagainst` field.
#' @param limit The maximum number of records to return. Defaults to 1000.
#'   Use `limit = Inf` to attempt retrieving all matching records.
#' @param select A character vector specifying which columns to retrieve.
#'   If NULL (default), all columns are retrieved.
#' @param where An optional character string containing a custom SoQL WHERE clause
#'   (e.g., `"holdtype = 'IMMIGRATION'"`). Overrides other filter arguments if provided.
#' @param ... Additional SODA query parameters passed directly to the API URL,
#'   (e.g., `$order = "arrestdate DESC"`).
#'
#' @return A `tibble` containing the requested arrest charge data.
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
#' # Get 10 most recent arrest charges (ordered by arrestdate text field)
#' recent_charges <- get_charges(limit = 10, `$order` = "arrestdate DESC")
#' print(recent_charges)
#'
#' # Get Felony charges from April 2024
#' felony_apr24 <- get_charges(
#'   start_date = "2024-04-01", end_date = "2024-04-30",
#'   severity = "FELONY",
#'   limit = 200
#' )
#' print(felony_apr24)
#'
#' # Get charges related to a specific statute
#' statute_charges <- get_charges(statute = "49.04", limit = 50) # Example statute
#' print(statute_charges)
#'
#' # Get NIBRS Group B charges with penalty class 'MC' (Misdemeanor C)
#' mc_groupb <- get_charges(nibrs_group = "B", penalty_class = "MC", limit = 100)
#' print(mc_groupb)
#' }
get_charges <- function(start_date = NULL, end_date = NULL,
                        charge_description = NULL,
                        severity = NULL,
                        penalty_class = NULL,
                        statute = NULL,
                        nibrs_group = NULL,
                        nibrs_code = NULL,
                        nibrs_crime_against = NULL,
                        limit = 1000, select = NULL, where = NULL, ...) {

  # --- Input Validation & Dependency Checks ---
  rlang::check_installed("httr", reason = "to fetch data from the API.")
  rlang::check_installed("jsonlite", reason = "to parse JSON data.")
  rlang::check_installed("dplyr", reason = "for data manipulation.")
  if (!is.numeric(limit) || limit <= 0) stop("`limit` must be a positive number or Inf.", call. = FALSE)

  # --- Base URL and Query Parameters ---
  base_url <- "https://www.dallasopendata.com/resource/9u3q-af6p.json"
  ua <- httr::user_agent("tidycops/0.2.0 (https://github.com/Steal-This-Code/tidycops)")
  query_params <- rlang::list2(...)

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
    # -- Date filtering (using 'arrestdate' text field) --
    # NOTE: Filtering assumes text dates are in sortable YYYY-MM-DD format
    if (!is.null(start_date)) {
      tryCatch({
        start_date_fmt <- format(as.Date(start_date), "%Y-%m-%d")
        all_where_clauses <- c(all_where_clauses, paste0("arrestdate >= '", start_date_fmt, "'"))
      }, error = function(e) stop("`start_date` invalid format.", call. = FALSE))
    }
    if (!is.null(end_date)) {
      tryCatch({
        # Use less than the day *after* the end date for inclusivity
        end_date_exclusive_fmt <- format(as.Date(end_date) + 1, "%Y-%m-%d")
        all_where_clauses <- c(all_where_clauses, paste0("arrestdate < '", end_date_exclusive_fmt, "'"))
      }, error = function(e) stop("`end_date` invalid format.", call. = FALSE))
    }

    # -- Charge Filters --
    if (!is.null(charge_description)) { # Field: chargedesc (Text)
      if(!is.character(charge_description)) stop("`charge_description` must be character.", call. = FALSE)
      clause <- sql_in_clause("chargedesc", charge_description)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(severity)) { # Field: severity (Text)
      if(!is.character(severity)) stop("`severity` must be character.", call. = FALSE)
      clause <- sql_in_clause("severity", severity)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(penalty_class)) { # Field: pclass (Text)
      if(!is.character(penalty_class)) stop("`penalty_class` must be character.", call. = FALSE)
      clause <- sql_in_clause("pclass", penalty_class)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(statute)) { # Field: statute (Text)
      if(!is.character(statute)) stop("`statute` must be character.", call. = FALSE)
      clause <- sql_in_clause("statute", statute)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }

    # -- NIBRS Filters --
    if (!is.null(nibrs_group)) { # Field: nibrs_group (Text)
      if(!is.character(nibrs_group)) stop("`nibrs_group` must be character.", call. = FALSE)
      clause <- sql_in_clause("nibrs_group", nibrs_group)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(nibrs_code)) { # Field: nibrs_code (Text)
      if(!is.character(nibrs_code)) stop("`nibrs_code` must be character.", call. = FALSE)
      clause <- sql_in_clause("nibrs_code", nibrs_code)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(nibrs_crime_against)) { # Field: nibrs_crimeagainst (Text)
      if(!is.character(nibrs_crime_against)) stop("`nibrs_crime_against` must be character.", call. = FALSE)
      clause <- sql_in_clause("nibrs_crimeagainst", nibrs_crime_against)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }

    # Geography & GeoConversion Filters Removed for this dataset

    # -- Combine clauses --
    if (length(all_where_clauses) > 0) {
      query_params[["$where"]] <- paste(all_where_clauses, collapse = " AND ")
    }
  } else { # User provided 'where' clause
    if (!is.character(where) || length(where) != 1) stop("`where` must be a single string.", call. = FALSE)
    # Check if other filters were also provided and issue a warning
    other_filters_present <- !is.null(start_date) || !is.null(end_date) ||
      !is.null(charge_description) || !is.null(severity) ||
      !is.null(penalty_class) || !is.null(statute) ||
      !is.null(nibrs_group) || !is.null(nibrs_code) ||
      !is.null(nibrs_crime_against)
    if (other_filters_present) {
      rlang::warn(paste("Using 'where'; ignoring other filter arguments:",
                        "start_date, end_date, charge_description, severity, penalty_class,",
                        "statute, nibrs_group, nibrs_code, nibrs_crime_against."))
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

  if (fetch_limit_this_req > 0) { rlang::inform("Starting data retrieval...") }
  else { rlang::inform("Limit is zero; returning empty dataset."); return(dplyr::tibble()) }

  repeat {
    if (fetch_limit_this_req <= 0) break

    query_params[["$limit"]] <- fetch_limit_this_req
    query_params[["$offset"]] <- current_offset
    request_url <- httr::modify_url(base_url, query = query_params)

    response <- httr::GET(request_url, ua)
    httr::stop_for_status(response, task = paste("fetch data. URL:", request_url)) # Basic status check

    if (httr::http_type(response) != "application/json") stop("API did not return JSON.", call. = FALSE)
    content_text <- httr::content(response, "text", encoding = "UTF-8")

    if(nchar(trimws(content_text)) <= 2 || content_text == "[]") {
      data_chunk <- dplyr::tibble()
    } else {
      tryCatch({
        data_chunk <- jsonlite::fromJSON(content_text, flatten = TRUE)
        data_chunk <- dplyr::as_tibble(data_chunk)
      }, error = function(e) {
        stop(paste("Failed to parse JSON response. Error:", e$message), call. = FALSE)
      })
    }

    chunk_rows <- nrow(data_chunk)
    if (chunk_rows == 0) { rlang::inform("No more data found."); break }

    all_data_chunks[[length(all_data_chunks) + 1]] <- data_chunk
    records_retrieved <- records_retrieved + chunk_rows
    current_offset <- current_offset + chunk_rows

    if (!is.infinite(limit) && records_retrieved >= limit) { rlang::inform(paste("Reached limit of", limit)); break }
    if (chunk_rows < fetch_limit_this_req) { rlang::inform("Retrieved last page."); break }

    fetch_limit_this_req <- if (is.infinite(limit)) { api_max_limit_per_req } else { min(limit - records_retrieved, api_max_limit_per_req) }
  } # End repeat loop

  rlang::inform(paste("Total records retrieved:", records_retrieved))

  # --- Combine and Finalize ---
  if (length(all_data_chunks) == 0) {
    rlang::inform("Query returned no matching records."); return(dplyr::tibble())
  }

  final_data <- dplyr::bind_rows(all_data_chunks)

  if (!is.infinite(limit) && nrow(final_data) > limit) {
    final_data <- final_data[1:limit, , drop = FALSE]
  }

  # Geographic Conversion Removed previously

  rlang::inform("Data retrieval complete.")
  return(final_data) # Returns a tibble
}
