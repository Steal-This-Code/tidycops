# R/get_ois.R

#' Fetch Dallas Police Officer Involved Shooting (OIS) Data
#'
#' Retrieves OIS data from the Dallas Open Data portal API
#' (SODA endpoint 4gmt-jyx2).
#'
#' @description
#' This function queries the Socrata Open Data API (SODA) for Dallas Police
#' Officer Involved Shootings. It allows filtering by date range, outcome,
#' suspect weapon, and grand jury disposition. It supports retrieving
#' large datasets through automatic pagination. Note that detailed geographic
#' filters (zip, beat, etc.) and geographic object conversion are not available
#' for this specific dataset via this function.
#'
#' If the `where` argument is provided, it overrides all other filter arguments.
#'
#' @param start_date Optional. A character string in 'YYYY-MM-DD' format or a
#'   Date object specifying the minimum incident date (inclusive, based on `date` field).
#' @param end_date Optional. A character string in 'YYYY-MM-DD' format or a
#'   Date object specifying the maximum incident date (inclusive, based on `date` field).
#' @param outcome Optional. A character vector of outcomes to filter by
#'   (e.g., 'DECEASED', 'INJURED', 'SHOOT AND MISS'), matching the
#'   `suspect_deceased_injured_or_shoot_and_miss` field.
#' @param suspect_weapon Optional. A character vector of suspect weapon types
#'   to filter by (e.g., 'HANDGUN', 'RIFLE', 'KNIFE'), matching the `suspect_weapon` field.
#' @param disposition Optional. A character vector of Grand Jury dispositions
#'   to filter by (e.g., 'NO BILL', 'TRUE BILL'), matching the `grand_jury_disposition` field.
#' @param limit The maximum number of records to return. Defaults to 1000.
#'   Use `limit = Inf` to attempt retrieving all matching records.
#' @param select A character vector specifying which columns to retrieve.
#'   If NULL (default), all available columns are retrieved.
#' @param where An optional character string containing a custom SoQL WHERE clause
#'   (e.g., `"case = '012345-2023'"`). Overrides other filter arguments if provided.
#' @param ... Additional SODA query parameters passed directly to the API URL,
#'   (e.g., `$order = "date DESC"`).
#'
#' @return A `tibble` containing the requested OIS data.
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
#' # Get 10 most recent OIS incidents
#' recent_ois <- get_ois(limit = 10, `$order` = "date DESC")
#' print(recent_ois)
#'
#' # Get OIS incidents from 2023 where outcome was 'DECEASED' (replace with valid year)
#' # deceased_ois <- get_ois(
#' #   start_date = "2023-01-01", end_date = "2023-12-31",
#' #   outcome = "DECEASED"
#' # )
#' # print(deceased_ois)
#'
#' # Get OIS incidents where suspect weapon was reported as 'HANDGUN' or 'RIFLE'
#' firearm_ois <- get_ois(suspect_weapon = c("HANDGUN", "RIFLE"), limit = 50)
#' print(firearm_ois)
#'
#' # Use 'where' clause for case number lookup
#' # specific_case <- get_ois(where = "case = '012345-2023'") # Use actual case number
#' # print(specific_case)
#' }
get_ois <- function(start_date = NULL, end_date = NULL,
                    outcome = NULL,
                    suspect_weapon = NULL,
                    disposition = NULL,
                    limit = 1000, select = NULL, where = NULL, ...) {

  # --- Input Validation & Dependency Checks ---
  rlang::check_installed("httr", reason = "to fetch data from the API.")
  rlang::check_installed("jsonlite", reason = "to parse JSON data.")
  rlang::check_installed("dplyr", reason = "for data manipulation.")
  if (!is.numeric(limit) || limit <= 0) stop("`limit` must be a positive number or Inf.", call. = FALSE)

  # --- Base URL and Query Parameters ---
  base_url <- "https://www.dallasopendata.com/resource/4gmt-jyx2.json"
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
    # -- Date filtering (using 'date' field) --
    if (!is.null(start_date)) {
      tryCatch({
        start_date_fmt <- format(as.Date(start_date), "%Y-%m-%dT00:00:00")
        all_where_clauses <- c(all_where_clauses, paste0("date >= '", start_date_fmt, "'"))
      }, error = function(e) stop("`start_date` invalid format.", call. = FALSE))
    }
    if (!is.null(end_date)) {
      tryCatch({
        end_date_exclusive_fmt <- format(as.Date(end_date) + 1, "%Y-%m-%dT00:00:00")
        all_where_clauses <- c(all_where_clauses, paste0("date < '", end_date_exclusive_fmt, "'"))
      }, error = function(e) stop("`end_date` invalid format.", call. = FALSE))
    }

    # -- OIS Specific Filters --
    if (!is.null(outcome)) { # Field: suspect_deceased_injured_or_shoot_and_miss (Text)
      if(!is.character(outcome)) stop("`outcome` must be character.", call. = FALSE)
      clause <- sql_in_clause("suspect_deceased_injured_or_shoot_and_miss", outcome)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(suspect_weapon)) { # Field: suspect_weapon (Text)
      if(!is.character(suspect_weapon)) stop("`suspect_weapon` must be character.", call. = FALSE)
      clause <- sql_in_clause("suspect_weapon", suspect_weapon)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }
    if (!is.null(disposition)) { # Field: grand_jury_disposition (Text)
      if(!is.character(disposition)) stop("`disposition` must be character.", call. = FALSE)
      clause <- sql_in_clause("grand_jury_disposition", disposition)
      if (!is.null(clause)) all_where_clauses <- c(all_where_clauses, clause)
    }

    # Geography Filters Removed previously

    # -- Combine clauses --
    if (length(all_where_clauses) > 0) {
      query_params[["$where"]] <- paste(all_where_clauses, collapse = " AND ")
    }
  } else { # User provided 'where' clause
    if (!is.character(where) || length(where) != 1) stop("`where` must be a single string.", call. = FALSE)
    # Check if other filters were also provided and issue a warning
    other_filters_present <- !is.null(start_date) || !is.null(end_date) ||
      !is.null(outcome) || !is.null(suspect_weapon) ||
      !is.null(disposition)
    if (other_filters_present) {
      rlang::warn("Using 'where'; ignoring other filter arguments: start_date, end_date, outcome, suspect_weapon, disposition.")
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
    httr::stop_for_status(response, task = paste("fetch data. URL:", request_url))

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
