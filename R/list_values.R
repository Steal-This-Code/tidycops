# R/list_values.R

#' List Distinct Values for a Field from a Specific Dataset
#'
#' Queries the Dallas Police Open Data API to retrieve unique values
#' for a specified field from a chosen dataset (Incidents, Arrests, Charges,
#' Officer Involved Shootings, or Use of Force by year).
#'
#' @description
#' This function fetches distinct values for a given field from one of the
#' supported Dallas Police datasets. This is useful for discovering filter options
#' for the corresponding `get_*` functions.
#'
#' Note: The `field` argument requires the **exact API field name** for the
#' chosen `dataset` (and `year`, if applicable), as field names can vary.
#' Check the source dataset documentation or field lists if unsure.
#'
#' @param field A character string specifying the **exact API field name** for which
#'   to retrieve distinct values (e.g., "division", "arlbeat", "chargedesc", "forcetype", "suspect_weapon").
#'   Case-sensitive.
#' @param dataset Character string specifying the dataset to query. Options are:
#'   `"incidents"` (default), `"arrests"`, `"charges"`, `"ois"`, `"uof"`.
#' @param year Numeric. Required **only** if `dataset = "uof"`. Specifies the year
#'   (2017-2020) for the Use of Force data. Ignored for other datasets.
#' @param max_values The maximum number of distinct values to retrieve. Defaults
#'   to 5000. Increase if needed, but be mindful of API performance.
#'
#' @return A character or numeric vector containing the unique values for the
#'   specified field, sorted. Returns `NULL` if the query fails,
#'   returns no data, or the field is invalid for the dataset.
#' @export
#'
#' @importFrom httr GET http_type content stop_for_status modify_url user_agent status_code
#' @importFrom jsonlite fromJSON
#' @importFrom rlang check_installed inform warn abort .data list2 %||%
#' @importFrom dplyr pull arrange all_of
#'
#' @examples
#' \dontrun{
#'   # Get distinct divisions from Incidents data (default dataset)
#'   incident_divisions <- list_distinct_values(field = "division")
#'   print(incident_divisions)
#'
#'   # Get distinct penalty classes ('pclass') from Charges data
#'   charge_pclasses <- list_distinct_values(field = "pclass", dataset = "charges")
#'   print(charge_pclasses)
#'
#'   # Get distinct arrest beats ('arlbeat') from Arrests data
#'   arrest_beats <- list_distinct_values(field = "arlbeat", dataset = "arrests")
#'   print(arrest_beats) # Note: field is numeric
#'
#'   # Get distinct outcomes from OIS data
#'   ois_outcomes <- list_distinct_values(
#'     field = "suspect_deceased_injured_or_shoot_and_miss",
#'     dataset = "ois")
#'     print(ois_outcomes)
#'
#'   # Get distinct Force Types from UoF data for 2020
#'   uof_force_types_2020 <- list_distinct_values(field = "forcetype", dataset = "uof", year = 2020)
#'   print(uof_force_types_2020)
#' }
list_distinct_values <- function(field, dataset = "incidents", year = NULL, max_values = 5000) {

  # --- Input Validation & Dependency Checks ---
  rlang::check_installed("httr", reason = "to fetch data from the API.")
  rlang::check_installed("jsonlite", reason = "to parse JSON data.")
  rlang::check_installed("dplyr", reason = "for data manipulation.")

  # Validate field
  if (!is.character(field) || length(field) != 1 || nchar(field) == 0) {
    stop("`field` must be a single, non-empty character string representing the exact API field name.", call. = FALSE)
  }
  # Validate dataset
  supported_datasets <- c("incidents", "arrests", "charges", "ois", "uof") # Added 'ois'
  if (!is.character(dataset) || length(dataset) != 1 || !dataset %in% supported_datasets) {
    stop(paste("`dataset` must be one of:", paste(supported_datasets, collapse=", ")), call. = FALSE)
  }
  # Validate year (conditionally required for UoF)
  if (dataset == "uof") {
    if (is.null(year) || !is.numeric(year) || length(year) != 1 || year %% 1 != 0) {
      stop("`year` must be provided as a single integer when `dataset = \"uof\"`.", call. = FALSE)
    }
  } else {
    if (!is.null(year)) {
      rlang::warn("`year` argument is ignored when `dataset` is not \"uof\".")
    }
  }
  # Validate max_values
  if (!is.numeric(max_values) || max_values <= 0) {
    stop("`max_values` must be a positive number.", call. = FALSE)
  }

  # --- Dataset to Resource ID Mapping ---
  resource_id <- NULL
  # Map for datasets with single endpoints
  dataset_resource_map <- list(
    "incidents" = "qv6i-rri7",
    "arrests"   = "sdr7-6v3j",
    "charges"   = "9u3q-af6p",
    "ois"       = "4gmt-jyx2"  # Added 'ois' mapping
  )
  # Map for UoF datasets (split by year)
  uof_year_resource_map <- list(
    "2020" = "nufk-2iqn",
    "2019" = "46zb-7qgj",
    "2018" = "33un-ry4j",
    "2017" = "tsu5-ca6k"
  )

  # Determine resource_id and label for messages
  if (dataset == "uof") {
    year_char <- as.character(year)
    if (!year_char %in% names(uof_year_resource_map)) {
      stop("For `dataset = \"uof\"`, `year` must be between 2017 and 2020.", call. = FALSE)
    }
    resource_id <- uof_year_resource_map[[year_char]]
    dataset_label <- paste(dataset, year)
  } else if (dataset %in% names(dataset_resource_map)) {
    resource_id <- dataset_resource_map[[dataset]]
    dataset_label <- dataset
  }
  # Safeguard check (shouldn't be reached due to validation)
  if (is.null(resource_id)) {
    stop("Internal error: Could not determine resource ID.", call. = FALSE)
  }

  # --- Base URL and Query Parameters ---
  base_url <- paste0("https://www.dallasopendata.com/resource/", resource_id, ".json")
  ua <- httr::user_agent("tidycops/0.2.0 (https://github.com/Steal-This-Code/tidycops)")

  # Construct SODA query for distinct values, ordered by the field
  query_params <- list(
    `$select` = paste0("distinct ", field),
    `$order` = field,
    `$limit` = as.integer(max_values)
  )

  # --- API Request ---
  request_url <- httr::modify_url(base_url, query = query_params)
  rlang::inform(paste("Querying distinct values for field:", shQuote(field), "from dataset:", dataset_label))

  values_vector <- NULL # Initialize return

  tryCatch({
    response <- httr::GET(request_url, ua)

    # Check for 400 Bad Request, potentially indicating invalid field
    if (httr::status_code(response) == 400) {
      error_content <- httr::content(response, "parsed", quiet = TRUE, encoding = "UTF-8") # Use quiet
      error_msg <- error_content$message %||% "Bad Request (HTTP 400)" # Use rlang %||%
      # Check specific Socrata error code if available
      if (grepl("no-such-column", error_msg, ignore.case=TRUE) || (is.list(error_content) && error_content$errorCode == "query.soql.no-such-column")){
        abort(paste0("API Error: Field ", shQuote(field), " not found or not queryable in dataset ", dataset_label, "."))
      } else {
        abort(paste("API Error:", error_msg)) # Report general 400 error
      }
    }
    # Check for other HTTP errors
    httr::stop_for_status(response, task = paste("fetch distinct values for", field, "from", dataset_label))

    # Check response type
    if (httr::http_type(response) != "application/json") {
      stop("API did not return JSON.", call. = FALSE)
    }

    content_text <- httr::content(response, "text", encoding = "UTF-8")

    # Handle empty response
    if(nchar(trimws(content_text)) <= 2 || content_text == "[]") {
      rlang::inform(paste("API returned no distinct values for field:", shQuote(field), "in dataset:", dataset_label))
      values_vector <- NULL # Return NULL if no values found
    } else {
      # Parse JSON
      data_df <- jsonlite::fromJSON(content_text, flatten = TRUE)

      # Handle potential Socrata `_1` suffix on distinct field name
      possible_field_names <- c(field, paste0(field,"_1"))
      actual_field_name <- intersect(possible_field_names, names(data_df))

      if (nrow(data_df) > 0 && length(actual_field_name) == 1) {
        # Extract the column vector
        values_vector <- dplyr::pull(data_df, dplyr::all_of(actual_field_name[1]))

        # Remove NAs and blank strings (common in distinct queries)
        if(is.character(values_vector)) {
          values_vector <- values_vector[!is.na(values_vector) & values_vector != ""]
        } else {
          values_vector <- values_vector[!is.na(values_vector)]
        }

        # Final sort in R (API $order might sometimes be inconsistent with R's sorting)
        if(length(values_vector) > 0) {
          tryCatch({
            values_vector <- sort(values_vector)
          }, error = function(e) {
            rlang::warn("Could not sort distinct values; returning in API order.")
          })
        } else {
          rlang::inform("No non-missing distinct values found after cleaning.")
          values_vector <- NULL # Return NULL if only NAs/blanks were found
        }

        # Warn if max_values limit was reached
        if(nrow(data_df) == max_values) {
          rlang::warn(paste("Reached the limit of", max_values,
                            "distinct values. Some values might be missing."))
        }
      } else {
        # Field not found in returned data frame
        rlang::warn(paste("Could not find field", shQuote(field), "in API response columns:", paste(names(data_df), collapse=", ")))
        values_vector <- NULL
      }
    }

  }, error = function(e) {
    # Report errors encountered during the process
    rlang::abort(paste("Failed to retrieve distinct values for field:", shQuote(field), "from dataset:", dataset_label),
                 parent = e)
  })

  return(values_vector)
}

#' List Available Incident Fields for a City
#'
#' Returns the available fields (columns) for a specified city's incident dataset,
#' including both standardized field names and their source field mappings.
#'
#' @description
#' This function helps users discover what fields are available in a city's
#' incident data. It shows the standardized field names (prefixed with `std_`)
#' and the corresponding source field names from the city's native dataset.
#'
#' @param city Character string specifying the city. Defaults to `"dallas"`.
#'   See [list_supported_incident_cities()] for the full list of supported cities.
#'
#' @return A tibble with columns:
#'   - `std_field`: Standardized field name (prefixed with `std_`)
#'   - `source_fields`: Comma-separated list of possible source field names
#'     that map to this standardized field
#'
#' @export
#'
#' @importFrom dplyr tibble
#'
#' @examples
#' \dontrun{
#'   # List available fields for Chicago
#'   chi_fields <- list_incident_fields("chicago")
#'   print(chi_fields)
#'
#'   # List available fields for San Francisco
#'   sf_fields <- list_incident_fields("san_francisco")
#'   print(sf_fields)
#' }
list_incident_fields <- function(city = "dallas") {
  # Validate city input
  if (!is.character(city) || length(city) != 1 || is.na(city) || !nzchar(trimws(city))) {
    stop("`city` must be a single non-empty string.", call. = FALSE)
  }

  # Normalize city key
  city <- normalize_incident_city_key(city)

  # Get the city specification
  tryCatch({
    city_spec <- get_incident_city_spec(city)
  }, error = function(e) {
    rlang::abort(paste0("Could not retrieve specifications for city: ", city,
                        ". Call list_supported_incident_cities() for available options."))
  })

  # Get the primary source (usually the first one)
  if (!is.null(city_spec$sources) && length(city_spec$sources) > 0) {
    source_spec <- city_spec$sources[[1]]
  } else {
    rlang::abort(paste("No sources found for city:", city))
  }

  # Extract field map
  field_map <- source_spec$field_map
  if (is.null(field_map)) {
    rlang::abort(paste("No field map found for city:", city))
  }

  # Convert field map to tibble
  fields_list <- mapply(
    function(std_name, source_names) {
      # Handle both single values and vectors
      if (is.null(source_names)) {
        source_str <- "N/A"
      } else if (is.character(source_names)) {
        source_str <- paste(source_names, collapse = ", ")
      } else {
        source_str <- as.character(source_names)
      }

      list(
        std_field = std_name,
        source_fields = source_str
      )
    },
    names(field_map),
    field_map,
    SIMPLIFY = FALSE
  )

  # Convert list to tibble
  result <- dplyr::tibble(
    std_field = sapply(fields_list, `[[`, 1),
    source_fields = sapply(fields_list, `[[`, 2)
  )

  # Sort by std_field name for easier reading
  result <- result[order(result$std_field), ]

  return(result)
}
