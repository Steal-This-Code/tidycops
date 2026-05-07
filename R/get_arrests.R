##### Arrest Data Retrieval #####
# R/get_arrests.R
# Fetch arrest records from Dallas Police Department open data portal

#' Fetch Dallas Police Arrest Data
#'
#' Retrieves arrest data from the Dallas Open Data portal API
#' (SODA endpoint `sdr7-6v3j`).
#'
#' @description
#' `get_arrests()` defaults to downloading raw records for the requested date
#' range, cleaning them, and then applying dedicated filters locally to
#' normalized helper fields. This is slower than portal-side filtering, but it
#' is more robust when raw values are inconsistent.
#'
#' Set `filter_strategy = "portal"` to use the previous portal-side filtering
#' behavior.
#'
#' @param start_date Optional. A character string in `'YYYY-MM-DD'` format or a
#'   `Date` object specifying the minimum arrest date (inclusive).
#' @param end_date Optional. A character string in `'YYYY-MM-DD'` format or a
#'   `Date` object specifying the maximum arrest date (inclusive).
#' @param zip_code Optional. A character or numeric vector of ZIP codes where
#'   the arrest occurred.
#' @param beat Optional. A character or numeric vector of police beats where the
#'   arrest occurred.
#' @param sector Optional. A character or numeric vector of police sectors where
#'   the arrest occurred.
#' @param district Optional. A character vector of police district names where
#'   the arrest occurred.
#' @param clean Logical. If `TRUE` (default), return cleaned data with helper
#'   columns such as `arlzip_clean` and `ararrestdate_parsed`.
#' @param filter_strategy Either `"cleaned"` (default) to filter after
#'   normalization or `"portal"` to push dedicated filters into the Socrata
#'   query.
#' @param limit The maximum number of records to return after filtering.
#'   Defaults to `1000`. Use `Inf` to return all matching records.
#' @param select A character vector specifying which columns to retrieve.
#' @param where An optional custom SoQL `WHERE` clause. If supplied, it
#'   overrides the dedicated filter arguments.
#' @param ... Additional SODA query parameters passed directly to the API URL,
#'   such as `$order = "ararrestdate DESC"`.
#'
#' @return A tibble containing the requested arrest data.
#' @export
get_arrests <- function(start_date = NULL,
                        end_date = NULL,
                        zip_code = NULL,
                        beat = NULL,
                        sector = NULL,
                        district = NULL,
                        clean = TRUE,
                        filter_strategy = c("cleaned", "portal"),
                        limit = 1000,
                        select = NULL,
                        where = NULL,
                        ...) {
  if (!is.logical(clean) || length(clean) != 1) {
    stop("`clean` must be TRUE or FALSE.", call. = FALSE)
  }

  validate_limit(limit)
  filter_strategy <- match.arg(filter_strategy)

  local_filters_present <- any(vapply(
    list(zip_code, beat, sector, district),
    function(value) !is.null(value),
    logical(1)
  ))

  other_filters_present <- !is.null(start_date) || !is.null(end_date) || local_filters_present

  if (filter_strategy == "cleaned" && local_filters_present) {
    select <- ensure_selected_columns(
      select,
      required_columns = c("arlzip", "arlbeat", "arlsector", "arldistrict"),
      context = "post-clean filtering"
    )
  }

  if (!is.null(where)) {
    if (!is.character(where) || length(where) != 1) {
      stop("`where` must be a single string.", call. = FALSE)
    }
    if (other_filters_present) {
      rlang::warn("Using 'where'; ignoring dedicated filter arguments and date range.")
    }

    final_data <- download_dpd_raw(
      dataset = "arrests",
      limit = limit,
      select = select,
      where = where,
      ...
    )
  } else if (identical(filter_strategy, "portal")) {
    where_clause <- compose_where_clause(c(
      build_date_range_clauses("ararrestdate", start_date, end_date),
      if (!is.null(zip_code)) sql_in_clause("arlzip", as.character(zip_code)),
      if (!is.null(beat)) sql_in_clause("arlbeat", coerce_numeric_filter(beat)),
      if (!is.null(sector)) sql_in_clause("arlsector", coerce_numeric_filter(sector)),
      if (!is.null(district)) sql_in_clause("arldistrict", district)
    ))

    final_data <- download_dpd_raw(
      dataset = "arrests",
      limit = limit,
      select = select,
      where = where_clause,
      ...
    )
  } else {
    download_limit <- if (local_filters_present) Inf else limit

    if (local_filters_present) {
      rlang::inform(
        paste(
          "Applying filters after normalization;",
          "downloading the full raw dataset for the requested date range first."
        )
      )
      if (is.null(start_date) && is.null(end_date)) {
        rlang::warn(
          paste(
            "No date range was supplied.",
            "Normalized filtering may require downloading the full arrests dataset."
          )
        )
      }
    }

    final_data <- download_dpd_raw(
      dataset = "arrests",
      start_date = start_date,
      end_date = end_date,
      limit = download_limit,
      select = select,
      ...
    )
  }

  if (clean && nrow(final_data) > 0) {
    final_data <- clean_arrests_data(final_data)
  }

  if (is.null(where) && identical(filter_strategy, "cleaned") && local_filters_present && nrow(final_data) > 0) {
    final_data <- filter_arrests_locally(
      final_data,
      zip_code = zip_code,
      beat = beat,
      sector = sector,
      district = district
    )
  }

  truncate_to_limit(final_data, limit)
}
