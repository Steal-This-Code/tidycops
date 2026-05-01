# R/get_incidents.R

#' Fetch Incident Data
#'
#' Retrieves incident data for Dallas or any supported city adapter.
#'
#' For `city = "dallas"` this function preserves the original Dallas behavior
#' and arguments. For non-Dallas cities, `view` controls whether results are:
#' full city schema (`"city_full"`), standardized comparison schema
#' (`"comparable"`), or both (`"both"`).
#'
#' Optionally converts Dallas projected coordinates to an `sf` object.
#'
#' @description
#' `get_incidents()` now defaults to a safer pipeline:
#'
#' 1. Download raw incident records for the requested date window.
#' 2. Clean and normalize the data.
#' 3. Apply dedicated filters to normalized helper fields locally.
#'
#' This is slower than filtering directly at the portal, but it is much more
#' resilient to inconsistent casing, category drift, and data dictionary
#' mismatches in the source data.
#'
#' Set `filter_strategy = "portal"` to use the previous portal-side filtering
#' behavior when speed matters more than normalization.
#'
#' @param start_date Optional. A character string in `'YYYY-MM-DD'` format or a
#'   `Date` object specifying the minimum incident date (inclusive).
#' @param end_date Optional. A character string in `'YYYY-MM-DD'` format or a
#'   `Date` object specifying the maximum incident date (inclusive).
#' @param last_n_days Optional. Positive integer shortcut that sets
#'   `start_date`/`end_date` to the last `n` calendar days ending today.
#'   Cannot be combined with `start_date` or `end_date`.
#' @param nibrs_group Optional. A character vector of NIBRS Group codes to
#'   filter incidents.
#' @param nibrs_code Optional. A character vector of specific NIBRS offense
#'   codes to filter incidents.
#' @param nibrs_crime_against Optional. A character vector specifying the NIBRS
#'   Crime Against category.
#' @param zip_code Optional. A character or numeric vector of ZIP codes.
#' @param beat Optional. A character or numeric vector of police beats.
#' @param division Optional. A character vector of police division names.
#' @param sector Optional. A character or numeric vector of police sectors.
#' @param district Optional. A character vector of council district names or
#'   numbers.
#' @param neighborhood Optional. Neighborhood filter. For Dallas this is only
#'   applied when `standardize = TRUE`; for other supported cities it maps to
#'   the standardized neighborhood field.
#' @param offense Optional. Offense description filter applied to the
#'   standardized incident schema.
#' @param offense_category Optional. Offense category filter applied to the
#'   standardized incident schema.
#' @param disposition Optional. Disposition/closure filter applied to the
#'   standardized incident schema.
#' @param convert_geo Logical. If `TRUE`, attempt to convert the data frame to
#'   an `sf` object using `x_coordinate` and `y_cordinate`.
#' @param as_sf Logical. If `TRUE`, attempt to convert non-Dallas standardized
#'   output (`view = "both"` or `"comparable"`) into an `sf` point object
#'   using `std_longitude`/`std_latitude` (EPSG:4326). Ignored for Dallas,
#'   where `convert_geo` controls legacy projected conversion.
#' @param clean Logical. If `TRUE` (default), return cleaned data with helper
#'   columns such as `division_standardized` and `date1_parsed`.
#' @param standardize Logical or `NULL`. Dallas-only behavior. When `TRUE`,
#'   append common `std_*` columns to Dallas records. Defaults to `FALSE` for
#'   Dallas and `TRUE` for other supported cities, though non-Dallas output is
#'   now controlled primarily by `view`.
#' @param city City key. Defaults to `"dallas"`. Non-Dallas cities use the
#'   standardized multi-city adapter.
#' @param view Output mode for non-Dallas city adapters:
#'   `"both"` (default) returns city raw columns plus standardized `std_*`
#'   columns; `"comparable"` returns only `std_*` columns; `"city_full"`
#'   returns the city's full exposed schema plus lightweight adapter metadata;
#'   `"city_raw"` returns untouched source payload rows with no added columns.
#' @param filter_strategy Either `"cleaned"` (default) to filter after
#'   normalization or `"portal"` to push dedicated filters into the Socrata
#'   query.
#' @param limit The maximum number of records to return after filtering.
#'   Defaults to `1000`. Use `Inf` to return all matching records.
#' @param select A character vector specifying which columns to retrieve.
#' @param where An optional custom SoQL `WHERE` clause. If supplied, it
#'   overrides the dedicated filter arguments.
#' @param ... Additional SODA query parameters passed directly to the API URL,
#'   such as `$order = "date1 DESC"`.
#'
#' @return A tibble by default. If `convert_geo = TRUE` and coordinates are
#'   valid, returns an `sf` object with CRS EPSG:2276. For non-Dallas
#'   standardized output, `as_sf = TRUE` returns EPSG:4326 points when
#'   `std_longitude`/`std_latitude` are available.
#' @export
get_incidents <- function(start_date = NULL,
                          end_date = NULL,
                          last_n_days = NULL,
                          nibrs_group = NULL,
                          nibrs_code = NULL,
                          nibrs_crime_against = NULL,
                          zip_code = NULL,
                          beat = NULL,
                          division = NULL,
                          sector = NULL,
                          district = NULL,
                          neighborhood = NULL,
                          offense = NULL,
                          offense_category = NULL,
                          disposition = NULL,
                          convert_geo = FALSE,
                          as_sf = FALSE,
                          clean = TRUE,
                          standardize = NULL,
                          city = "dallas",
                          view = c("both", "comparable", "city_full", "city_raw"),
                          filter_strategy = c("cleaned", "portal"),
                          limit = 1000,
                          select = NULL,
                          where = NULL,
                          ...) {
  city <- normalize_incident_city_key(city)
  view <- match.arg(view)
  date_window <- resolve_incident_date_window(
    start_date = start_date,
    end_date = end_date,
    last_n_days = last_n_days
  )
  start_date <- date_window$start_date
  end_date <- date_window$end_date

  if (is.null(standardize)) {
    standardize <- !identical(city, "dallas")
  }
  if (!is.logical(as_sf) || length(as_sf) != 1) {
    stop("`as_sf` must be TRUE or FALSE.", call. = FALSE)
  }
  if (!is.logical(standardize) || length(standardize) != 1) {
    stop("`standardize` must be TRUE, FALSE, or NULL.", call. = FALSE)
  }

  if (!identical(city, "dallas")) {
    unsupported <- c(
      nibrs_group = !is.null(nibrs_group),
      nibrs_code = !is.null(nibrs_code),
      nibrs_crime_against = !is.null(nibrs_crime_against),
      sector = !is.null(sector),
      convert_geo = isTRUE(convert_geo)
    )

    if (any(unsupported)) {
      rlang::warn(
        paste(
          "Ignoring Dallas-specific arguments for city adapter",
          shQuote(city), ":",
          paste(names(unsupported)[unsupported], collapse = ", ")
        )
      )
    }

    if (identical(view, "city_full") || identical(view, "city_raw")) {
      ignored_filters <- c(
        offense = !is.null(offense),
        offense_category = !is.null(offense_category),
        neighborhood = !is.null(neighborhood),
        zip_code = !is.null(zip_code),
        beat = !is.null(beat),
        district = !is.null(district),
        division = !is.null(division),
        disposition = !is.null(disposition)
      )

      if (any(ignored_filters)) {
        rlang::warn(
          paste(
            "Ignoring standardized filters for `view = ", shQuote(view), "`:",
            paste(names(ignored_filters)[ignored_filters], collapse = ", ")
          )
        )
      }
      if (isTRUE(as_sf)) {
        rlang::warn("Ignoring `as_sf` for `view = \"city_full\"` and `view = \"city_raw\"`.")
      }

      if (identical(view, "city_raw")) {
        return(get_city_incidents_raw(
          city = city,
          start_date = start_date,
          end_date = end_date,
          limit = limit,
          select = select,
          where = where,
          ...
        ))
      }

      return(get_city_incidents(
        city = city,
        start_date = start_date,
        end_date = end_date,
        limit = limit,
        select = select,
        where = where,
        clean = clean,
        ...
      ))
    }

    return(get_standardized_incidents(
      city = city,
      start_date = start_date,
      end_date = end_date,
      offense = offense,
      offense_category = offense_category,
      neighborhood = neighborhood,
      zip_code = zip_code,
      beat = beat,
      district = district,
      division = division,
      disposition = disposition,
      limit = limit,
      select = select,
      where = where,
      output = if (identical(view, "comparable")) "comparable" else "both",
      as_sf = as_sf,
      ...
    ))
  }

  if (!is.logical(convert_geo) || length(convert_geo) != 1) {
    stop("`convert_geo` must be TRUE or FALSE.", call. = FALSE)
  }
  if (isTRUE(as_sf)) {
    rlang::warn("Ignoring `as_sf` for Dallas. Use `convert_geo = TRUE` for Dallas projected geometry.")
  }
  if (!is.logical(clean) || length(clean) != 1) {
    stop("`clean` must be TRUE or FALSE.", call. = FALSE)
  }
  if (!identical(view, "both")) {
    rlang::warn("`view` is only used for non-Dallas adapters; using Dallas default behavior.")
  }

  validate_limit(limit)
  filter_strategy <- match.arg(filter_strategy)

  local_filters_present <- any(vapply(
    list(
      nibrs_group, nibrs_code, nibrs_crime_against, zip_code,
      beat, division, sector, district
    ),
    function(value) !is.null(value),
    logical(1)
  ))

  other_filters_present <- !is.null(start_date) || !is.null(end_date) || local_filters_present

  if (convert_geo) {
    select <- ensure_selected_columns(
      select,
      required_columns = c("x_coordinate", "y_cordinate"),
      context = "geographic conversion"
    )
  }

  if (filter_strategy == "cleaned" && local_filters_present) {
    select <- ensure_selected_columns(
      select,
      required_columns = c(
        "nibrs_group", "nibrs_code", "nibrs_crimeagainst", "zip_code",
        "beat", "division", "sector", "district"
      ),
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
      dataset = "incidents",
      limit = limit,
      select = select,
      where = where,
      ...
    )
  } else if (identical(filter_strategy, "portal")) {
    where_clause <- compose_where_clause(c(
      build_date_range_clauses("date1", start_date, end_date),
      if (!is.null(nibrs_group)) sql_in_clause("nibrs_group", nibrs_group),
      if (!is.null(nibrs_code)) sql_in_clause("nibrs_code", nibrs_code),
      if (!is.null(nibrs_crime_against)) sql_in_clause("nibrs_crimeagainst", nibrs_crime_against),
      if (!is.null(zip_code)) sql_in_clause("zip_code", coerce_numeric_filter(zip_code)),
      if (!is.null(beat)) sql_in_clause("beat", coerce_numeric_filter(beat)),
      if (!is.null(division)) sql_in_clause("division", division),
      if (!is.null(sector)) sql_in_clause("sector", coerce_numeric_filter(sector)),
      if (!is.null(district)) sql_in_clause("district", district)
    ))

    final_data <- download_dpd_raw(
      dataset = "incidents",
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
            "Normalized filtering may require downloading the full incidents dataset."
          )
        )
      }
    }

    final_data <- download_dpd_raw(
      dataset = "incidents",
      start_date = start_date,
      end_date = end_date,
      limit = download_limit,
      select = select,
      ...
    )
  }

  if (clean && nrow(final_data) > 0) {
    final_data <- clean_incidents_data(final_data)
  }

  if (is.null(where) && identical(filter_strategy, "cleaned") && local_filters_present && nrow(final_data) > 0) {
    final_data <- filter_incidents_locally(
      final_data,
      nibrs_group = nibrs_group,
      nibrs_code = nibrs_code,
      nibrs_crime_against = nibrs_crime_against,
      zip_code = zip_code,
      beat = beat,
      division = division,
      sector = sector,
      district = district
    )
  }

  if (standardize && nrow(final_data) > 0) {
    final_data <- standardize_incident_records(
      data = final_data,
      city = "dallas",
      source = get_incident_city_spec("dallas")$sources[[1]]
    )

    final_data <- filter_standardized_incidents(
      final_data,
      offense = offense,
      offense_category = offense_category,
      neighborhood = neighborhood,
      zip_code = zip_code,
      beat = beat,
      district = district,
      division = division,
      disposition = disposition
    )
  }

  final_data <- truncate_to_limit(final_data, limit)

  if (convert_geo && nrow(final_data) > 0) {
    if (!requireNamespace("sf", quietly = TRUE)) {
      rlang::warn("Package 'sf' needed for `convert_geo = TRUE` but is not installed. Returning a regular tibble.")
    } else {
      required_cols <- c("x_coordinate", "y_cordinate")

      if (!all(required_cols %in% names(final_data))) {
        missing_cols <- setdiff(required_cols, names(final_data))
        rlang::warn(
          paste(
            "Cannot convert to geographic object. Required coordinate columns missing:",
            paste(missing_cols, collapse = ", "),
            "Returning a regular tibble."
          )
        )
      } else {
        rlang::inform("Attempting conversion to geographic object (sf CRS: EPSG:2276)...")

        suppressWarnings({
          final_data$x_coordinate <- as.numeric(final_data$x_coordinate)
          final_data$y_cordinate <- as.numeric(final_data$y_cordinate)
        })

        keep <- !is.na(final_data$x_coordinate) & !is.na(final_data$y_cordinate)
        dropped_rows <- sum(!keep)
        final_data_valid <- final_data[keep, , drop = FALSE]

        if (dropped_rows > 0) {
          rlang::warn(
            paste(
              "Removed", dropped_rows,
              "rows with missing or invalid coordinates before geographic conversion."
            )
          )
        }

        if (nrow(final_data_valid) > 0) {
          final_data <- tryCatch({
            sf_data <- sf::st_as_sf(
              final_data_valid,
              coords = c("x_coordinate", "y_cordinate"),
              crs = 2276,
              remove = FALSE,
              na.fail = FALSE
            )
            dplyr::relocate(sf_data, "geometry", .after = dplyr::last_col())
          }, error = function(e) {
            rlang::warn(
              paste(
                "Failed to convert data to geographic object (sf).",
                "Returning a regular tibble instead.",
                e$message
              )
            )
            final_data_valid
          })
        } else {
          rlang::warn("No rows with valid coordinates found; cannot create geographic object.")
          final_data <- final_data_valid
        }
      }
    }
  }

  final_data
}
