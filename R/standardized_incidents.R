# R/standardized_incidents.R

standard_incident_columns <- function() {
  c(
    "std_city",
    "std_city_display",
    "std_source_id",
    "std_source_name",
    "std_source_dataset",
    "std_source_url",
    "std_source_record_id",
    "std_incident_id",
    "std_incident_number",
    "std_incident_date",
    "std_reported_date",
    "std_offense_code",
    "std_offense_description",
    "std_offense_category",
    "std_disposition",
    "std_address",
    "std_zip_code",
    "std_neighborhood",
    "std_district",
    "std_beat",
    "std_division",
    "std_latitude",
    "std_longitude"
  )
}

standard_incident_field_definitions <- function() {
  dplyr::tibble(
    field = standard_incident_columns(),
    description = c(
      "Normalized city key used by tidycops.",
      "Human-readable city display name.",
      "Internal source adapter id.",
      "Human-readable source dataset name.",
      "Provider dataset id (for example Socrata id).",
      "Provider API URL used for retrieval.",
      "Row-level source record identifier.",
      "Standardized incident id (best available source id).",
      "Incident/case number displayed by source.",
      "Incident occurrence datetime parsed with city timezone.",
      "Report filing datetime parsed with city timezone.",
      "Offense or statute code when available.",
      "Offense description normalized to lower case.",
      "Offense category/group normalized to lower case.",
      "Disposition/closure value normalized to lower case.",
      "Incident address/location text.",
      "Postal/ZIP code as string.",
      "Neighborhood/community label.",
      "District/precinct identifier.",
      "Beat identifier.",
      "Division/station label.",
      "Latitude in WGS84 when available.",
      "Longitude in WGS84 when available."
    )
  )
}

#' List Standard Incident Fields
#'
#' Returns the canonical incident schema used by `get_standardized_incidents()`.
#'
#' @return A tibble with standardized field names and descriptions.
#' @export
list_standard_incident_fields <- function() {
  standard_incident_field_definitions()
}

is_standard_metadata_field <- function(field) {
  field %in% c(
    "std_city",
    "std_city_display",
    "std_source_id",
    "std_source_name",
    "std_source_dataset",
    "std_source_url"
  )
}

#' List Common Comparable Incident Fields
#'
#' Summarizes which standardized incident fields are available across supported
#' city adapters. This helps identify fields suited for multi-city comparison.
#'
#' @param min_cities Minimum number of supported cities that must expose a
#'   field for it to be returned. Defaults to `1`.
#' @param include_metadata If `TRUE`, include adapter metadata fields such as
#'   `std_source_id` and `std_source_dataset`. Defaults to `FALSE`.
#'
#' @return A tibble with standardized fields, descriptions, and coverage
#'   metadata (`city_count`, `coverage_pct`, and `cities`).
#' @export
list_common_incident_fields <- function(min_cities = 1, include_metadata = FALSE) {
  if (!is.numeric(min_cities) || length(min_cities) != 1 || is.na(min_cities) || min_cities < 1) {
    stop("`min_cities` must be a single number greater than or equal to 1.", call. = FALSE)
  }
  if (!is.logical(include_metadata) || length(include_metadata) != 1) {
    stop("`include_metadata` must be TRUE or FALSE.", call. = FALSE)
  }

  cities_tbl <- list_supported_incident_cities()
  city_keys <- cities_tbl$city

  defs <- standard_incident_field_definitions()
  all_fields <- defs$field

  if (!include_metadata) {
    all_fields <- all_fields[!vapply(all_fields, is_standard_metadata_field, logical(1))]
  }

  coverage_rows <- lapply(all_fields, function(field) {
    available_in <- vapply(city_keys, function(city_key) {
      spec <- get_incident_city_spec(city_key)
      any(vapply(spec$sources, function(source) {
        mapping <- source$field_map[[field]]
        if (is.null(mapping)) {
          return(FALSE)
        }
        any(!is.na(mapping) & nzchar(mapping))
      }, logical(1)))
    }, logical(1))

    cities_with_field <- city_keys[available_in]
    city_count <- length(cities_with_field)

    dplyr::tibble(
      field = field,
      city_count = city_count,
      coverage_pct = city_count / length(city_keys),
      cities = paste(cities_with_field, collapse = ", ")
    )
  })

  out <- dplyr::left_join(defs, dplyr::bind_rows(coverage_rows), by = "field")
  out <- dplyr::filter(out, .data$city_count >= min_cities)
  dplyr::arrange(out, dplyr::desc(.data$city_count), .data$field)
}

first_present_field <- function(data, fields) {
  if (is.null(fields)) {
    return(rep(NA_character_, nrow(data)))
  }

  fields <- fields[!is.na(fields) & nzchar(fields)]
  if (length(fields) == 0) {
    return(rep(NA_character_, nrow(data)))
  }

  present <- fields[fields %in% names(data)]
  if (length(present) == 0) {
    return(rep(NA_character_, nrow(data)))
  }

  out <- as.character(data[[present[[1]]]])

  if (length(present) > 1) {
    for (field in present[-1]) {
      replacement <- as.character(data[[field]])
      replace_idx <- is.na(out) | trimws(out) == ""
      out[replace_idx] <- replacement[replace_idx]
    }
  }

  out
}

normalize_optional_id <- function(x) {
  normalize_id_value(x)
}

normalize_optional_text <- function(x) {
  normalize_text_value(x)
}

parse_standard_datetime <- function(x, tz) {
  if (length(x) == 0) {
    return(as.POSIXct(character(), tz = tz))
  }

  if (inherits(x, "POSIXt")) {
    return(lubridate::with_tz(x, tzone = tz))
  }

  if (is.numeric(x)) {
    epoch_seconds <- ifelse(abs(x) > 1e11, x / 1000, x)
    return(as.POSIXct(epoch_seconds, origin = "1970-01-01", tz = "UTC") |>
      lubridate::with_tz(tzone = tz))
  }

  x_chr <- as.character(x)
  numeric_x <- suppressWarnings(as.numeric(x_chr))
  numeric_like <- !is.na(numeric_x)

  parsed <- lubridate::parse_date_time(
    x_chr,
    orders = c(
      "Ymd HMSOS",
      "Ymd HMS",
      "Ymd HM",
      "Ymd",
      "mdY HMSOS",
      "mdY HMS",
      "mdY HM",
      "mdY"
    ),
    tz = tz,
    quiet = TRUE
  )

  if (any(numeric_like)) {
    epoch_seconds <- ifelse(abs(numeric_x[numeric_like]) > 1e11, numeric_x[numeric_like] / 1000, numeric_x[numeric_like])
    parsed[numeric_like] <- as.POSIXct(epoch_seconds, origin = "1970-01-01", tz = "UTC") |>
      lubridate::with_tz(tzone = tz)
  }

  parsed
}

standardize_incident_records <- function(data, city, source) {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame or tibble.", call. = FALSE)
  }

  city_spec <- get_incident_city_spec(city)
  field_map <- source$field_map

  standardized <- data
  standardized[["std_city"]] <- city_spec$city
  standardized[["std_city_display"]] <- city_spec$display_name
  standardized[["std_source_id"]] <- source$source_id
  standardized[["std_source_name"]] <- source$display_name
  standardized[["std_source_dataset"]] <- source$dataset_id
  standardized[["std_source_url"]] <- source$base_url

  for (std_col in names(field_map)) {
    standardized[[std_col]] <- first_present_field(standardized, field_map[[std_col]])
  }

  standardized[["std_incident_date"]] <- parse_standard_datetime(
    standardized[["std_incident_date"]],
    tz = city_spec$timezone
  )
  standardized[["std_reported_date"]] <- parse_standard_datetime(
    standardized[["std_reported_date"]],
    tz = city_spec$timezone
  )

  standardized[["std_offense_code"]] <- normalize_optional_id(standardized[["std_offense_code"]])
  standardized[["std_offense_description"]] <- normalize_optional_text(standardized[["std_offense_description"]])
  standardized[["std_offense_category"]] <- normalize_optional_text(standardized[["std_offense_category"]])
  standardized[["std_disposition"]] <- normalize_optional_text(standardized[["std_disposition"]])
  standardized[["std_address"]] <- normalize_id_value(standardized[["std_address"]])
  standardized[["std_zip_code"]] <- normalize_optional_id(standardized[["std_zip_code"]])
  standardized[["std_neighborhood"]] <- normalize_optional_text(standardized[["std_neighborhood"]])
  standardized[["std_district"]] <- normalize_optional_id(standardized[["std_district"]])
  standardized[["std_beat"]] <- normalize_optional_id(standardized[["std_beat"]])
  standardized[["std_division"]] <- normalize_optional_text(standardized[["std_division"]])
  standardized[["std_latitude"]] <- suppressWarnings(as.numeric(standardized[["std_latitude"]]))
  standardized[["std_longitude"]] <- suppressWarnings(as.numeric(standardized[["std_longitude"]]))

  standardized
}

intersect_source_window <- function(start_date, end_date, active_from, active_to) {
  source_start <- start_date
  source_end <- end_date

  if (!is.null(active_from)) {
    source_start <- if (is.null(source_start)) active_from else max(source_start, active_from)
  }

  if (!is.null(active_to)) {
    source_end <- if (is.null(source_end)) active_to else min(source_end, active_to)
  }

  if (!is.null(source_start) && !is.null(source_end) && source_start > source_end) {
    return(NULL)
  }

  list(start_date = source_start, end_date = source_end)
}

build_limited_source_messages <- function(city_spec, start_date = NULL, end_date = NULL) {
  messages <- character()

  for (source in city_spec$sources) {
    window <- intersect_source_window(
      start_date = start_date,
      end_date = end_date,
      active_from = source$active_from,
      active_to = source$active_to
    )

    if (is.null(window)) {
      next
    }

    source_status <- source$source_status %||%
      if (is.null(source$active_to)) "current" else "historical_window"
    scope_status <- source$scope_status %||% "all_incidents"

    has_source_limit <- source_status %in% c("historical_capped", "rolling_window")
    has_scope_limit <- !identical(scope_status, "all_incidents")

    if (!has_source_limit && !has_scope_limit) {
      next
    }

    details <- c()
    if (has_source_limit) {
      details <- c(details, paste0("source_status=", source_status))
    }
    if (has_scope_limit) {
      details <- c(details, paste0("scope_status=", scope_status))
    }

    messages <- c(
      messages,
      paste0(source$source_id, " (", paste(details, collapse = ", "), ")")
    )
  }

  messages
}

warn_limited_source_coverage <- function(city_spec, start_date = NULL, end_date = NULL) {
  limited <- build_limited_source_messages(
    city_spec = city_spec,
    start_date = start_date,
    end_date = end_date
  )

  if (length(limited) == 0) {
    return(invisible(FALSE))
  }

  rlang::warn(
    paste0(
      "Coverage note for ", city_spec$display_name, ": ",
      paste(limited, collapse = "; "),
      ". Use list_incident_sources(city = '", city_spec$city, "') for details."
    )
  )

  invisible(TRUE)
}

download_incident_source_raw <- function(source,
                                         start_date = NULL,
                                         end_date = NULL,
                                         limit = Inf,
                                         select = NULL,
                                         where = NULL,
                                         ...) {
  if (identical(source$provider, "socrata")) {
    effective_where <- if (is.null(where)) {
      compose_where_clause(
        build_date_range_clauses(
          date_field = source$date_field,
          start_date = start_date,
          end_date = end_date
        )
      )
    } else {
      where
    }

    return(fetch_socrata_dataset(
      base_url = source$base_url,
      limit = limit,
      select = select,
      where = effective_where,
      ...
    ))
  }

  if (identical(source$provider, "arcgis")) {
    effective_where <- if (is.null(where)) {
      compose_where_clause(
        build_arcgis_date_range_clauses(
          date_field = source$date_field,
          start_date = start_date,
          end_date = end_date,
          field_type = source$arcgis_date_field_type %||% "string"
        )
      )
    } else {
      where
    }

    return(fetch_arcgis_dataset(
      base_url = source$base_url,
      limit = limit,
      select = select,
      where = effective_where,
      order_by = source$order_by %||% paste0(source$object_id_field %||% "OBJECTID", " DESC"),
      return_geometry = source$return_geometry %||% FALSE,
      ...
    ))
  }

  if (identical(source$provider, "ckan")) {
    effective_where <- if (is.null(where)) {
      compose_where_clause(
        build_ckan_date_range_clauses(
          date_field = source$date_field,
          start_date = start_date,
          end_date = end_date,
          field_type = source$ckan_date_field_type %||% "text"
        )
      )
    } else {
      where
    }

    return(fetch_ckan_dataset(
      base_url = source$base_url,
      resource_id = source$dataset_id,
      limit = limit,
      select = select,
      where = effective_where,
      order_by = source$order_by %||%
        paste0(escape_ckan_identifier(source$date_field), " DESC"),
      ...
    ))
  }

  stop(
    paste0(
      "Unsupported incident provider `",
      source$provider,
      "`. Currently implemented providers: socrata, arcgis, ckan."
    ),
    call. = FALSE
  )
}

filter_standardized_incidents <- function(data,
                                          offense = NULL,
                                          offense_category = NULL,
                                          neighborhood = NULL,
                                          zip_code = NULL,
                                          beat = NULL,
                                          district = NULL,
                                          division = NULL,
                                          disposition = NULL) {
  filtered <- data

  filtered <- filter_vector_in(
    filtered,
    column = "std_offense_description",
    values = offense,
    transform = normalize_text_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_offense_category",
    values = offense_category,
    transform = normalize_text_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_neighborhood",
    values = neighborhood,
    transform = normalize_text_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_zip_code",
    values = zip_code,
    transform = normalize_id_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_beat",
    values = beat,
    transform = normalize_id_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_district",
    values = district,
    transform = normalize_id_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_division",
    values = division,
    transform = normalize_text_value
  )
  filtered <- filter_vector_in(
    filtered,
    column = "std_disposition",
    values = disposition,
    transform = normalize_text_value
  )

  filtered
}

sort_standardized_incidents <- function(data) {
  if (!"std_incident_date" %in% names(data)) {
    return(data)
  }

  order_idx <- order(data$std_incident_date, decreasing = TRUE, na.last = TRUE)
  data[order_idx, , drop = FALSE]
}

standardized_incidents_to_sf <- function(data,
                                         lon_col = "std_longitude",
                                         lat_col = "std_latitude") {
  if (!requireNamespace("sf", quietly = TRUE)) {
    rlang::warn(
      "Package 'sf' needed for `as_sf = TRUE` but is not installed. Returning a regular tibble."
    )
    return(data)
  }

  required_cols <- c(lon_col, lat_col)
  if (!all(required_cols %in% names(data))) {
    missing_cols <- setdiff(required_cols, names(data))
    rlang::warn(
      paste(
        "Cannot convert to geographic object. Required coordinate columns missing:",
        paste(missing_cols, collapse = ", "),
        "Returning a regular tibble."
      )
    )
    return(data)
  }

  suppressWarnings({
    data[[lon_col]] <- as.numeric(data[[lon_col]])
    data[[lat_col]] <- as.numeric(data[[lat_col]])
  })

  keep <- !is.na(data[[lon_col]]) & !is.na(data[[lat_col]])
  dropped_rows <- sum(!keep)
  data_valid <- data[keep, , drop = FALSE]

  if (dropped_rows > 0) {
    rlang::warn(
      paste(
        "Removed", dropped_rows,
        "rows with missing or invalid coordinates before geographic conversion."
      )
    )
  }

  if (nrow(data_valid) == 0) {
    rlang::warn("No rows with valid coordinates found; cannot create geographic object.")
    return(data_valid)
  }

  tryCatch({
    sf_data <- sf::st_as_sf(
      data_valid,
      coords = c(lon_col, lat_col),
      crs = 4326,
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
    data_valid
  })
}

trim_empty_character_values <- function(data) {
  if (!is.data.frame(data) || nrow(data) == 0) {
    return(data)
  }

  out <- data
  char_cols <- names(out)[vapply(out, is.character, logical(1))]

  for (col in char_cols) {
    value <- trimws(out[[col]])
    value[value == ""] <- NA_character_
    out[[col]] <- value
  }

  out
}

build_city_incident_chunk <- function(raw_chunk, city_spec, source, clean = TRUE) {
  chunk <- raw_chunk
  if (clean) {
    chunk <- trim_empty_character_values(chunk)
  }

  chunk[["city"]] <- city_spec$city
  chunk[["city_display"]] <- city_spec$display_name
  chunk[["source_id"]] <- source$source_id
  chunk[["source_name"]] <- source$display_name
  chunk[["source_dataset"]] <- source$dataset_id
  chunk[["source_url"]] <- source$base_url

  incident_date_raw <- first_present_field(chunk, source$date_field)
  reported_date_raw <- first_present_field(chunk, source$field_map$std_reported_date)

  chunk[["city_incident_date"]] <- parse_standard_datetime(
    incident_date_raw,
    tz = city_spec$timezone
  )
  chunk[["city_reported_date"]] <- parse_standard_datetime(
    reported_date_raw,
    tz = city_spec$timezone
  )

  chunk
}

sort_city_incidents <- function(data) {
  if (!"city_incident_date" %in% names(data)) {
    return(data)
  }

  order_idx <- order(data$city_incident_date, decreasing = TRUE, na.last = TRUE)
  data[order_idx, , drop = FALSE]
}

#' Fetch City Incident Data (Full City Schema)
#'
#' Downloads all available incident columns for one supported city adapter.
#' This path is useful when you want the city's full exposed schema for local
#' analysis in `dplyr`, `data.table`, `sf`, or other packages, without reducing
#' to cross-city standardized fields.
#'
#' @param city City key. See [list_supported_incident_cities()].
#' @param start_date,end_date Optional date bounds in `YYYY-MM-DD` format or as
#'   `Date` objects.
#' @param last_n_days Optional. Positive integer shortcut that sets
#'   `start_date`/`end_date` to the last `n` calendar days ending today.
#'   Cannot be combined with `start_date` or `end_date`.
#' @param limit Maximum number of rows to return after combining source chunks.
#' @param select Optional raw source columns to request. If `NULL`, all source
#'   columns are requested.
#' @param where Optional provider-specific filter clause. When supplied for a
#'   city with multiple backing sources, an error is raised.
#' @param clean If `TRUE` (default), trim whitespace and convert empty strings
#'   to `NA` for character columns.
#' @param ... Additional query parameters passed to the provider API
#'   (Socrata or ArcGIS).
#'
#' @return A tibble containing city raw columns plus adapter metadata columns:
#'   `city`, `city_display`, `source_id`, `source_name`, `source_dataset`,
#'   `source_url`, `city_incident_date`, and `city_reported_date`.
#' @export
get_city_incidents <- function(city,
                               start_date = NULL,
                               end_date = NULL,
                               last_n_days = NULL,
                               limit = 1000,
                               select = NULL,
                               where = NULL,
                               clean = TRUE,
                               ...) {
  validate_limit(limit)

  if (!is.logical(clean) || length(clean) != 1) {
    stop("`clean` must be TRUE or FALSE.", call. = FALSE)
  }

  city_spec <- get_incident_city_spec(city)
  date_window <- resolve_incident_date_window(
    start_date = start_date,
    end_date = end_date,
    last_n_days = last_n_days
  )
  start_date <- date_window$start_date
  end_date <- date_window$end_date

  if (!is.null(where) && length(city_spec$sources) > 1) {
    stop(
      paste0(
        "`where` is source-specific and is not supported for ", city_spec$display_name,
        " because this adapter spans multiple source datasets."
      ),
      call. = FALSE
    )
  }

  warn_limited_source_coverage(
    city_spec = city_spec,
    start_date = start_date,
    end_date = end_date
  )

  chunks <- list()

  for (source in city_spec$sources) {
    window <- intersect_source_window(
      start_date = start_date,
      end_date = end_date,
      active_from = source$active_from,
      active_to = source$active_to
    )

    if (is.null(window)) {
      next
    }

    raw_chunk <- download_incident_source_raw(
      source = source,
      start_date = window$start_date,
      end_date = window$end_date,
      limit = Inf,
      select = select,
      where = where,
      ...
    )

    if (nrow(raw_chunk) == 0) {
      next
    }

    chunks[[length(chunks) + 1]] <- build_city_incident_chunk(
      raw_chunk = raw_chunk,
      city_spec = city_spec,
      source = source,
      clean = clean
    )
  }

  if (length(chunks) == 0) {
    return(dplyr::tibble())
  }

  combined <- dplyr::bind_rows(chunks)
  combined <- sort_city_incidents(combined)
  truncate_to_limit(combined, limit)
}

#' Fetch City Incident Data (Raw Payload)
#'
#' Downloads untouched incident payload rows for one supported city adapter.
#' This mode is intended for "universal wrapper" usage where you want the
#' source schema exactly as exposed by each city portal, with no package-level
#' cleaning, standardization, or metadata columns added.
#'
#' @param city City key. See [list_supported_incident_cities()].
#' @param start_date,end_date Optional date bounds in `YYYY-MM-DD` format or as
#'   `Date` objects.
#' @param last_n_days Optional. Positive integer shortcut that sets
#'   `start_date`/`end_date` to the last `n` calendar days ending today.
#'   Cannot be combined with `start_date` or `end_date`.
#' @param limit Maximum number of rows to return after combining source chunks.
#' @param select Optional raw source columns to request. If `NULL`, all source
#'   columns are requested.
#' @param where Optional provider-specific filter clause. When supplied for a
#'   city with multiple backing sources, an error is raised.
#' @param ... Additional query parameters passed to the provider API
#'   (Socrata or ArcGIS).
#'
#' @return A tibble containing untouched source payload columns.
#' @export
get_city_incidents_raw <- function(city,
                                   start_date = NULL,
                                   end_date = NULL,
                                   last_n_days = NULL,
                                   limit = 1000,
                                   select = NULL,
                                   where = NULL,
                                   ...) {
  validate_limit(limit)

  city_spec <- get_incident_city_spec(city)
  date_window <- resolve_incident_date_window(
    start_date = start_date,
    end_date = end_date,
    last_n_days = last_n_days
  )
  start_date <- date_window$start_date
  end_date <- date_window$end_date

  if (!is.null(where) && length(city_spec$sources) > 1) {
    stop(
      paste0(
        "`where` is source-specific and is not supported for ", city_spec$display_name,
        " because this adapter spans multiple source datasets."
      ),
      call. = FALSE
    )
  }

  warn_limited_source_coverage(
    city_spec = city_spec,
    start_date = start_date,
    end_date = end_date
  )

  chunks <- list()

  for (source in city_spec$sources) {
    window <- intersect_source_window(
      start_date = start_date,
      end_date = end_date,
      active_from = source$active_from,
      active_to = source$active_to
    )

    if (is.null(window)) {
      next
    }

    raw_chunk <- download_incident_source_raw(
      source = source,
      start_date = window$start_date,
      end_date = window$end_date,
      limit = Inf,
      select = select,
      where = where,
      ...
    )

    if (nrow(raw_chunk) == 0) {
      next
    }

    chunks[[length(chunks) + 1]] <- raw_chunk
  }

  if (length(chunks) == 0) {
    return(dplyr::tibble())
  }

  combined <- dplyr::bind_rows(chunks)
  truncate_to_limit(combined, limit)
}

#' Fetch Standardized Incident Data
#'
#' Downloads incident data for a supported city and appends a common set of
#' standardized `std_*` columns so downstream code can work across cities.
#'
#' @param city City key. Currently supports `"dallas"`, `"san_francisco"`,
#'   `"cincinnati"`, `"providence"`, `"chicago"`, `"cleveland"`,
#'   `"rochester"`, `"seattle"`, `"boston"`, `"washington_dc"`,
#'   `"kansas_city"`, `"gainesville"`, `"hartford"`, `"houston"`,
#'   `"new_orleans"`,
#'   `"fort_lauderdale"`, `"naperville"`, `"denver"`, `"detroit"`,
#'   `"indianapolis"`, `"minneapolis"`, `"grand_rapids"`, `"pittsburgh"`,
#'   `"san_antonio"`, and `"new_york"`.
#' @param start_date,end_date Optional date bounds in `YYYY-MM-DD` format or as
#'   `Date` objects.
#' @param last_n_days Optional. Positive integer shortcut that sets
#'   `start_date`/`end_date` to the last `n` calendar days ending today.
#'   Cannot be combined with `start_date` or `end_date`.
#' @param offense Optional offense description filter applied to
#'   `std_offense_description`.
#' @param offense_category Optional offense category filter applied to
#'   `std_offense_category`.
#' @param neighborhood Optional neighborhood filter applied to
#'   `std_neighborhood`.
#' @param zip_code Optional ZIP/postal code filter applied to `std_zip_code`.
#' @param beat Optional police beat filter applied to `std_beat`.
#' @param district Optional district/precinct filter applied to `std_district`.
#' @param division Optional division/station filter applied to `std_division`.
#' @param disposition Optional disposition/closure filter applied to
#'   `std_disposition`.
#' @param limit Maximum number of rows to return after local filtering.
#' @param select Optional raw source columns to request. Required standardization
#'   columns are added automatically.
#' @param where Optional provider-specific filter clause. When supplied for a
#'   city with multiple backing sources, an error is raised.
#' @param output Output shape. `"both"` (default) returns source raw columns
#'   plus standardized `std_*` columns. `"comparable"` returns only the
#'   standardized `std_*` columns for multi-city comparisons.
#' @param as_sf Logical. If `TRUE`, attempt to convert standardized rows into
#'   an `sf` point object using `std_longitude`/`std_latitude` (EPSG:4326).
#' @param ... Additional query parameters passed to the provider API
#'   (Socrata or ArcGIS).
#'
#' @return A tibble by default. If `as_sf = TRUE` and valid coordinates are
#'   available, returns an `sf` object with CRS EPSG:4326.
#'
#' @examples
#' \dontrun{
#' sf_recent <- get_standardized_incidents(
#'   city = "san_francisco",
#'   start_date = "2026-04-01",
#'   end_date = "2026-04-07",
#'   offense_category = "larceny theft",
#'   limit = 250
#' )
#' }
#' @export
get_standardized_incidents <- function(city,
                                       start_date = NULL,
                                       end_date = NULL,
                                       last_n_days = NULL,
                                       offense = NULL,
                                       offense_category = NULL,
                                       neighborhood = NULL,
                                       zip_code = NULL,
                                       beat = NULL,
                                       district = NULL,
                                       division = NULL,
                                       disposition = NULL,
                                       limit = 1000,
                                       select = NULL,
                                       where = NULL,
                                       output = c("both", "comparable"),
                                       as_sf = FALSE,
                                       ...) {
  validate_limit(limit)
  output <- match.arg(output)
  if (!is.logical(as_sf) || length(as_sf) != 1) {
    stop("`as_sf` must be TRUE or FALSE.", call. = FALSE)
  }

  city_spec <- get_incident_city_spec(city)
  date_window <- resolve_incident_date_window(
    start_date = start_date,
    end_date = end_date,
    last_n_days = last_n_days
  )
  start_date <- date_window$start_date
  end_date <- date_window$end_date

  if (!is.null(where) && length(city_spec$sources) > 1) {
    stop(
      paste0(
        "`where` is source-specific and is not supported for ", city_spec$display_name,
        " because this adapter spans multiple source datasets."
      ),
      call. = FALSE
    )
  }

  warn_limited_source_coverage(
    city_spec = city_spec,
    start_date = start_date,
    end_date = end_date
  )

  chunks <- list()

  for (source in city_spec$sources) {
    window <- intersect_source_window(
      start_date = start_date,
      end_date = end_date,
      active_from = source$active_from,
      active_to = source$active_to
    )

    if (is.null(window)) {
      next
    }

    required_fields <- unlist(source$field_map, use.names = FALSE)
    required_fields <- required_fields[!is.na(required_fields) & nzchar(required_fields)]
    select_for_source <- ensure_selected_columns(
      select,
      required_columns = required_fields,
      context = paste(city_spec$display_name, "standardization")
    )

    raw_chunk <- download_incident_source_raw(
      source = source,
      start_date = window$start_date,
      end_date = window$end_date,
      limit = Inf,
      select = select_for_source,
      where = where,
      ...
    )

    if (nrow(raw_chunk) == 0) {
      next
    }

    chunks[[length(chunks) + 1]] <- standardize_incident_records(
      data = raw_chunk,
      city = city_spec$city,
      source = source
    )
  }

  if (length(chunks) == 0) {
    return(dplyr::tibble())
  }

  combined <- dplyr::bind_rows(chunks)
  combined <- filter_standardized_incidents(
    combined,
    offense = offense,
    offense_category = offense_category,
    neighborhood = neighborhood,
    zip_code = zip_code,
    beat = beat,
    district = district,
    division = division,
    disposition = disposition
  )
  combined <- sort_standardized_incidents(combined)
  combined <- truncate_to_limit(combined, limit)

  if (identical(output, "comparable")) {
    combined <- dplyr::select(combined, dplyr::any_of(standard_incident_columns()))
  }

  if (isTRUE(as_sf) && nrow(combined) > 0) {
    combined <- standardized_incidents_to_sf(combined)
  }

  combined
}
