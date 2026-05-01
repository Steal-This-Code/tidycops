# R/clean_data.R

#' Clean Dallas Police Incidents Data
#'
#' Performs basic cleaning on text fields and converts date/time columns for
#' data retrieved using [get_incidents()] or [download_city_incidents_raw()].
#'
#' @description
#' By default, this function preserves the original portal fields and adds
#' helper columns such as `division_clean`, `division_standardized`, and
#' `date1_parsed`. This makes it easier to filter safely without losing the raw
#' source values.
#'
#' @param data A data frame or tibble, typically the output from
#'   [get_incidents()].
#' @param text_fields A character vector specifying the names of text columns to
#'   clean. Defaults to common categorical fields known to have inconsistencies.
#'   Provide `NULL` to skip text cleaning.
#' @param date_fields A character vector specifying the names of date/time
#'   columns to parse. Defaults to known date/time fields in the Incidents
#'   dataset. Provide `NULL` to skip date parsing.
#' @param tz The timezone to assign during date/time parsing. Defaults to
#'   `"America/Chicago"`.
#' @param preserve_raw Logical. If `TRUE` (default), keep the original columns
#'   and add helper columns with `_clean` and `_parsed` suffixes. If `FALSE`,
#'   overwrite the source columns in place.
#' @param add_normalized_fields Logical. If `TRUE` (default), add additional
#'   helper columns used by the package for safer filtering, such as
#'   `division_standardized`, `district_standardized`, `beat_clean`, and
#'   `nibrs_code_clean`.
#'
#' @return A tibble with the requested cleaning and parsing applied.
#' @export
clean_incidents_data <- function(data,
                                 text_fields = c(
                                   "division", "district", "premise", "offincident",
                                   "signal", "ucr_disp", "status", "type"
                                 ),
                                 date_fields = c(
                                   "date1", "date2_of_occurrence_2", "reporteddate",
                                   "edate", "callorgdate", "callreceived",
                                   "callcleared", "calldispatched", "upzdate"
                                 ),
                                 tz = "America/Chicago",
                                 preserve_raw = TRUE,
                                 add_normalized_fields = TRUE) {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame or tibble.", call. = FALSE)
  }
  if (nrow(data) == 0) {
    rlang::inform("Input data has 0 rows, returning unmodified.")
    return(data)
  }
  if (!is.null(text_fields) && !is.character(text_fields)) {
    stop("`text_fields` must be a character vector of column names or NULL.", call. = FALSE)
  }
  if (!is.null(date_fields) && !is.character(date_fields)) {
    stop("`date_fields` must be a character vector of column names or NULL.", call. = FALSE)
  }
  if (!is.character(tz) || length(tz) != 1 || !(tz %in% base::OlsonNames())) {
    stop("`tz` must be a valid timezone name (see OlsonNames()).", call. = FALSE)
  }
  if (!is.logical(preserve_raw) || length(preserve_raw) != 1) {
    stop("`preserve_raw` must be TRUE or FALSE.", call. = FALSE)
  }
  if (!is.logical(add_normalized_fields) || length(add_normalized_fields) != 1) {
    stop("`add_normalized_fields` must be TRUE or FALSE.", call. = FALSE)
  }

  cleaned_data <- data

  if (!is.null(text_fields)) {
    fields_to_clean_present <- intersect(text_fields, names(cleaned_data))
    missing_fields <- setdiff(text_fields, fields_to_clean_present)
    if (length(missing_fields) > 0) {
      rlang::warn(
        paste(
          "Specified `text_fields` not found and skipped:",
          paste(missing_fields, collapse = ", ")
        )
      )
    }

    if (length(fields_to_clean_present) > 0) {
      rlang::check_installed("stringr", reason = "to clean text fields.")
      rlang::inform(
        paste(
          "Cleaning incident text fields:",
          paste(fields_to_clean_present, collapse = ", ")
        )
      )
      cleaned_data <- append_clean_columns(
        cleaned_data,
        fields = fields_to_clean_present,
        transform = normalize_text_value,
        overwrite = !preserve_raw
      )
    }
  }

  if (!is.null(date_fields)) {
    fields_to_convert_present <- intersect(date_fields, names(cleaned_data))
    missing_fields_date <- setdiff(date_fields, fields_to_convert_present)
    if (length(missing_fields_date) > 0) {
      rlang::warn(
        paste(
          "Specified `date_fields` not found and skipped:",
          paste(missing_fields_date, collapse = ", ")
        )
      )
    }

    if (length(fields_to_convert_present) > 0) {
      rlang::check_installed("lubridate", reason = "to parse date/time strings.")
      rlang::inform(
        paste(
          "Parsing incident date columns (tz =", tz, "):",
          paste(fields_to_convert_present, collapse = ", ")
        )
      )
      cleaned_data <- append_parsed_date_columns(
        cleaned_data,
        fields = fields_to_convert_present,
        tz = tz,
        overwrite = !preserve_raw
      )
    }
  }

  if (add_normalized_fields) {
    cleaned_data <- append_clean_columns(
      cleaned_data,
      fields = c("zip_code", "beat", "sector"),
      transform = normalize_id_value
    )
    cleaned_data <- append_clean_columns(
      cleaned_data,
      fields = c("nibrs_group", "nibrs_code", "nibrs_crimeagainst"),
      transform = normalize_code_value
    )

    if ("division" %in% names(cleaned_data)) {
      cleaned_data[["division_standardized"]] <- standardize_division_values(cleaned_data[["division"]])
    }
    if ("district" %in% names(cleaned_data)) {
      cleaned_data[["district_standardized"]] <- standardize_council_district_values(cleaned_data[["district"]])
    }
  }

  cleaned_data
}


#' Standardize Police Division Names
#'
#' Maps known variations of Dallas Police division names to a standard set and
#' converts the column to a factor.
#'
#' @param data A data frame or tibble containing a division column.
#' @param division_col The name of the division column as a string.
#'
#' @return A tibble with the specified division column standardized and
#'   converted to a factor.
#' @export
standardize_division <- function(data, division_col = "division") {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame or tibble.", call. = FALSE)
  }
  if (!is.character(division_col) || length(division_col) != 1) {
    stop("`division_col` must be a single string naming the column.", call. = FALSE)
  }
  if (!division_col %in% names(data)) {
    rlang::warn(paste("Column", shQuote(division_col), "not found in data. Skipping standardization."))
    return(data)
  }

  standard_levels <- c(
    "central", "northeast", "northwest", "southcentral",
    "southeast", "southwest", "northcentral"
  )

  standardized <- standardize_division_values(data[[division_col]])
  original_values <- normalize_text_value(data[[division_col]])
  introduced_na <- sum(is.na(standardized) & !is.na(original_values))

  if (introduced_na > 0) {
    rlang::warn(
      paste0(
        introduced_na, " value(s) in column ", shQuote(division_col),
        " did not match standard divisions and became NA."
      )
    )
  }

  data[[division_col]] <- factor(standardized, levels = standard_levels)
  data
}


#' Standardize Dallas Council District Names
#'
#' Attempts to map variations of Dallas City Council District names or numbers
#' to a standard format (`"1"` through `"14"`) and converts the column to a
#' factor.
#'
#' @param data A data frame or tibble containing a council district column.
#' @param district_col The name of the district column as a string.
#'
#' @return A tibble with the specified district column standardized to
#'   character values `"1"` through `"14"` and converted to a factor.
#' @export
standardize_district <- function(data, district_col = "district") {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame or tibble.", call. = FALSE)
  }
  if (!is.character(district_col) || length(district_col) != 1) {
    stop("`district_col` must be a single string naming the column.", call. = FALSE)
  }
  if (!district_col %in% names(data)) {
    rlang::warn(paste("Column", shQuote(district_col), "not found in data. Skipping standardization."))
    return(data)
  }

  standard_levels <- as.character(1:14)
  standardized <- standardize_council_district_values(data[[district_col]])
  original_values <- normalize_text_value(data[[district_col]])
  introduced_na <- sum(is.na(standardized) & !is.na(original_values))

  if (introduced_na > 0) {
    rlang::warn(
      paste0(
        introduced_na, " value(s) in column ", shQuote(district_col),
        " could not be mapped to a standard district (1-14) and became NA."
      )
    )
  }

  data[[district_col]] <- factor(standardized, levels = standard_levels)
  data
}


#' Clean Dallas Police Arrests Data
#'
#' Performs basic cleaning on text fields and converts date/time columns for
#' data retrieved using [get_arrests()] or [download_dpd_raw()].
#'
#' @description
#' By default, this function preserves the original portal fields and adds
#' helper columns such as `arlzip_clean`, `arldistrict_clean`, and
#' `ararrestdate_parsed`.
#'
#' @param data A data frame or tibble, typically the output from
#'   [get_arrests()].
#' @param text_fields A character vector specifying the names of text columns to
#'   clean. Defaults to common categorical fields in the Arrests dataset.
#'   Provide `NULL` to skip text cleaning.
#' @param date_fields A character vector specifying the names of date/time
#'   columns to parse. Defaults to known date/time fields in the Arrests
#'   dataset. Provide `NULL` to skip date parsing.
#' @param tz The timezone to assign during date/time parsing. Defaults to
#'   `"America/Chicago"`.
#' @param preserve_raw Logical. If `TRUE` (default), keep the original columns
#'   and add helper columns with `_clean` and `_parsed` suffixes. If `FALSE`,
#'   overwrite the source columns in place.
#' @param add_normalized_fields Logical. If `TRUE` (default), add additional
#'   helper columns used for safer local filtering, such as `arlbeat_clean` and
#'   `arlsector_clean`.
#'
#' @return A tibble with the requested cleaning and parsing applied.
#' @export
clean_arrests_data <- function(data,
                               text_fields = c(
                                 "arlzip", "arlcity", "arstate", "arldistrict",
                                 "aradow", "arpremises", "arweapon", "arcond",
                                 "race", "ethnic", "sex"
                               ),
                               date_fields = c(
                                 "ararrestdate", "arbkdate", "warrantissueddate",
                                 "changedate", "ofcr_rpt_written_by_date",
                                 "ofcr_approved_by_date", "ofcr_received_by_date",
                                 "apprehended_date", "final_disp_date", "upzdate"
                               ),
                               tz = "America/Chicago",
                               preserve_raw = TRUE,
                               add_normalized_fields = TRUE) {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame or tibble.", call. = FALSE)
  }
  if (nrow(data) == 0) {
    rlang::inform("Input data has 0 rows, returning unmodified.")
    return(data)
  }
  if (!is.null(text_fields) && !is.character(text_fields)) {
    stop("`text_fields` must be a character vector of column names or NULL.", call. = FALSE)
  }
  if (!is.null(date_fields) && !is.character(date_fields)) {
    stop("`date_fields` must be a character vector of column names or NULL.", call. = FALSE)
  }
  if (!is.character(tz) || length(tz) != 1 || !(tz %in% base::OlsonNames())) {
    stop("`tz` must be a valid timezone name (see OlsonNames()).", call. = FALSE)
  }
  if (!is.logical(preserve_raw) || length(preserve_raw) != 1) {
    stop("`preserve_raw` must be TRUE or FALSE.", call. = FALSE)
  }
  if (!is.logical(add_normalized_fields) || length(add_normalized_fields) != 1) {
    stop("`add_normalized_fields` must be TRUE or FALSE.", call. = FALSE)
  }

  cleaned_data <- data

  if (!is.null(text_fields)) {
    fields_to_clean_present <- intersect(text_fields, names(cleaned_data))
    missing_fields <- setdiff(text_fields, fields_to_clean_present)
    if (length(missing_fields) > 0) {
      rlang::warn(
        paste(
          "Specified `text_fields` not found and skipped:",
          paste(missing_fields, collapse = ", ")
        )
      )
    }

    if (length(fields_to_clean_present) > 0) {
      rlang::check_installed("stringr", reason = "to clean text fields.")
      rlang::inform(
        paste(
          "Cleaning arrest text fields:",
          paste(fields_to_clean_present, collapse = ", ")
        )
      )
      cleaned_data <- append_clean_columns(
        cleaned_data,
        fields = fields_to_clean_present,
        transform = normalize_text_value,
        overwrite = !preserve_raw
      )
    }
  }

  if (!is.null(date_fields)) {
    fields_to_convert_present <- intersect(date_fields, names(cleaned_data))
    missing_fields_date <- setdiff(date_fields, fields_to_convert_present)
    if (length(missing_fields_date) > 0) {
      rlang::warn(
        paste(
          "Specified `date_fields` not found and skipped:",
          paste(missing_fields_date, collapse = ", ")
        )
      )
    }

    if (length(fields_to_convert_present) > 0) {
      rlang::check_installed("lubridate", reason = "to parse date/time strings.")
      rlang::inform(
        paste(
          "Parsing arrest date columns (tz =", tz, "):",
          paste(fields_to_convert_present, collapse = ", ")
        )
      )
      cleaned_data <- append_parsed_date_columns(
        cleaned_data,
        fields = fields_to_convert_present,
        tz = tz,
        overwrite = !preserve_raw
      )
    }
  }

  if (add_normalized_fields) {
    cleaned_data <- append_clean_columns(
      cleaned_data,
      fields = c("arlzip", "arlbeat", "arlsector"),
      transform = normalize_id_value
    )
    cleaned_data <- append_clean_columns(
      cleaned_data,
      fields = c("arldistrict"),
      transform = normalize_text_value
    )
  }

  cleaned_data
}
