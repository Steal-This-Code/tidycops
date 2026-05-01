# R/incident_registry.R

normalize_incident_city_key <- function(city) {
  if (!is.character(city) || length(city) != 1 || is.na(city) || !nzchar(trimws(city))) {
    stop("`city` must be a single non-empty string.", call. = FALSE)
  }

  normalized <- tolower(trimws(city))
  normalized <- gsub("[^a-z0-9]+", "_", normalized)
  normalized <- gsub("^_+|_+$", "", normalized)

  aliases <- c(
    dallas = "dallas",
    san_francisco = "san_francisco",
    sf = "san_francisco",
    cincinnati = "cincinnati",
    cincy = "cincinnati",
    cincinnati_oh = "cincinnati",
    providence = "providence",
    chicago = "chicago",
    cleveland = "cleveland",
    cleveland_oh = "cleveland",
    seattle = "seattle",
    rochester = "rochester",
    rochester_ny = "rochester",
    kansas_city = "kansas_city",
    kansas_city_mo = "kansas_city",
    kansascity = "kansas_city",
    kc = "kansas_city",
    gainesville = "gainesville",
    gainesville_fl = "gainesville",
    houston = "houston",
    houston_tx = "houston",
    htx = "houston",
    grand_rapids = "grand_rapids",
    grandrapids = "grand_rapids",
    grand_rapids_mi = "grand_rapids",
    gr = "grand_rapids",
    boston = "boston",
    hartford = "hartford",
    hartford_ct = "hartford",
    washington_dc = "washington_dc",
    dc = "washington_dc",
    washington = "washington_dc",
    district_of_columbia = "washington_dc",
    new_orleans = "new_orleans",
    nola = "new_orleans",
    new_orleans_la = "new_orleans",
    fort_lauderdale = "fort_lauderdale",
    ft_lauderdale = "fort_lauderdale",
    fortlauderdale = "fort_lauderdale",
    fort_lauderdale_fl = "fort_lauderdale",
    naperville = "naperville",
    naperville_il = "naperville",
    denver = "denver",
    denver_co = "denver",
    detroit = "detroit",
    detroit_mi = "detroit",
    indianapolis = "indianapolis",
    indy = "indianapolis",
    indianapolis_in = "indianapolis",
    minneapolis = "minneapolis",
    mpls = "minneapolis",
    minneapolis_mn = "minneapolis",
    pittsburgh = "pittsburgh",
    pgh = "pittsburgh",
    pittsburgh_pa = "pittsburgh",
    san_antonio = "san_antonio",
    sanantonio = "san_antonio",
    sa = "san_antonio",
    satx = "san_antonio",
    san_antonio_tx = "san_antonio",
    new_york = "new_york",
    nyc = "new_york",
    new_york_city = "new_york"
  )

  resolved <- unname(aliases[normalized])

  if (length(resolved) == 0 || is.na(resolved)) {
    stop(
      paste0(
        "Unsupported city `", city, "`. ",
        "Call `list_supported_incident_cities()` to see available options."
      ),
      call. = FALSE
    )
  }

  resolved
}

get_incident_city_spec <- function(city) {
  city <- normalize_incident_city_key(city)

  kc_source <- function(year, dataset_id) {
    list(
      source_id = paste0("kc_crime_", year),
      display_name = paste("KCPD Crime Data", year),
      provider = "socrata",
      dataset_id = dataset_id,
      base_url = paste0("https://data.kcmo.org/resource/", dataset_id, ".json"),
      date_field = "from_date",
      active_from = as.Date(sprintf("%d-01-01", year)),
      active_to = as.Date(sprintf("%d-12-31", year)),
      field_map = list(
        std_source_record_id = c("report_no", "report"),
        std_incident_id = c("report_no", "report"),
        std_incident_number = c("report_no", "report"),
        std_incident_date = "from_date",
        std_reported_date = c("reported_date", "report_date"),
        std_offense_code = "ibrs",
        std_offense_description = "description",
        std_offense_category = "offense",
        std_disposition = NULL,
        std_address = "address",
        std_zip_code = c("zip_code", "zipcode", "zip_code_1"),
        std_neighborhood = NULL,
        std_district = c("rep_dist", "rep_dist_1"),
        std_beat = "beat",
        std_division = c("area", "area_1"),
        std_latitude = "latitude",
        std_longitude = "longitude"
      )
    )
  }

  nola_source <- function(year, dataset_id) {
    list(
      source_id = paste0("nola_calls_", year),
      display_name = paste("Calls for Service", year),
      provider = "socrata",
      dataset_id = dataset_id,
      base_url = paste0("https://data.nola.gov/resource/", dataset_id, ".json"),
      date_field = "timecreate",
      active_from = as.Date(sprintf("%d-01-01", year)),
      active_to = if (year == 2026) NULL else as.Date(sprintf("%d-12-31", year)),
      scope_status = "calls_for_service",
      scope_note = "This source is calls-for-service and may not match report-based incident definitions in other cities.",
      field_map = list(
        std_source_record_id = "nopd_item",
        std_incident_id = "nopd_item",
        std_incident_number = "nopd_item",
        std_incident_date = "timecreate",
        std_reported_date = c("timedispatch", "timecreate"),
        std_offense_code = c("type_", "type", "initialtype"),
        std_offense_description = c("typetext", "initialtypetext"),
        std_offense_category = c("typetext", "initialtypetext"),
        std_disposition = c("dispositiontext", "disposition"),
        std_address = "block_address",
        std_zip_code = "zip",
        std_neighborhood = NULL,
        std_district = "policedistrict",
        std_beat = "beat",
        std_division = NULL,
        std_latitude = "location.latitude",
        std_longitude = "location.longitude"
      )
    )
  }

  dc_source <- function(year, layer_id) {
    list(
      source_id = paste0("dc_crime_", year),
      display_name = paste("Crime Incidents -", year),
      provider = "arcgis",
      dataset_id = paste0("dc_mpd_layer_", layer_id),
      base_url = paste0("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/", layer_id),
      date_field = "START_DATE",
      arcgis_date_field_type = "date",
      object_id_field = "OBJECTID",
      order_by = "OBJECTID DESC",
      return_geometry = FALSE,
      active_from = as.Date(sprintf("%d-01-01", year)),
      active_to = if (year == 2026) NULL else as.Date(sprintf("%d-12-31", year)),
      field_map = list(
        std_source_record_id = c("CCN", "OCTO_RECORD_ID", "OBJECTID"),
        std_incident_id = c("CCN", "OCTO_RECORD_ID"),
        std_incident_number = "CCN",
        std_incident_date = "START_DATE",
        std_reported_date = "REPORT_DAT",
        std_offense_code = NULL,
        std_offense_description = "OFFENSE",
        std_offense_category = c("OFFENSE", "METHOD"),
        std_disposition = NULL,
        std_address = "BLOCK",
        std_zip_code = NULL,
        std_neighborhood = "NEIGHBORHOOD_CLUSTER",
        std_district = "DISTRICT",
        std_beat = "PSA",
        std_division = c("WARD", "SHIFT"),
        std_latitude = "LATITUDE",
        std_longitude = "LONGITUDE"
      )
    )
  }

  cleveland_source <- function(source_id,
                               display_name,
                               dataset_id,
                               base_url,
                               active_from,
                               active_to,
                               offense_desc_field,
                               scope_status = "all_incidents",
                               scope_note = NULL,
                               source_note = NULL) {
    list(
      source_id = source_id,
      display_name = display_name,
      provider = "arcgis",
      dataset_id = dataset_id,
      base_url = base_url,
      date_field = "OffenseDate",
      arcgis_date_field_type = "date",
      object_id_field = "OBJECTID",
      order_by = "OBJECTID DESC",
      return_geometry = FALSE,
      active_from = active_from,
      active_to = active_to,
      scope_status = scope_status,
      scope_note = scope_note,
      source_note = source_note,
      field_map = list(
        std_source_record_id = c("PrimaryKey", "CaseNumber", "OBJECTID"),
        std_incident_id = c("CaseNumber", "PrimaryKey"),
        std_incident_number = "CaseNumber",
        std_incident_date = "OffenseDate",
        std_reported_date = "ReportedDate",
        std_offense_code = "Statute",
        std_offense_description = offense_desc_field,
        std_offense_category = c(offense_desc_field, "StatDesc"),
        std_disposition = NULL,
        std_address = "Address_Public",
        std_zip_code = "Zip",
        std_neighborhood = "NEIGHBORHOOD",
        std_district = "District",
        std_beat = NULL,
        std_division = c("WARD_2026", "WARD", "WARD_2014"),
        std_latitude = "LAT",
        std_longitude = "LON"
      )
    )
  }

  houston_source <- function(layer_id, source_id, display_name) {
    list(
      source_id = source_id,
      display_name = display_name,
      provider = "arcgis",
      dataset_id = paste0("hpd_nibrs_recent_crime_reports_layer_", layer_id),
      base_url = paste0(
        "https://mycity2.houstontx.gov/pubgis02/rest/services/HPD/NIBRS_Recent_Crime_Reports/FeatureServer/",
        layer_id
      ),
      date_field = "USER_RMSOccurrenceDate",
      arcgis_date_field_type = "date",
      object_id_field = "OBJECTID",
      order_by = "OBJECTID DESC",
      return_geometry = TRUE,
      active_from = as.Date("2026-03-29"),
      active_to = NULL,
      source_status = "rolling_window",
      source_note = paste(
        "HPD ArcGIS feed is a recent rolling window",
        "(observed earliest date 2026-03-29 as of 2026-04-30);",
        "all layers are point geometry.",
        "Location context is street/block + beat/district, not areal geometries."
      ),
      field_map = list(
        std_source_record_id = c("USER_Incident", "OBJECTID"),
        std_incident_id = c("USER_Incident", "OBJECTID"),
        std_incident_number = "USER_Incident",
        std_incident_date = "USER_RMSOccurrenceDate",
        std_reported_date = "USER_RMSOccurrenceDate",
        std_offense_code = "USER_NIBRSClass",
        std_offense_description = "USER_NIBRSDescription",
        std_offense_category = c("USER_NIBRSDescription", "USER_NIBRSClass"),
        std_disposition = NULL,
        std_address = c("USER_OffenseAddressMiddle", "USER_BlockRange", "USER_StreetName"),
        std_zip_code = "USER_ZIPCode",
        std_neighborhood = "SNBNAME",
        std_district = "USER_District",
        std_beat = "USER_Beat",
        std_division = NULL,
        std_latitude = "geometry_y",
        std_longitude = "geometry_x"
      )
    )
  }

  specs <- list(
    dallas = list(
      city = "dallas",
      display_name = "Dallas",
      timezone = "America/Chicago",
      sources = list(
        list(
          source_id = "dallas_incidents",
          display_name = "Dallas Police Incidents",
          provider = "socrata",
          dataset_id = "qv6i-rri7",
          base_url = "https://www.dallasopendata.com/resource/qv6i-rri7.json",
          date_field = "date1",
          active_from = NULL,
          active_to = NULL,
          field_map = list(
            std_source_record_id = "incidentnum",
            std_incident_id = "servnumid",
            std_incident_number = "incidentnum",
            std_incident_date = "date1",
            std_reported_date = "reporteddate",
            std_offense_code = "nibrs_code",
            std_offense_description = "offincident",
            std_offense_category = "nibrs_group",
            std_disposition = "status",
            std_address = "incident_address",
            std_zip_code = "zip_code",
            std_neighborhood = "ra",
            std_district = "district",
            std_beat = "beat",
            std_division = "division",
            std_latitude = NULL,
            std_longitude = NULL
          )
        )
      )
    ),
    san_francisco = list(
      city = "san_francisco",
      display_name = "San Francisco",
      timezone = "America/Los_Angeles",
      sources = list(
        list(
          source_id = "san_francisco_incidents",
          display_name = "Police Department Incident Reports: 2018 to Present",
          provider = "socrata",
          dataset_id = "wg3w-h783",
          base_url = "https://data.sfgov.org/resource/wg3w-h783.json",
          date_field = "incident_datetime",
          active_from = as.Date("2018-01-01"),
          active_to = NULL,
          field_map = list(
            std_source_record_id = "row_id",
            std_incident_id = "incident_id",
            std_incident_number = "incident_number",
            std_incident_date = "incident_datetime",
            std_reported_date = "report_datetime",
            std_offense_code = "incident_code",
            std_offense_description = "incident_description",
            std_offense_category = "incident_category",
            std_disposition = "resolution",
            std_address = "intersection",
            std_zip_code = NULL,
            std_neighborhood = "analysis_neighborhood",
            std_district = "police_district",
            std_beat = NULL,
            std_division = NULL,
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        )
      )
    ),
    cincinnati = list(
      city = "cincinnati",
      display_name = "Cincinnati",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "cincinnati_incidents_legacy",
          display_name = "PDI Crime Incidents (through 2024-06-02)",
          provider = "socrata",
          dataset_id = "k59e-2pvf",
          base_url = "https://data.cincinnati-oh.gov/resource/k59e-2pvf.json",
          date_field = "date_from",
          active_from = NULL,
          active_to = as.Date("2024-06-02"),
          field_map = list(
            std_source_record_id = c("instanceid", "incident_no"),
            std_incident_id = c("incident_no", "instanceid"),
            std_incident_number = "incident_no",
            std_incident_date = "date_from",
            std_reported_date = "date_reported",
            std_offense_code = "ucr",
            std_offense_description = c("stars_category", "offense"),
            std_offense_category = c("ucr_group", "crime"),
            std_disposition = "clsd",
            std_address = "street_block",
            std_zip_code = c("zip", "zip_code"),
            std_neighborhood = c(
              "sna_neighborhood",
              "cpd_neighborhood",
              "community_council_neighborhood"
            ),
            std_district = "dst",
            std_beat = "beat",
            std_division = NULL,
            std_latitude = c("latitude_x", "latitude"),
            std_longitude = c("longitude_x", "longitude")
          )
        ),
        list(
          source_id = "cincinnati_incidents_current",
          display_name = "Reported Crime (STARS Category Offenses) on or after 2024-06-03",
          provider = "socrata",
          dataset_id = "7aqy-xrv9",
          base_url = "https://data.cincinnati-oh.gov/resource/7aqy-xrv9.json",
          date_field = "datefrom",
          active_from = as.Date("2024-06-03"),
          active_to = NULL,
          field_map = list(
            std_source_record_id = "incident_no",
            std_incident_id = "incident_no",
            std_incident_number = "incident_no",
            std_incident_date = "datefrom",
            std_reported_date = "datereported",
            std_offense_code = NULL,
            std_offense_description = c("stars_category", "offense"),
            std_offense_category = c("type", "crime"),
            std_disposition = "clsd",
            std_address = "address_x",
            std_zip_code = NULL,
            std_neighborhood = c("sna_neighborhood", "cpd_neighborhood", "cc_neighborhood"),
            std_district = NULL,
            std_beat = "beat",
            std_division = NULL,
            std_latitude = c("latitude_x", "latitude"),
            std_longitude = c("longitude_x", "longitude")
          )
        )
      )
    ),
    providence = list(
      city = "providence",
      display_name = "Providence",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "providence_case_log",
          display_name = "Providence Police Case Log - Past 180 days",
          provider = "socrata",
          dataset_id = "rz3y-pz8v",
          base_url = "https://data.providenceri.gov/resource/rz3y-pz8v.json",
          date_field = "reported_date",
          active_from = NULL,
          active_to = NULL,
          source_status = "rolling_window",
          source_note = "Feed publishes a rolling case log (about the past 180 days), not full historical coverage.",
          field_map = list(
            std_source_record_id = "casenumber",
            std_incident_id = "casenumber",
            std_incident_number = "casenumber",
            std_incident_date = "reported_date",
            std_reported_date = "reported_date",
            std_offense_code = "statute_code",
            std_offense_description = "offense_desc",
            std_offense_category = "statute_desc",
            std_disposition = NULL,
            std_address = "location",
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = NULL,
            std_beat = NULL,
            std_division = NULL,
            std_latitude = NULL,
            std_longitude = NULL
          )
        )
      )
    ),
    chicago = list(
      city = "chicago",
      display_name = "Chicago",
      timezone = "America/Chicago",
      sources = list(
        list(
          source_id = "chicago_crimes",
          display_name = "Crimes - 2001 to Present",
          provider = "socrata",
          dataset_id = "ijzp-q8t2",
          base_url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json",
          date_field = "date",
          active_from = NULL,
          active_to = NULL,
          field_map = list(
            std_source_record_id = "id",
            std_incident_id = "id",
            std_incident_number = "case_number",
            std_incident_date = "date",
            std_reported_date = "date",
            std_offense_code = "iucr",
            std_offense_description = "description",
            std_offense_category = "primary_type",
            std_disposition = NULL,
            std_address = "block",
            std_zip_code = NULL,
            std_neighborhood = "community_area",
            std_district = "district",
            std_beat = "beat",
            std_division = NULL,
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        )
      )
    ),
    cleveland = list(
      city = "cleveland",
      display_name = "Cleveland",
      timezone = "America/New_York",
      sources = list(
        cleveland_source(
          source_id = "cleveland_incidents_legacy",
          display_name = "Crime Incidents [2016 to 11/11/25]",
          dataset_id = "c749e34199c1425cbbc5959308658ec3",
          base_url = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Crime_Incidents/FeatureServer/0",
          active_from = as.Date("2016-01-01"),
          active_to = as.Date("2025-11-11"),
          offense_desc_field = "UCRdesc"
        ),
        cleveland_source(
          source_id = "cleveland_incidents_current",
          display_name = "Crime Incidents",
          dataset_id = "e15e8989c83e4cbd841fb171a6c62f68",
          base_url = "https://services3.arcgis.com/dty2kHktVXHrqO8i/arcgis/rest/services/Crime_Incidents_P1RMS/FeatureServer/0",
          active_from = as.Date("2025-11-12"),
          active_to = NULL,
          offense_desc_field = "IncidentDesc",
          scope_status = "highest_offense_only",
          scope_note = "Current RMS feed reports the highest offense per incident and omits lesser-included offenses.",
          source_note = "City indicates RMS transition around 2025-11-11; pre-cutover records are in legacy source."
        )
      )
    ),
    rochester = list(
      city = "rochester",
      display_name = "Rochester",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "rochester_part1_2011_present",
          display_name = "RPD - Part I Crime 2011 to Present",
          provider = "arcgis",
          dataset_id = "74c62e65e3b347e289a07d02d4b8c899",
          base_url = "https://maps.cityofrochester.gov/arcgis/rest/services/RPD/RPD_Part_I_Crime/FeatureServer/3",
          date_field = "OccurredFrom_Timestamp",
          arcgis_date_field_type = "date",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = TRUE,
          active_from = as.Date("2011-01-01"),
          active_to = NULL,
          scope_status = "part_i_only",
          scope_note = "Portal currently exposes Part I crime only; no Part II counterpart found in catalog.",
          field_map = list(
            std_source_record_id = c("Case_Number", "OBJECTID"),
            std_incident_id = c("Case_Number", "OBJECTID"),
            std_incident_number = "Case_Number",
            std_incident_date = "OccurredFrom_Timestamp",
            std_reported_date = "Reported_Timestamp",
            std_offense_code = c("Statute_Section", "Statute_Subsection"),
            std_offense_description = "Statute_Description",
            std_offense_category = c("Statute_Text", "Larceny_Type"),
            std_disposition = "Case_Status",
            std_address = "Address_StreetFull",
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = "Patrol_Section",
            std_beat = "Patrol_Beat",
            std_division = "Geo_Section",
            std_latitude = "geometry_y",
            std_longitude = "geometry_x"
          )
        )
      )
    ),
    seattle = list(
      city = "seattle",
      display_name = "Seattle",
      timezone = "America/Los_Angeles",
      sources = list(
        list(
          source_id = "seattle_spd_crime",
          display_name = "SPD Crime Data: 2008-Present",
          provider = "socrata",
          dataset_id = "tazs-3rd5",
          base_url = "https://data.seattle.gov/resource/tazs-3rd5.json",
          date_field = "offense_date",
          active_from = as.Date("2008-01-01"),
          active_to = NULL,
          field_map = list(
            std_source_record_id = "offense_id",
            std_incident_id = "offense_id",
            std_incident_number = "report_number",
            std_incident_date = "offense_date",
            std_reported_date = "report_date_time",
            std_offense_code = "nibrs_offense_code",
            std_offense_description = "nibrs_offense_code_description",
            std_offense_category = "offense_category",
            std_disposition = NULL,
            std_address = "block_address",
            std_zip_code = NULL,
            std_neighborhood = "neighborhood",
            std_district = "precinct",
            std_beat = "beat",
            std_division = "sector",
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        )
      )
    ),
    boston = list(
      city = "boston",
      display_name = "Boston",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "boston_incidents",
          display_name = "Boston Police Incidents",
          provider = "arcgis",
          dataset_id = "d42bd4040bca419a824ae5062488aced",
          base_url = "https://services.arcgis.com/sFnw0xNflSi8J0uh/arcgis/rest/services/Boston_Incidents_View/FeatureServer/0",
          date_field = "FROM_DATE",
          arcgis_date_field_type = "date",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = FALSE,
          active_from = NULL,
          active_to = NULL,
          field_map = list(
            std_source_record_id = c("INC_NUM", "OBJECTID"),
            std_incident_id = c("INC_NUM", "OBJECTID"),
            std_incident_number = "INC_NUM",
            std_incident_date = "FROM_DATE",
            std_reported_date = "REPORT_DATE",
            std_offense_code = "OFFENSE_CODE",
            std_offense_description = "OFFENSE_DESC",
            std_offense_category = c("CRIME_CATEGORY", "CRIME"),
            std_disposition = NULL,
            std_address = "BLOCK",
            std_zip_code = "ZIP",
            std_neighborhood = "NEIGHBORHOOD",
            std_district = "DISTRICT",
            std_beat = NULL,
            std_division = NULL,
            std_latitude = NULL,
            std_longitude = NULL
          )
        )
      )
    ),
    washington_dc = list(
      city = "washington_dc",
      display_name = "Washington, DC",
      timezone = "America/New_York",
      sources = list(
        dc_source(2026, 41),
        dc_source(2025, 7),
        dc_source(2024, 6),
        dc_source(2023, 5),
        dc_source(2022, 4),
        dc_source(2021, 3),
        dc_source(2020, 2),
        dc_source(2019, 1),
        dc_source(2018, 0),
        dc_source(2017, 38),
        dc_source(2016, 26),
        dc_source(2015, 27),
        dc_source(2014, 40),
        dc_source(2013, 10),
        dc_source(2012, 11),
        dc_source(2011, 35),
        dc_source(2010, 34),
        dc_source(2009, 33),
        dc_source(2008, 32)
      )
    ),
    kansas_city = list(
      city = "kansas_city",
      display_name = "Kansas City",
      timezone = "America/Chicago",
      sources = list(
        kc_source(2026, "f7wj-ckmw"),
        kc_source(2025, "dmnp-9ajg"),
        kc_source(2024, "isbe-v4d8"),
        kc_source(2023, "bfyq-5nh6"),
        kc_source(2022, "x39y-7d3m"),
        kc_source(2021, "w795-ffu6"),
        kc_source(2020, "vsgj-uufz"),
        kc_source(2019, "pxaa-ahcm"),
        kc_source(2018, "dmjw-d28i"),
        kc_source(2017, "98is-shjt"),
        kc_source(2016, "wbz8-pdv7"),
        kc_source(2015, "kbzx-7ehe")
      )
    ),
    gainesville = list(
      city = "gainesville",
      display_name = "Gainesville",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "gainesville_crime_responses",
          display_name = "Crime Responses",
          provider = "socrata",
          dataset_id = "gvua-xt9q",
          base_url = "https://data.cityofgainesville.org/resource/gvua-xt9q.json",
          date_field = "offense_date",
          active_from = NULL,
          active_to = NULL,
          field_map = list(
            std_source_record_id = "id",
            std_incident_id = "id",
            std_incident_number = "id",
            std_incident_date = "offense_date",
            std_reported_date = "report_date",
            std_offense_code = NULL,
            std_offense_description = "narrative",
            std_offense_category = NULL,
            std_disposition = NULL,
            std_address = "address",
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = NULL,
            std_beat = NULL,
            std_division = NULL,
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        )
      )
    ),
    hartford = list(
      city = "hartford",
      display_name = "Hartford",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "hartford_incidents_current",
          display_name = "Police Incidents Current Year to 10 Days before Current Date",
          provider = "arcgis",
          dataset_id = "4bc28c820ebd45df8a62feae6dc8822d",
          base_url = "https://utility.arcgis.com/usrsvcs/servers/4bc28c820ebd45df8a62feae6dc8822d/rest/services/OpenData_PublicSafety/FeatureServer/21",
          date_field = "Date",
          arcgis_date_field_type = "date",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = TRUE,
          active_from = NULL,
          active_to = NULL,
          source_status = "rolling_window",
          source_note = "Feed is current-year with about a 10-day lag; it does not contain full historical years.",
          field_map = list(
            std_source_record_id = c("CaseNum", "OBJECTID"),
            std_incident_id = c("CaseNum", "OBJECTID"),
            std_incident_number = "CaseNum",
            std_incident_date = "Date",
            std_reported_date = "Date",
            std_offense_code = "NibrsCode",
            std_offense_description = c("OffenseDesc", "NibrsDesc"),
            std_offense_category = "NibrsDesc",
            std_disposition = NULL,
            std_address = "Address",
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = NULL,
            std_beat = NULL,
            std_division = NULL,
            std_latitude = "geometry_y",
            std_longitude = "geometry_x"
          )
        )
      )
    ),
    houston = list(
      city = "houston",
      display_name = "Houston",
      timezone = "America/Chicago",
      sources = list(
        houston_source(
          layer_id = 0,
          source_id = "houston_group_a_person",
          display_name = "HPD NIBRS Recent Crime Reports - Group A Person"
        ),
        houston_source(
          layer_id = 1,
          source_id = "houston_group_a_property",
          display_name = "HPD NIBRS Recent Crime Reports - Group A Property"
        ),
        houston_source(
          layer_id = 2,
          source_id = "houston_group_a_society",
          display_name = "HPD NIBRS Recent Crime Reports - Group A Society"
        ),
        houston_source(
          layer_id = 3,
          source_id = "houston_group_b",
          display_name = "HPD NIBRS Recent Crime Reports - Group B"
        )
      )
    ),
    new_orleans = list(
      city = "new_orleans",
      display_name = "New Orleans",
      timezone = "America/Chicago",
      sources = list(
        nola_source(2026, "es9j-6y5d"),
        nola_source(2025, "4xwx-sfte"),
        nola_source(2024, "2zcj-b6ts"),
        nola_source(2023, "pc5d-tvaw"),
        nola_source(2022, "nci8-thrr"),
        nola_source(2021, "3pha-hum9"),
        nola_source(2020, "hp7u-i9hf"),
        nola_source(2019, "qf6q-pp4b"),
        nola_source(2018, "9san-ivhk"),
        nola_source(2017, "bqmt-f3jk"),
        nola_source(2016, "wgrp-d3ma"),
        nola_source(2015, "w68y-xmk6"),
        nola_source(2014, "jsyu-nz5r"),
        nola_source(2013, "5fn8-vtui"),
        nola_source(2012, "rv3g-ypg7"),
        nola_source(2011, "28ec-c8d6")
      )
    ),
    fort_lauderdale = list(
      city = "fort_lauderdale",
      display_name = "Fort Lauderdale",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "fort_lauderdale_incidents",
          display_name = "Incident",
          provider = "socrata",
          dataset_id = "4gb7-f88q",
          base_url = "https://fortlauderdale.data.socrata.com/resource/4gb7-f88q.json",
          date_field = "date_occu",
          active_from = as.Date("2014-01-01"),
          active_to = as.Date("2022-09-18"),
          source_status = "historical_capped",
          source_note = "Portal incident feed appears to stop at 2022-09-18; monitor for replacement source.",
          field_map = list(
            std_source_record_id = "incidentid",
            std_incident_id = "incidentid",
            std_incident_number = "incidentid",
            std_incident_date = "date_occu",
            std_reported_date = "date_rept",
            std_offense_code = NULL,
            std_offense_description = c("offense", "reportedas"),
            std_offense_category = c("reportedas", "offense"),
            std_disposition = c("dispostndesc", "dispostn"),
            std_address = "street",
            std_zip_code = "zip",
            std_neighborhood = c("neighborhddesc", "neighborhd"),
            std_district = "district",
            std_beat = "reportarea",
            std_division = c("zonedesc", "zone"),
            std_latitude = "geox",
            std_longitude = "geoy"
          )
        )
      )
    ),
    naperville = list(
      city = "naperville",
      display_name = "Naperville",
      timezone = "America/Chicago",
      sources = list(
        list(
          source_id = "naperville_incidents_legacy",
          display_name = "Police Department Incidents 2010 to 06_03_2021",
          provider = "arcgis",
          dataset_id = "584e8dcdd12649fe97a1ddb774705092",
          base_url = "https://services1.arcgis.com/rXJ6QApc2sOtl1Pd/arcgis/rest/services/Police_Department_Incidents_2010_to_06_03_2021/FeatureServer/0",
          date_field = "DATE_OCCU",
          arcgis_date_field_type = "string",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2010-01-01"),
          active_to = as.Date("2021-06-02"),
          field_map = list(
            std_source_record_id = c("INCIKEY", "INCI_ID", "OBJECTID"),
            std_incident_id = c("INCI_ID", "INCIKEY"),
            std_incident_number = c("INCI_ID", "INCIKEY"),
            std_incident_date = "DATE_OCCU",
            std_reported_date = "DATE_REPT",
            std_offense_code = "UCR_CODE",
            std_offense_description = "OFFENSE",
            std_offense_category = c("CATEGORY", "REPORTEDAS"),
            std_disposition = NULL,
            std_address = c("STREETMASKED", "STREET"),
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = NULL,
            std_beat = NULL,
            std_division = NULL,
            std_latitude = "GEOX",
            std_longitude = "GEOY"
          )
        ),
        list(
          source_id = "naperville_incidents_nibrs",
          display_name = "Police Department Incidents 06_03_2021 to 12_01_2024",
          provider = "arcgis",
          dataset_id = "8eb05471b7d740b8b5b610060cef6118",
          base_url = "https://services1.arcgis.com/rXJ6QApc2sOtl1Pd/arcgis/rest/services/NPD_DailyBulletinBlockNIBRSFinal_view/FeatureServer/0",
          date_field = "DATE_OCCU",
          arcgis_date_field_type = "string",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2021-06-03"),
          active_to = as.Date("2024-12-01"),
          source_status = "historical_capped",
          source_note = "Published as a historical slice ending 2024-12-01 while newer system is in progress.",
          field_map = list(
            std_source_record_id = c("INCIKEY", "INCI_ID", "OBJECTID"),
            std_incident_id = c("INCI_ID", "INCIKEY"),
            std_incident_number = c("INCI_ID", "INCIKEY"),
            std_incident_date = "DATE_OCCU",
            std_reported_date = "DATE_REPT",
            std_offense_code = "NIBRS",
            std_offense_description = "OFFENSE",
            std_offense_category = c("CATEGORY", "REPORTEDAS"),
            std_disposition = NULL,
            std_address = c("STREETMASKED", "STREET"),
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = NULL,
            std_beat = NULL,
            std_division = NULL,
            std_latitude = "GEOX",
            std_longitude = "GEOY"
          )
        )
      )
    ),
    denver = list(
      city = "denver",
      display_name = "Denver",
      timezone = "America/Denver",
      sources = list(
        list(
          source_id = "denver_crime_offenses",
          display_name = "Denver Crime Offenses (Rolling 5 Years + Current)",
          provider = "arcgis",
          dataset_id = "1e080d3ce2ae4e2698745a0d02345d4a",
          base_url = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_OFFENSES_P/FeatureServer/324",
          date_field = "FIRST_OCCURRENCE_DATE",
          arcgis_date_field_type = "date",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2021-01-01"),
          active_to = NULL,
          source_status = "rolling_window",
          source_note = "City publishes a rolling crime feed covering about the previous five years plus the current year-to-date.",
          field_map = list(
            std_source_record_id = c("OFFENSE_ID", "OBJECTID"),
            std_incident_id = c("INCIDENT_ID", "OFFENSE_ID"),
            std_incident_number = "INCIDENT_ID",
            std_incident_date = "FIRST_OCCURRENCE_DATE",
            std_reported_date = "REPORTED_DATE",
            std_offense_code = "OFFENSE_CODE",
            std_offense_description = "OFFENSE_TYPE_ID",
            std_offense_category = "OFFENSE_CATEGORY_ID",
            std_disposition = NULL,
            std_address = "INCIDENT_ADDRESS",
            std_zip_code = NULL,
            std_neighborhood = "NEIGHBORHOOD_ID",
            std_district = "DISTRICT_ID",
            std_beat = "PRECINCT_ID",
            std_division = NULL,
            std_latitude = "GEO_LAT",
            std_longitude = "GEO_LON"
          )
        )
      )
    ),
    detroit = list(
      city = "detroit",
      display_name = "Detroit",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "detroit_rms_crime_incidents",
          display_name = "Detroit RMS Crime Incidents",
          provider = "arcgis",
          dataset_id = "8e532daeec1149879bd5e67fdd9c8be0",
          base_url = "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/RMS_Crime_Incidents/FeatureServer/0",
          date_field = "incident_occurred_at",
          arcgis_date_field_type = "date",
          object_id_field = "ESRI_OID",
          order_by = "ESRI_OID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2016-12-13"),
          active_to = NULL,
          scope_status = "offense_rows",
          scope_note = "Each row is an incident-offense entry; one incident/report number may appear in multiple rows.",
          field_map = list(
            std_source_record_id = c("incident_entry_id", "crime_id", "ESRI_OID"),
            std_incident_id = c("case_id", "report_number"),
            std_incident_number = "report_number",
            std_incident_date = "incident_occurred_at",
            std_reported_date = c("updated_in_ibr_at", "updated_at", "case_status_updated_at"),
            std_offense_code = "state_offense_code",
            std_offense_description = "offense_description",
            std_offense_category = "offense_category",
            std_disposition = "case_status",
            std_address = "nearest_intersection",
            std_zip_code = "zip_code",
            std_neighborhood = "neighborhood",
            std_district = "police_precinct",
            std_beat = "scout_car_area",
            std_division = "council_district",
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        )
      )
    ),
    indianapolis = list(
      city = "indianapolis",
      display_name = "Indianapolis",
      timezone = "America/Indiana/Indianapolis",
      sources = list(
        list(
          source_id = "indy_incidents_public",
          display_name = "IMPD Incidents Public",
          provider = "arcgis",
          dataset_id = "2017ad323ea444ea92590254f08629a9",
          base_url = "https://gis.indy.gov/server/rest/services/IMPD/IMPD_Public_Data/FeatureServer/1",
          date_field = "OccurredFrom",
          arcgis_date_field_type = "date",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2020-01-01"),
          active_to = NULL,
          field_map = list(
            std_source_record_id = c("CaseNum", "OBJECTID"),
            std_incident_id = c("CaseNum", "CAD"),
            std_incident_number = "CaseNum",
            std_incident_date = "OccurredFrom",
            std_reported_date = c("sOccDate", "OccurredFrom"),
            std_offense_code = c("NIBRSClassCode", "NIBRSClassID"),
            std_offense_description = c("CR_Desc", "NIBRSClassCodeDesc"),
            std_offense_category = c("NIBRSClassDesc", "CAIU_ClassType"),
            std_disposition = "Disposition",
            std_address = "sAddress",
            std_zip_code = "sZip",
            std_neighborhood = NULL,
            std_district = "Geo_Districts",
            std_beat = "Geo_Beats",
            std_division = "Geo_Zones",
            std_latitude = "Latitude",
            std_longitude = "Longitude"
          )
        )
      )
    ),
    minneapolis = list(
      city = "minneapolis",
      display_name = "Minneapolis",
      timezone = "America/Chicago",
      sources = list(
        list(
          source_id = "minneapolis_incidents_last_2_years",
          display_name = "Police Incidents Last 2 Years",
          provider = "arcgis",
          dataset_id = "e83a2845d2384759a0a08614fc3fe812",
          base_url = "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Police_Incidents_Last_2Years/FeatureServer/0",
          date_field = "reportedDateTime",
          arcgis_date_field_type = "date",
          object_id_field = "OBJECTID",
          order_by = "OBJECTID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2024-04-30"),
          active_to = NULL,
          source_status = "rolling_window",
          source_note = "City feed represents a rolling last-two-years window with daily refresh.",
          field_map = list(
            std_source_record_id = c("caseNumber", "OBJECTID"),
            std_incident_id = "caseNumber",
            std_incident_number = "caseNumber",
            std_incident_date = "reportedDateTime",
            std_reported_date = c("reportedDate", "enteredDate"),
            std_offense_code = "UCRCode",
            std_offense_description = "description",
            std_offense_category = "offense",
            std_disposition = NULL,
            std_address = "publicaddress",
            std_zip_code = NULL,
            std_neighborhood = "neighborhood",
            std_district = "precinct",
            std_beat = NULL,
            std_division = NULL,
            std_latitude = "centerLat",
            std_longitude = "centerLong"
          )
        )
      )
    ),
    grand_rapids = list(
      city = "grand_rapids",
      display_name = "Grand Rapids",
      timezone = "America/Detroit",
      sources = list(
        list(
          source_id = "grand_rapids_incident_reports",
          display_name = "Incident Reports",
          provider = "arcgis",
          dataset_id = "a7dc3002434d4ab6869feb02ec9f7a30",
          base_url = "https://services2.arcgis.com/L81TiOwAPO1ZvU9b/arcgis/rest/services/incident_reports/FeatureServer/0",
          date_field = "DATEOFOFFENSE",
          arcgis_date_field_type = "date",
          object_id_field = "FID",
          order_by = "FID DESC",
          return_geometry = FALSE,
          active_from = as.Date("2023-01-01"),
          active_to = NULL,
          source_note = "ArcGIS layer metadata reports `hasStaticData=true`; update cadence may vary.",
          field_map = list(
            std_source_record_id = c("INCNUMBER", "FID"),
            std_incident_id = "INCNUMBER",
            std_incident_number = "INCNUMBER",
            std_incident_date = "DATEOFOFFENSE",
            std_reported_date = "DATEOFOFFENSE",
            std_offense_code = c("MICRCODE", "OFFENSECODE"),
            std_offense_description = c("OFFENSETITLE", "Offense_Description", "NIBRS_Maping"),
            std_offense_category = c("NIBRS_Category", "NIBRS_GRP"),
            std_disposition = NULL,
            std_address = "BLOCK_ADDRESS__INCIDENT_LOCATIONS",
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = "Service_Area",
            std_beat = "Beat__",
            std_division = NULL,
            std_latitude = "X",
            std_longitude = "Y"
          )
        )
      )
    ),
    pittsburgh = list(
      city = "pittsburgh",
      display_name = "Pittsburgh",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "pittsburgh_monthly_criminal_activity",
          display_name = "Monthly Criminal Activity",
          provider = "ckan",
          dataset_id = "bd41992a-987a-4cca-8798-fbe1cd946b07",
          base_url = "https://data.wprdc.org",
          date_field = "ReportedDate",
          ckan_date_field_type = "date",
          order_by = "\"ReportedDate\" DESC",
          active_from = as.Date("2024-01-01"),
          active_to = NULL,
          source_status = "rolling_window",
          source_note = "Monthly incident feed on WPRDC (current resource updates monthly and supersedes prior blotter archive).",
          field_map = list(
            std_source_record_id = c("Report_Number", "_id"),
            std_incident_id = "Report_Number",
            std_incident_number = "Report_Number",
            std_incident_date = "ReportedDate",
            std_reported_date = "ReportedDate",
            std_offense_code = "NIBRS_Offense_Code",
            std_offense_description = c("NIBRS_Coded_Offense", "NIBRS_Offense_Type"),
            std_offense_category = c("NIBRS_Offense_Category", "NIBRS_Crime_Against"),
            std_disposition = NULL,
            std_address = "Block_Address",
            std_zip_code = NULL,
            std_neighborhood = "Neighborhood",
            std_district = "Zone",
            std_beat = "Tract",
            std_division = NULL,
            std_latitude = "YCOORD",
            std_longitude = "XCOORD"
          )
        )
      )
    ),
    san_antonio = list(
      city = "san_antonio",
      display_name = "San Antonio",
      timezone = "America/Chicago",
      sources = list(
        list(
          source_id = "san_antonio_sapd_offenses",
          display_name = "SAPD Offenses",
          provider = "ckan",
          dataset_id = "f36bb931-8fb4-481c-83d9-a3589108bb20",
          base_url = "https://data.sanantonio.gov",
          date_field = "Report_Date",
          ckan_date_field_type = "text",
          order_by = "\"Report_Date\" DESC",
          active_from = NULL,
          active_to = NULL,
          field_map = list(
            std_source_record_id = c("Report_ID", "_id"),
            std_incident_id = "Report_ID",
            std_incident_number = "Report_ID",
            std_incident_date = "Report_Date",
            std_reported_date = c("DateTime", "Report_Date"),
            std_offense_code = NULL,
            std_offense_description = "NIBRS_Code_Name",
            std_offense_category = c("NIBRS_Group", "NIBRS_Crime_Against"),
            std_disposition = NULL,
            std_address = NULL,
            std_zip_code = "Zip_Code",
            std_neighborhood = NULL,
            std_district = "Service_Area",
            std_beat = NULL,
            std_division = NULL,
            std_latitude = NULL,
            std_longitude = NULL
          )
        )
      )
    ),
    new_york = list(
      city = "new_york",
      display_name = "New York City",
      timezone = "America/New_York",
      sources = list(
        list(
          source_id = "nyc_nypd_complaints_historic",
          display_name = "NYPD Complaint Data Historic",
          provider = "socrata",
          dataset_id = "qgea-i56i",
          base_url = "https://data.cityofnewyork.us/resource/qgea-i56i.json",
          date_field = "cmplnt_fr_dt",
          active_from = NULL,
          active_to = as.Date("2025-12-31"),
          field_map = list(
            std_source_record_id = "cmplnt_num",
            std_incident_id = "cmplnt_num",
            std_incident_number = "cmplnt_num",
            std_incident_date = "cmplnt_fr_dt",
            std_reported_date = "rpt_dt",
            std_offense_code = "pd_cd",
            std_offense_description = "pd_desc",
            std_offense_category = "ofns_desc",
            std_disposition = "crm_atpt_cptd_cd",
            std_address = NULL,
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = "addr_pct_cd",
            std_beat = NULL,
            std_division = "patrol_boro",
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        ),
        list(
          source_id = "nyc_nypd_complaints_current",
          display_name = "NYPD Complaint Data Current (Year To Date)",
          provider = "socrata",
          dataset_id = "5uac-w243",
          base_url = "https://data.cityofnewyork.us/resource/5uac-w243.json",
          date_field = "cmplnt_fr_dt",
          active_from = as.Date("2026-01-01"),
          active_to = NULL,
          field_map = list(
            std_source_record_id = "cmplnt_num",
            std_incident_id = "cmplnt_num",
            std_incident_number = "cmplnt_num",
            std_incident_date = "cmplnt_fr_dt",
            std_reported_date = "rpt_dt",
            std_offense_code = "pd_cd",
            std_offense_description = "pd_desc",
            std_offense_category = "ofns_desc",
            std_disposition = "crm_atpt_cptd_cd",
            std_address = NULL,
            std_zip_code = NULL,
            std_neighborhood = NULL,
            std_district = "addr_pct_cd",
            std_beat = NULL,
            std_division = "patrol_boro",
            std_latitude = "latitude",
            std_longitude = "longitude"
          )
        )
      )
    )
  )

  specs[[city]]
}

#' List Supported Incident Cities
#'
#' Returns the cities currently supported by the standardized incident wrapper.
#'
#' @return A tibble describing the supported city adapters.
#' @export
list_supported_incident_cities <- function() {
  cities <- c(
    "dallas",
    "san_francisco",
    "cincinnati",
    "providence",
    "chicago",
    "cleveland",
    "rochester",
    "seattle",
    "boston",
    "washington_dc",
    "kansas_city",
    "gainesville",
    "houston",
    "grand_rapids",
    "hartford",
    "new_orleans",
    "fort_lauderdale",
    "naperville",
    "denver",
    "detroit",
    "indianapolis",
    "minneapolis",
    "pittsburgh",
    "san_antonio",
    "new_york"
  )

  rows <- lapply(cities, function(city) {
    spec <- get_incident_city_spec(city)
    dplyr::tibble(
      city = spec$city,
      display_name = spec$display_name,
      timezone = spec$timezone,
      sources = length(spec$sources)
    )
  })

  dplyr::bind_rows(rows)
}

#' List Incident Source Registry
#'
#' Lists the configured incident source adapters and date coverage windows.
#'
#' @param city Optional city key. If `NULL` (default), returns sources for all
#'   supported cities.
#'
#' @return A tibble describing each configured incident source, including
#'   optional freshness metadata (`source_status`, `source_note`) and scope
#'   metadata (`scope_status`, `scope_note`) for bounded or definition-limited
#'   feeds.
#' @export
list_incident_sources <- function(city = NULL) {
  cities <- if (is.null(city)) {
    list_supported_incident_cities()$city
  } else {
    normalize_incident_city_key(city)
  }

  rows <- lapply(cities, function(city_key) {
    spec <- get_incident_city_spec(city_key)

    dplyr::bind_rows(lapply(spec$sources, function(source) {
      dplyr::tibble(
        city = spec$city,
        display_name = spec$display_name,
        source_id = source$source_id,
        source_name = source$display_name,
        provider = source$provider,
        dataset_id = source$dataset_id,
        source_url = source$base_url,
        date_field = source$date_field,
        active_from = source$active_from %||% as.Date(NA),
        active_to = source$active_to %||% as.Date(NA),
        source_status = source$source_status %||%
          if (is.null(source$active_to)) "current" else "historical_window",
        source_note = source$source_note %||% NA_character_,
        scope_status = source$scope_status %||% "all_incidents",
        scope_note = source$scope_note %||% NA_character_
      )
    }))
  })

  dplyr::bind_rows(rows)
}
