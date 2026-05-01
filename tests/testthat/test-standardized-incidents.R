test_that("standardized schema helper documents std columns", {
  fields <- list_standard_incident_fields()
  expect_s3_class(fields, "tbl_df")
  expect_true(all(c("field", "description") %in% names(fields)))
  expect_true("std_incident_date" %in% fields$field)
})

test_that("common field coverage helper returns ranked comparable fields", {
  coverage <- list_common_incident_fields(min_cities = 2)
  expect_s3_class(coverage, "tbl_df")
  expect_true(all(c("field", "city_count", "coverage_pct", "cities") %in% names(coverage)))
  expect_true("std_incident_date" %in% coverage$field)
  expect_false(any(coverage$field == "std_source_id"))
})

test_that("standardization maps and parses fields", {
  source <- tidycops:::get_incident_city_spec("san_francisco")$sources[[1]]
  raw <- data.frame(
    row_id = "abc-123",
    incident_id = "i-1",
    incident_number = "2026-00001",
    incident_datetime = "2026-04-01T14:15:00.000",
    report_datetime = "2026-04-01T15:00:00.000",
    incident_code = "123",
    incident_description = "Larceny Theft",
    incident_category = "Larceny Theft",
    resolution = "Open or Active",
    intersection = "1ST ST / MARKET ST",
    analysis_neighborhood = "Mission",
    police_district = "Mission",
    latitude = "37.7751",
    longitude = "-122.4193",
    stringsAsFactors = FALSE
  )

  out <- tidycops:::standardize_incident_records(raw, city = "san_francisco", source = source)

  expect_true(all(tidycops:::standard_incident_columns() %in% names(out)))
  expect_identical(out$std_source_record_id[[1]], "abc-123")
  expect_s3_class(out$std_incident_date, "POSIXct")
  expect_identical(out$std_offense_description[[1]], "larceny theft")
  expect_equal(out$std_latitude[[1]], 37.7751)
  expect_equal(out$std_longitude[[1]], -122.4193)
})

test_that("chicago and seattle maps standardize with expected fields", {
  chicago_source <- tidycops:::get_incident_city_spec("chicago")$sources[[1]]
  chicago_raw <- data.frame(
    id = "12345",
    case_number = "JD123456",
    date = "2026-04-27T13:10:00.000",
    iucr = "0820",
    description = "$500 AND UNDER",
    primary_type = "THEFT",
    block = "001XX N STATE ST",
    district = "1",
    beat = "111",
    community_area = "32",
    latitude = "41.8835",
    longitude = "-87.6279",
    stringsAsFactors = FALSE
  )

  chicago_out <- tidycops:::standardize_incident_records(
    chicago_raw,
    city = "chicago",
    source = chicago_source
  )

  expect_identical(chicago_out$std_incident_number[[1]], "JD123456")
  expect_identical(chicago_out$std_offense_category[[1]], "theft")
  expect_identical(chicago_out$std_neighborhood[[1]], "32")
  expect_equal(chicago_out$std_latitude[[1]], 41.8835)

  seattle_source <- tidycops:::get_incident_city_spec("seattle")$sources[[1]]
  seattle_raw <- data.frame(
    offense_id = "7651042037",
    report_number = "2026-037979",
    offense_date = "2026-02-08T20:37:37.000",
    report_date_time = "2026-02-08T20:55:00.000",
    nibrs_offense_code = "23F",
    nibrs_offense_code_description = "Theft From Motor Vehicle",
    offense_category = "PROPERTY CRIME",
    block_address = "6XX BLOCK OF PINE ST",
    neighborhood = "DOWNTOWN COMMERCIAL",
    precinct = "West",
    beat = "M2",
    sector = "M",
    latitude = "47.60974973",
    longitude = "-122.3378",
    stringsAsFactors = FALSE
  )

  seattle_out <- tidycops:::standardize_incident_records(
    seattle_raw,
    city = "seattle",
    source = seattle_source
  )

  expect_identical(seattle_out$std_offense_code[[1]], "23F")
  expect_identical(seattle_out$std_offense_description[[1]], "theft from motor vehicle")
  expect_identical(seattle_out$std_division[[1]], "m")
  expect_s3_class(seattle_out$std_reported_date, "POSIXct")
})

test_that("kansas city adapter handles year-to-year field drift", {
  source <- tidycops:::get_incident_city_spec("kansas_city")$sources[[1]]

  raw_2025 <- data.frame(
    report = "2025-123456",
    report_date = "2025-06-01T12:00:00.000",
    from_date = "2025-06-01T11:30:00.000",
    ibrs = "90J",
    description = "Trespass of Real Property",
    offense = "TRESPASS",
    address = "100 BLOCK MAIN ST",
    zipcode = "64106",
    rep_dist = "123",
    beat = "123",
    area = "CENTRAL",
    stringsAsFactors = FALSE
  )

  out_2025 <- tidycops:::standardize_incident_records(
    raw_2025,
    city = "kansas_city",
    source = source
  )

  expect_identical(out_2025$std_incident_number[[1]], "2025-123456")
  expect_identical(out_2025$std_zip_code[[1]], "64106")
  expect_identical(out_2025$std_division[[1]], "central")

  raw_2024 <- data.frame(
    report_no = "2024-654321",
    reported_date = "2024-07-02T09:00:00.000",
    from_date = "2024-07-02T08:45:00.000",
    ibrs = "13B",
    description = "Simple Assault",
    offense = "ASSAULT",
    address = "200 BLOCK OAK AVE",
    zip_code = "64111",
    rep_dist = "456",
    beat = "456",
    area = "SOUTH",
    stringsAsFactors = FALSE
  )

  out_2024 <- tidycops:::standardize_incident_records(
    raw_2024,
    city = "kansas_city",
    source = source
  )

  expect_identical(out_2024$std_incident_number[[1]], "2024-654321")
  expect_identical(out_2024$std_zip_code[[1]], "64111")
  expect_identical(out_2024$std_offense_description[[1]], "simple assault")
})

test_that("cincinnati adapters handle legacy/current field drift", {
  legacy_source <- tidycops:::get_incident_city_spec("cincinnati")$sources[[1]]
  legacy_raw <- data.frame(
    instanceid = "legacy-1",
    incident_no = "149999999",
    date_from = "2024-06-02T21:15:00.000",
    date_reported = "2024-06-02T21:20:00.000",
    offense = "THEFT",
    ucr_group = "Part 1",
    clsd = "OPEN",
    street_block = "12XX E MAIN ST",
    zip = "45202",
    cpd_neighborhood = "CBD/Riverfront",
    sna_neighborhood = "Central Business District",
    beat = "D1A",
    dst = "1",
    latitude_x = "39.1031",
    longitude_x = "-84.5120",
    stringsAsFactors = FALSE
  )

  legacy_out <- tidycops:::standardize_incident_records(
    legacy_raw,
    city = "cincinnati",
    source = legacy_source
  )

  expect_identical(legacy_out$std_source_record_id[[1]], "legacy-1")
  expect_identical(legacy_out$std_incident_id[[1]], "149999999")
  expect_identical(legacy_out$std_neighborhood[[1]], "central business district")
  expect_identical(legacy_out$std_offense_description[[1]], "theft")

  current_source <- tidycops:::get_incident_city_spec("cincinnati")$sources[[2]]
  current_raw <- data.frame(
    incident_no = "259999999",
    datefrom = "2024-06-03T01:05:00.000",
    datereported = "2024-06-03T01:10:00.000",
    stars_category = "Motor Vehicle Theft",
    type = "Property Crime",
    clsd = "CLOSED",
    beat = "D2B",
    cpd_neighborhood = "Avondale",
    cc_neighborhood = "Avondale CC",
    address_x = "34XX READING RD",
    latitude_x = "39.1400",
    longitude_x = "-84.5000",
    stringsAsFactors = FALSE
  )

  current_out <- tidycops:::standardize_incident_records(
    current_raw,
    city = "cincinnati",
    source = current_source
  )

  expect_identical(current_out$std_offense_description[[1]], "motor vehicle theft")
  expect_identical(current_out$std_offense_category[[1]], "property crime")
  expect_identical(current_out$std_neighborhood[[1]], "avondale")
  expect_identical(current_out$std_beat[[1]], "D2B")
})

test_that("boston, cleveland, dc, rochester, gainesville, hartford, nola, fort lauderdale, naperville, and new york adapters map expected standardized fields", {
  boston_source <- tidycops:::get_incident_city_spec("boston")$sources[[1]]
  boston_raw <- data.frame(
    INC_NUM = "262034835",
    CRIME = "MEDICAL ASSISTANCE",
    OFFENSE_CODE = "1831",
    OFFENSE_DESC = "SICK ASSIST",
    BLOCK = "0 BLOCK SUDBURY ST",
    ZIP = "02114",
    DISTRICT = "A1",
    REPORT_DATE = 1777448460000,
    FROM_DATE = 1777448460000,
    NEIGHBORHOOD = "Downtown",
    stringsAsFactors = FALSE
  )

  boston_out <- tidycops:::standardize_incident_records(
    boston_raw,
    city = "boston",
    source = boston_source
  )

  expect_identical(boston_out$std_incident_number[[1]], "262034835")
  expect_identical(boston_out$std_offense_code[[1]], "1831")
  expect_identical(boston_out$std_offense_description[[1]], "sick assist")
  expect_identical(boston_out$std_district[[1]], "A1")
  expect_identical(boston_out$std_neighborhood[[1]], "downtown")

  cleveland_current_source <- tidycops:::get_incident_city_spec("cleveland")$sources[[2]]
  cleveland_current_raw <- data.frame(
    PrimaryKey = "2147483647",
    CaseNumber = "2026-00099927",
    District = "5",
    IncidentDesc = "Intimidation",
    ReportedDate = 1774896120000,
    OffenseDate = 1774895940000,
    Statute = "2903.22",
    StatDesc = "Menacing",
    Zip = "44108",
    Address_Public = "100XX Westchester Ave",
    WARD_2026 = "Ward 9",
    NEIGHBORHOOD = "Glenville",
    LAT = "41.5404",
    LON = "-81.6086",
    stringsAsFactors = FALSE
  )

  cleveland_current_out <- tidycops:::standardize_incident_records(
    cleveland_current_raw,
    city = "cleveland",
    source = cleveland_current_source
  )

  expect_identical(cleveland_current_out$std_incident_number[[1]], "2026-00099927")
  expect_identical(cleveland_current_out$std_offense_description[[1]], "intimidation")
  expect_identical(cleveland_current_out$std_offense_code[[1]], "2903.22")
  expect_identical(cleveland_current_out$std_neighborhood[[1]], "glenville")

  cleveland_legacy_source <- tidycops:::get_incident_city_spec("cleveland")$sources[[1]]
  cleveland_legacy_raw <- data.frame(
    PrimaryKey = "202508001553001",
    CaseNumber = "2025-08001553",
    District = "District 1",
    UCRdesc = "Vandalism",
    ReportedDate = 1762884300000,
    OffenseDate = 1761278400000,
    Statute = "623.02",
    StatDesc = "Criminal Damaging Or Endangering",
    Zip = "44102",
    Address_Public = "13XX WEST BLVD",
    WARD_2026 = "Ward 12",
    NEIGHBORHOOD = "Cudell",
    LAT = "41.4751",
    LON = "-81.7402",
    stringsAsFactors = FALSE
  )

  cleveland_legacy_out <- tidycops:::standardize_incident_records(
    cleveland_legacy_raw,
    city = "cleveland",
    source = cleveland_legacy_source
  )

  expect_identical(cleveland_legacy_out$std_offense_description[[1]], "vandalism")
  expect_identical(cleveland_legacy_out$std_incident_number[[1]], "2025-08001553")

  dc_source <- tidycops:::get_incident_city_spec("washington_dc")$sources[[1]]
  dc_raw <- data.frame(
    CCN = "26036178",
    REPORT_DAT = 1777389825000,
    START_DATE = 1777389600000,
    BLOCK = "6500 - 6599 BLOCK OF PINEY BRANCH ROAD NW",
    OFFENSE = "BURGLARY",
    METHOD = "OTHERS",
    SHIFT = "MIDNIGHT",
    WARD = "4",
    DISTRICT = "4",
    PSA = "402",
    NEIGHBORHOOD_CLUSTER = "Cluster 17",
    LATITUDE = "38.968782",
    LONGITUDE = "-77.032064",
    stringsAsFactors = FALSE
  )

  dc_out <- tidycops:::standardize_incident_records(
    dc_raw,
    city = "washington_dc",
    source = dc_source
  )

  expect_identical(dc_out$std_incident_number[[1]], "26036178")
  expect_identical(dc_out$std_offense_description[[1]], "burglary")
  expect_identical(dc_out$std_offense_category[[1]], "burglary")
  expect_identical(dc_out$std_beat[[1]], "402")
  expect_equal(dc_out$std_latitude[[1]], 38.968782)
  expect_equal(dc_out$std_longitude[[1]], -77.032064)

  rochester_source <- tidycops:::get_incident_city_spec("rochester")$sources[[1]]
  rochester_raw <- data.frame(
    Case_Number = "2015-00144240",
    OccurredFrom_Timestamp = 1433476500000,
    Reported_Timestamp = 1433480100000,
    Address_StreetFull = "1 HAWTHORNE ST",
    Patrol_Beat = "255",
    Patrol_Section = "Goodman",
    Geo_Section = "Goodman",
    Case_Status = "Office",
    Statute_Section = "155.25",
    Statute_Subsection = "",
    Statute_Description = "PETIT LARCENY",
    Statute_Text = "Larceny",
    Larceny_Type = "Theft from Motor Vehicle",
    geometry_x = "-77.6123",
    geometry_y = "43.1517",
    stringsAsFactors = FALSE
  )

  rochester_out <- tidycops:::standardize_incident_records(
    rochester_raw,
    city = "rochester",
    source = rochester_source
  )

  expect_identical(rochester_out$std_incident_number[[1]], "2015-00144240")
  expect_identical(rochester_out$std_offense_description[[1]], "petit larceny")
  expect_identical(rochester_out$std_offense_category[[1]], "larceny")
  expect_identical(rochester_out$std_district[[1]], "Goodman")

  gainesville_source <- tidycops:::get_incident_city_spec("gainesville")$sources[[1]]
  gainesville_raw <- data.frame(
    id = "123",
    offense_date = "2026-04-03T12:34:00.000",
    report_date = "2026-04-03T12:45:00.000",
    narrative = "Auto Burglary",
    address = "1200 W UNIVERSITY AVE",
    latitude = "29.6516",
    longitude = "-82.3248",
    stringsAsFactors = FALSE
  )

  gainesville_out <- tidycops:::standardize_incident_records(
    gainesville_raw,
    city = "gainesville",
    source = gainesville_source
  )

  expect_identical(gainesville_out$std_incident_id[[1]], "123")
  expect_identical(gainesville_out$std_offense_description[[1]], "auto burglary")
  expect_equal(gainesville_out$std_latitude[[1]], 29.6516)

  hartford_source <- tidycops:::get_incident_city_spec("hartford")$sources[[1]]
  hartford_raw <- data.frame(
    CaseNum = "25-030008",
    Date = 1745543160000,
    Time = "2146",
    NibrsCode = "9999",
    NibrsDesc = "NOT NIBRS REPORTABLE",
    OffenseDesc = "9999 NO OFFENSE",
    Address = "78 OAKLAND TER",
    geometry_x = "-72.69471",
    geometry_y = "41.78057",
    stringsAsFactors = FALSE
  )

  hartford_out <- tidycops:::standardize_incident_records(
    hartford_raw,
    city = "hartford",
    source = hartford_source
  )

  expect_identical(hartford_out$std_incident_number[[1]], "25-030008")
  expect_s3_class(hartford_out$std_incident_date, "POSIXct")
  expect_identical(hartford_out$std_offense_category[[1]], "not nibrs reportable")
  expect_equal(hartford_out$std_latitude[[1]], 41.78057)
  expect_equal(hartford_out$std_longitude[[1]], -72.69471)

  nola_source <- tidycops:::get_incident_city_spec("new_orleans")$sources[[1]]
  nola_raw <- data.frame(
    nopd_item = "A0000126",
    type_ = "TOW",
    typetext = "TOW IMPOUNDED VEHICLE (PRIVATE)",
    initialtypetext = "TOW IMPOUNDED VEHICLE (PRIVATE)",
    timecreate = "2026-01-01T00:00:54.023000",
    timedispatch = "2026-01-01T00:00:54.023000",
    dispositiontext = "Necessary Action Taken",
    block_address = "003XX Canal St",
    zip = "70130",
    policedistrict = "8",
    beat = "8G02",
    location.latitude = "29.9511",
    location.longitude = "-90.0715",
    stringsAsFactors = FALSE
  )

  nola_out <- tidycops:::standardize_incident_records(
    nola_raw,
    city = "new_orleans",
    source = nola_source
  )

  expect_identical(nola_out$std_incident_number[[1]], "A0000126")
  expect_identical(nola_out$std_offense_code[[1]], "TOW")
  expect_identical(nola_out$std_offense_description[[1]], "tow impounded vehicle (private)")
  expect_identical(nola_out$std_district[[1]], "8")
  expect_equal(nola_out$std_latitude[[1]], 29.9511)
  expect_equal(nola_out$std_longitude[[1]], -90.0715)

  ftl_source <- tidycops:::get_incident_city_spec("fort_lauderdale")$sources[[1]]
  ftl_raw <- data.frame(
    incidentid = "341503037300",
    offense = "SUSPICIOUS INCIDENT",
    reportedas = "DOMESTIC DISTUR",
    date_rept = "2015-03-06T03:10:58.000",
    date_occu = "2015-03-06T03:10:57.000",
    dispostndesc = "Necessary Action Taken",
    street = "NW 31ST AV",
    zip = "33309",
    district = "34",
    reportarea = "34D5",
    zonedesc = "FT LAUDERDALE ZONE 60",
    neighborhddesc = "EDGEWOOD CIVIC ASSOC",
    geox = "26.1900787353516",
    geoy = "-80.1874847412109",
    stringsAsFactors = FALSE
  )

  ftl_out <- tidycops:::standardize_incident_records(
    ftl_raw,
    city = "fort_lauderdale",
    source = ftl_source
  )

  expect_identical(ftl_out$std_incident_id[[1]], "341503037300")
  expect_identical(ftl_out$std_offense_description[[1]], "suspicious incident")
  expect_identical(ftl_out$std_offense_category[[1]], "domestic distur")
  expect_identical(ftl_out$std_beat[[1]], "34D5")
  expect_equal(ftl_out$std_latitude[[1]], 26.1900787353516)
  expect_equal(ftl_out$std_longitude[[1]], -80.1874847412109)

  naperville_source <- tidycops:::get_incident_city_spec("naperville")$sources[[2]]
  naperville_raw <- data.frame(
    INCIKEY = "53097",
    INCI_ID = "2023006559",
    NIBRS = "99L2",
    OFFENSE = "DESPONDENT / SUICIDAL SUBJECT",
    DATE_OCCU = "2023-07-02",
    DATE_REPT = "2023-07-02",
    STREETMASKED = "100-BLK of",
    STREET = "BRUCE LN/N ROUTE 59",
    REPORTEDAS = "TRAFFIC STOP",
    CATEGORY = "Mental Health",
    GEOX = "41.80",
    GEOY = "-88.20",
    stringsAsFactors = FALSE
  )

  naperville_out <- tidycops:::standardize_incident_records(
    naperville_raw,
    city = "naperville",
    source = naperville_source
  )

  expect_identical(naperville_out$std_incident_id[[1]], "2023006559")
  expect_identical(naperville_out$std_offense_code[[1]], "99L2")
  expect_identical(naperville_out$std_offense_category[[1]], "mental health")
  expect_equal(naperville_out$std_latitude[[1]], 41.8)
  expect_equal(naperville_out$std_longitude[[1]], -88.2)

  nyc_source <- tidycops:::get_incident_city_spec("new_york")$sources[[1]]
  nyc_raw <- data.frame(
    cmplnt_num = "123456789",
    cmplnt_fr_dt = "2026-03-30T00:00:00.000",
    rpt_dt = "2026-03-31T00:00:00.000",
    pd_cd = "341",
    pd_desc = "LARCENY,GRAND FROM BUILDING (NON-RESID)",
    ofns_desc = "GRAND LARCENY",
    crm_atpt_cptd_cd = "COMPLETED",
    addr_pct_cd = "14",
    patrol_boro = "PATROL BORO MAN SOUTH",
    latitude = "40.7505",
    longitude = "-73.9934",
    stringsAsFactors = FALSE
  )

  nyc_out <- tidycops:::standardize_incident_records(
    nyc_raw,
    city = "new_york",
    source = nyc_source
  )

  expect_identical(nyc_out$std_incident_number[[1]], "123456789")
  expect_identical(nyc_out$std_offense_category[[1]], "grand larceny")
  expect_identical(nyc_out$std_disposition[[1]], "completed")
  expect_identical(nyc_out$std_district[[1]], "14")
})

test_that("city-full adapter validation rejects source-specific where across multi-source city", {
  expect_error(
    get_city_incidents(
      city = "cincinnati",
      where = "incident_no IS NOT NULL",
      limit = 10
    ),
    "source-specific"
  )
})

test_that("denver, detroit, indianapolis, minneapolis, grand rapids, and houston adapters map expected standardized fields", {
  denver_source <- tidycops:::get_incident_city_spec("denver")$sources[[1]]
  denver_raw <- data.frame(
    INCIDENT_ID = "20261000001",
    OFFENSE_ID = "2026100000101",
    FIRST_OCCURRENCE_DATE = 1777351200000,
    REPORTED_DATE = 1777353000000,
    OFFENSE_CODE = "1313",
    OFFENSE_TYPE_ID = "all-other-larceny",
    OFFENSE_CATEGORY_ID = "larceny",
    INCIDENT_ADDRESS = "100 BLK 16TH ST MALL",
    DISTRICT_ID = "6",
    PRECINCT_ID = "611",
    NEIGHBORHOOD_ID = "CBD",
    GEO_LAT = "39.7487",
    GEO_LON = "-104.9962",
    stringsAsFactors = FALSE
  )

  denver_out <- tidycops:::standardize_incident_records(denver_raw, "denver", denver_source)
  expect_identical(denver_out$std_incident_number[[1]], "20261000001")
  expect_identical(denver_out$std_offense_category[[1]], "larceny")
  expect_identical(denver_out$std_district[[1]], "6")

  detroit_source <- tidycops:::get_incident_city_spec("detroit")$sources[[1]]
  detroit_raw <- data.frame(
    incident_entry_id = "abc123",
    case_id = "42026000001",
    report_number = "2501001234",
    incident_occurred_at = 1777338600000,
    updated_in_ibr_at = 1777342200000,
    state_offense_code = "750.356",
    offense_description = "Larceny",
    offense_category = "Property",
    case_status = "Active",
    nearest_intersection = "WOODWARD AVE / W GRAND BLVD",
    zip_code = "48202",
    neighborhood = "New Center",
    police_precinct = "10",
    scout_car_area = "101",
    council_district = "5",
    latitude = "42.3650",
    longitude = "-83.0735",
    stringsAsFactors = FALSE
  )

  detroit_out <- tidycops:::standardize_incident_records(detroit_raw, "detroit", detroit_source)
  expect_identical(detroit_out$std_source_record_id[[1]], "abc123")
  expect_identical(detroit_out$std_incident_number[[1]], "2501001234")
  expect_identical(detroit_out$std_offense_description[[1]], "larceny")

  indy_source <- tidycops:::get_incident_city_spec("indianapolis")$sources[[1]]
  indy_raw <- data.frame(
    CaseNum = "IP26012345",
    OccurredFrom = 1777344000000,
    sOccDate = "2026-04-27",
    CR_Desc = "THEFT",
    NIBRSClassCode = "23H",
    NIBRSClassDesc = "All Other Larceny",
    Disposition = "Open",
    sAddress = "1000 BLK N ILLINOIS ST",
    sZip = "46204",
    Geo_Districts = "N",
    Geo_Beats = "N12",
    Geo_Zones = "NORTH",
    Latitude = "39.7817",
    Longitude = "-86.1580",
    stringsAsFactors = FALSE
  )

  indy_out <- tidycops:::standardize_incident_records(indy_raw, "indianapolis", indy_source)
  expect_identical(indy_out$std_incident_number[[1]], "IP26012345")
  expect_identical(indy_out$std_offense_code[[1]], "23H")
  expect_identical(indy_out$std_division[[1]], "north")

  mpls_source <- tidycops:::get_incident_city_spec("minneapolis")$sources[[1]]
  mpls_raw <- data.frame(
    caseNumber = "MP-2026-000001",
    reportedDateTime = 1777347600000,
    reportedDate = 1777347600000,
    offense = "ASLT4",
    description = "ASSAULT 4",
    UCRCode = "5",
    publicaddress = "100 BLK HENNEPIN AVE",
    neighborhood = "Downtown West",
    precinct = "1",
    centerLat = "44.9800",
    centerLong = "-93.2700",
    stringsAsFactors = FALSE
  )

  mpls_out <- tidycops:::standardize_incident_records(mpls_raw, "minneapolis", mpls_source)
  expect_identical(mpls_out$std_incident_number[[1]], "MP-2026-000001")
  expect_identical(mpls_out$std_offense_category[[1]], "aslt4")
  expect_identical(mpls_out$std_neighborhood[[1]], "downtown west")

  gr_source <- tidycops:::get_incident_city_spec("grand_rapids")$sources[[1]]
  gr_raw <- data.frame(
    INCNUMBER = "23-000002",
    DATEOFOFFENSE = 1672531200000,
    OFFENSECODE = "13A",
    MICRCODE = "13A",
    OFFENSETITLE = "AGGRAVATED ASSAULT",
    Offense_Description = "Aggravated Assault",
    NIBRS_Category = "Assault Offenses",
    NIBRS_GRP = "Part I",
    BLOCK_ADDRESS__INCIDENT_LOCATIONS = "100 BLOCK MONROE AVE NW",
    Service_Area = "Downtown",
    Beat__ = "31",
    X = "42.9634",
    Y = "-85.6681",
    stringsAsFactors = FALSE
  )

  gr_out <- tidycops:::standardize_incident_records(gr_raw, "grand_rapids", gr_source)
  expect_identical(gr_out$std_incident_number[[1]], "23-000002")
  expect_identical(gr_out$std_offense_code[[1]], "13A")
  expect_identical(gr_out$std_offense_category[[1]], "assault offenses")
  expect_identical(gr_out$std_district[[1]], "Downtown")
  expect_equal(gr_out$std_latitude[[1]], 42.9634)
  expect_equal(gr_out$std_longitude[[1]], -85.6681)

  houston_source <- tidycops:::get_incident_city_spec("houston")$sources[[1]]
  houston_raw <- data.frame(
    USER_Incident = "054915626",
    USER_RMSOccurrenceDate = 1777420800000,
    USER_NIBRSClass = "13B",
    USER_NIBRSDescription = "Simple assault",
    USER_District = "WESTSIDE",
    USER_Beat = "18F40",
    USER_OffenseAddressMiddle = "4200 BLOCK ALMEDA RD",
    USER_BlockRange = "4200",
    USER_StreetName = "ALMEDA",
    USER_ZIPCode = "77004",
    SNBNAME = "Midtown",
    geometry_x = -95.3762,
    geometry_y = 29.7604,
    stringsAsFactors = FALSE
  )

  houston_out <- tidycops:::standardize_incident_records(houston_raw, "houston", houston_source)
  expect_identical(houston_out$std_incident_number[[1]], "054915626")
  expect_identical(houston_out$std_offense_code[[1]], "13B")
  expect_identical(houston_out$std_district[[1]], "WESTSIDE")
  expect_identical(houston_out$std_beat[[1]], "18F40")
  expect_identical(houston_out$std_neighborhood[[1]], "midtown")
  expect_equal(houston_out$std_latitude[[1]], 29.7604)
  expect_equal(houston_out$std_longitude[[1]], -95.3762)
})

test_that("pittsburgh and san antonio ckan adapters map expected standardized fields", {
  pgh_source <- tidycops:::get_incident_city_spec("pittsburgh")$sources[[1]]
  pgh_raw <- data.frame(
    Report_Number = "PGHP24000024",
    ReportedDate = "2024-01-01",
    NIBRS_Coded_Offense = "13A AGGRAVATED ASSAULT",
    NIBRS_Offense_Code = "13A",
    NIBRS_Offense_Category = "Assault Offenses",
    NIBRS_Crime_Against = "Person",
    Zone = "Zone 6",
    Tract = "1919",
    Neighborhood = "Brookline",
    Block_Address = "2800 Block of FITZHUGH WAY Pittsburgh, PA",
    XCOORD = "-80.0268",
    YCOORD = "40.3964",
    stringsAsFactors = FALSE
  )

  pgh_out <- tidycops:::standardize_incident_records(pgh_raw, "pittsburgh", pgh_source)
  expect_identical(pgh_out$std_incident_number[[1]], "PGHP24000024")
  expect_identical(pgh_out$std_offense_code[[1]], "13A")
  expect_identical(pgh_out$std_neighborhood[[1]], "brookline")
  expect_equal(pgh_out$std_latitude[[1]], 40.3964)
  expect_equal(pgh_out$std_longitude[[1]], -80.0268)

  sa_source <- tidycops:::get_incident_city_spec("san_antonio")$sources[[1]]
  sa_raw <- data.frame(
    Report_ID = "56008634329",
    Report_Date = "2024-05-09",
    DateTime = "2026-04-01",
    NIBRS_Code_Name = "Impersonation",
    NIBRS_Crime_Against = "PROPERTY",
    NIBRS_Group = "Fraud Offenses",
    Service_Area = "WEST",
    Zip_Code = "78201",
    stringsAsFactors = FALSE
  )

  sa_out <- tidycops:::standardize_incident_records(sa_raw, "san_antonio", sa_source)
  expect_identical(sa_out$std_incident_number[[1]], "56008634329")
  expect_identical(sa_out$std_offense_description[[1]], "impersonation")
  expect_identical(sa_out$std_offense_category[[1]], "fraud offenses")
  expect_identical(sa_out$std_district[[1]], "WEST")
  expect_identical(sa_out$std_zip_code[[1]], "78201")
})

test_that("city-raw adapter validation rejects source-specific where across multi-source city", {
  expect_error(
    get_city_incidents_raw(
      city = "cincinnati",
      where = "incident_no IS NOT NULL",
      limit = 10
    ),
    "source-specific"
  )
})

test_that("get_incidents city_raw mode uses raw path validation", {
  expect_error(
    get_incidents(
      city = "cincy",
      view = "city_raw",
      where = "incident_no IS NOT NULL",
      limit = 10
    ),
    "source-specific"
  )
})

test_that("limited-source coverage warnings are surfaced for scoped feeds", {
  rochester_spec <- tidycops:::get_incident_city_spec("rochester")
  rochester_msgs <- tidycops:::build_limited_source_messages(
    city_spec = rochester_spec,
    start_date = as.Date("2026-04-01"),
    end_date = as.Date("2026-04-03")
  )
  expect_true(any(grepl("scope_status=part_i_only", rochester_msgs, fixed = TRUE)))

  providence_spec <- tidycops:::get_incident_city_spec("providence")
  providence_msgs <- tidycops:::build_limited_source_messages(
    city_spec = providence_spec,
    start_date = as.Date("2026-04-01"),
    end_date = as.Date("2026-04-03")
  )
  expect_true(any(grepl("source_status=rolling_window", providence_msgs, fixed = TRUE)))

  chicago_spec <- tidycops:::get_incident_city_spec("chicago")
  chicago_msgs <- tidycops:::build_limited_source_messages(
    city_spec = chicago_spec,
    start_date = as.Date("2026-04-01"),
    end_date = as.Date("2026-04-03")
  )
  expect_length(chicago_msgs, 0)

  expect_warning(
    tidycops:::warn_limited_source_coverage(
      city_spec = rochester_spec,
      start_date = as.Date("2026-04-01"),
      end_date = as.Date("2026-04-03")
    ),
    "Coverage note"
  )
})

test_that("last_n_days date window helper computes inclusive bounds and validates input", {
  window <- tidycops:::resolve_incident_date_window(last_n_days = 7)
  expect_identical(window$end_date, Sys.Date())
  expect_identical(window$start_date, Sys.Date() - 6)

  expect_error(
    tidycops:::resolve_incident_date_window(last_n_days = 0),
    "positive integer"
  )
  expect_error(
    tidycops:::resolve_incident_date_window(last_n_days = 1.5),
    "positive integer"
  )
  expect_error(
    tidycops:::resolve_incident_date_window(
      start_date = "2026-04-01",
      last_n_days = 7
    ),
    "either `last_n_days` or `start_date`/`end_date`"
  )
})

test_that("incident fetchers reject mixing last_n_days with explicit dates", {
  expect_error(
    get_incidents(
      city = "cincy",
      last_n_days = 7,
      start_date = "2026-04-01",
      limit = 10
    ),
    "either `last_n_days` or `start_date`/`end_date`"
  )

  expect_error(
    get_standardized_incidents(
      city = "cincinnati",
      last_n_days = 7,
      end_date = "2026-04-10",
      limit = 10
    ),
    "either `last_n_days` or `start_date`/`end_date`"
  )
})

test_that("download_city_incidents_raw validates date shortcut args", {
  expect_error(
    download_city_incidents_raw(
      city = "cincinnati",
      last_n_days = 7,
      start_date = "2026-04-01",
      limit = 10
    ),
    "either `last_n_days` or `start_date`/`end_date`"
  )
})

test_that("as_sf argument validation catches non-logical inputs", {
  expect_error(
    get_incidents(
      city = "cincinnati",
      as_sf = "yes",
      limit = 10
    ),
    "`as_sf` must be TRUE or FALSE"
  )

  expect_error(
    get_standardized_incidents(
      city = "cincinnati",
      as_sf = "yes",
      limit = 10
    ),
    "`as_sf` must be TRUE or FALSE"
  )
})

test_that("standardized sf helper converts lon/lat when sf is available", {
  raw <- data.frame(
    std_city = "cincinnati",
    std_incident_number = "123",
    std_latitude = "39.1031",
    std_longitude = "-84.5120",
    stringsAsFactors = FALSE
  )

  out <- suppressWarnings(tidycops:::standardized_incidents_to_sf(raw))

  if (requireNamespace("sf", quietly = TRUE)) {
    expect_s3_class(out, "sf")
    expect_true("geometry" %in% names(out))
    expect_identical(sf::st_crs(out)$epsg, 4326L)
  } else {
    expect_false(inherits(out, "sf"))
    expect_s3_class(out, "data.frame")
  }
})
