test_that("city key normalization resolves aliases", {
  expect_identical(tidycops:::normalize_incident_city_key("Dallas"), "dallas")
  expect_identical(tidycops:::normalize_incident_city_key("SF"), "san_francisco")
  expect_identical(tidycops:::normalize_incident_city_key("san francisco"), "san_francisco")
  expect_identical(tidycops:::normalize_incident_city_key("cincy"), "cincinnati")
  expect_identical(tidycops:::normalize_incident_city_key("cincinnati oh"), "cincinnati")
  expect_identical(tidycops:::normalize_incident_city_key("Chicago"), "chicago")
  expect_identical(tidycops:::normalize_incident_city_key("Cleveland"), "cleveland")
  expect_identical(tidycops:::normalize_incident_city_key("Rochester NY"), "rochester")
  expect_identical(tidycops:::normalize_incident_city_key("Seattle"), "seattle")
  expect_identical(tidycops:::normalize_incident_city_key("Kansas City"), "kansas_city")
  expect_identical(tidycops:::normalize_incident_city_key("kc"), "kansas_city")
  expect_identical(tidycops:::normalize_incident_city_key("Gainesville"), "gainesville")
  expect_identical(tidycops:::normalize_incident_city_key("Boston"), "boston")
  expect_identical(tidycops:::normalize_incident_city_key("Hartford CT"), "hartford")
  expect_identical(tidycops:::normalize_incident_city_key("Houston"), "houston")
  expect_identical(tidycops:::normalize_incident_city_key("DC"), "washington_dc")
  expect_identical(tidycops:::normalize_incident_city_key("NOLA"), "new_orleans")
  expect_identical(tidycops:::normalize_incident_city_key("Fort Lauderdale"), "fort_lauderdale")
  expect_identical(tidycops:::normalize_incident_city_key("Naperville"), "naperville")
  expect_identical(tidycops:::normalize_incident_city_key("Denver"), "denver")
  expect_identical(tidycops:::normalize_incident_city_key("Detroit"), "detroit")
  expect_identical(tidycops:::normalize_incident_city_key("Indy"), "indianapolis")
  expect_identical(tidycops:::normalize_incident_city_key("MPLS"), "minneapolis")
  expect_identical(tidycops:::normalize_incident_city_key("Grand Rapids"), "grand_rapids")
  expect_identical(tidycops:::normalize_incident_city_key("PGH"), "pittsburgh")
  expect_identical(tidycops:::normalize_incident_city_key("SATX"), "san_antonio")
  expect_identical(tidycops:::normalize_incident_city_key("NYC"), "new_york")
  expect_error(
    tidycops:::normalize_incident_city_key("not-a-city"),
    "Unsupported city"
  )
})

test_that("supported city list has expected columns", {
  x <- list_supported_incident_cities()
  expect_s3_class(x, "tbl_df")
  expect_true(all(c("city", "display_name", "timezone", "sources") %in% names(x)))
  expect_true("dallas" %in% x$city)
  expect_true("chicago" %in% x$city)
  expect_true("cleveland" %in% x$city)
  expect_true("rochester" %in% x$city)
  expect_true("seattle" %in% x$city)
  expect_true("boston" %in% x$city)
  expect_true("washington_dc" %in% x$city)
  expect_true("kansas_city" %in% x$city)
  expect_true("gainesville" %in% x$city)
  expect_true("houston" %in% x$city)
  expect_true("hartford" %in% x$city)
  expect_true("new_orleans" %in% x$city)
  expect_true("fort_lauderdale" %in% x$city)
  expect_true("naperville" %in% x$city)
  expect_true("denver" %in% x$city)
  expect_true("detroit" %in% x$city)
  expect_true("indianapolis" %in% x$city)
  expect_true("minneapolis" %in% x$city)
  expect_true("grand_rapids" %in% x$city)
  expect_true("pittsburgh" %in% x$city)
  expect_true("san_antonio" %in% x$city)
  expect_true("new_york" %in% x$city)
})

test_that("incident source registry returns source coverage metadata", {
  x <- list_incident_sources("cincinnati")
  expect_s3_class(x, "tbl_df")
  expect_true(all(c(
    "source_id", "dataset_id", "active_from", "active_to",
    "source_status", "source_note", "scope_status", "scope_note"
  ) %in% names(x)))
  expect_equal(nrow(x), 2)
  expect_true(any(x$source_id == "cincinnati_incidents_current"))

  seattle <- list_incident_sources("seattle")
  expect_equal(nrow(seattle), 1)
  expect_identical(seattle$dataset_id[[1]], "tazs-3rd5")

  cleveland <- list_incident_sources("cleveland")
  expect_equal(nrow(cleveland), 2)
  expect_true(any(cleveland$dataset_id == "e15e8989c83e4cbd841fb171a6c62f68"))
  expect_true(any(cleveland$dataset_id == "c749e34199c1425cbbc5959308658ec3"))
  expect_identical(
    dplyr::filter(cleveland, .data$source_id == "cleveland_incidents_current")$scope_status[[1]],
    "highest_offense_only"
  )

  rochester <- list_incident_sources("rochester")
  expect_equal(nrow(rochester), 1)
  expect_identical(rochester$scope_status[[1]], "part_i_only")

  boston <- list_incident_sources("boston")
  expect_equal(nrow(boston), 1)
  expect_identical(boston$provider[[1]], "arcgis")
  expect_identical(boston$dataset_id[[1]], "d42bd4040bca419a824ae5062488aced")

  dc <- list_incident_sources("washington_dc")
  expect_equal(nrow(dc), 19)
  expect_true(any(dc$source_id == "dc_crime_2026"))
  expect_true(any(dc$source_id == "dc_crime_2008"))

  kc <- list_incident_sources("kansas_city")
  expect_equal(nrow(kc), 12)
  expect_true(any(kc$dataset_id == "f7wj-ckmw"))
  expect_true(any(kc$dataset_id == "kbzx-7ehe"))

  gainesville <- list_incident_sources("gainesville")
  expect_equal(nrow(gainesville), 1)
  expect_identical(gainesville$dataset_id[[1]], "gvua-xt9q")

  hartford <- list_incident_sources("hartford")
  expect_equal(nrow(hartford), 1)
  expect_identical(hartford$provider[[1]], "arcgis")
  expect_identical(hartford$dataset_id[[1]], "4bc28c820ebd45df8a62feae6dc8822d")
  expect_identical(hartford$source_status[[1]], "rolling_window")

  houston <- list_incident_sources("houston")
  expect_equal(nrow(houston), 4)
  expect_true(all(houston$provider == "arcgis"))
  expect_true(any(houston$dataset_id == "hpd_nibrs_recent_crime_reports_layer_0"))
  expect_true(any(houston$dataset_id == "hpd_nibrs_recent_crime_reports_layer_3"))
  expect_true(all(houston$source_status == "rolling_window"))

  providence <- list_incident_sources("providence")
  expect_equal(nrow(providence), 1)
  expect_identical(providence$source_status[[1]], "rolling_window")

  nola <- list_incident_sources("new_orleans")
  expect_equal(nrow(nola), 16)
  expect_true(any(nola$dataset_id == "es9j-6y5d"))
  expect_true(any(nola$dataset_id == "hp7u-i9hf"))
  expect_true(all(nola$scope_status == "calls_for_service"))

  fort_lauderdale <- list_incident_sources("fort_lauderdale")
  expect_equal(nrow(fort_lauderdale), 1)
  expect_identical(fort_lauderdale$provider[[1]], "socrata")
  expect_identical(fort_lauderdale$dataset_id[[1]], "4gb7-f88q")
  expect_identical(fort_lauderdale$source_status[[1]], "historical_capped")

  naperville <- list_incident_sources("naperville")
  expect_equal(nrow(naperville), 2)
  expect_true(any(naperville$dataset_id == "584e8dcdd12649fe97a1ddb774705092"))
  expect_true(any(naperville$dataset_id == "8eb05471b7d740b8b5b610060cef6118"))
  expect_true(any(naperville$source_status == "historical_capped"))

  denver <- list_incident_sources("denver")
  expect_equal(nrow(denver), 1)
  expect_identical(denver$provider[[1]], "arcgis")
  expect_identical(denver$dataset_id[[1]], "1e080d3ce2ae4e2698745a0d02345d4a")
  expect_identical(denver$source_status[[1]], "rolling_window")

  detroit <- list_incident_sources("detroit")
  expect_equal(nrow(detroit), 1)
  expect_identical(detroit$provider[[1]], "arcgis")
  expect_identical(detroit$dataset_id[[1]], "8e532daeec1149879bd5e67fdd9c8be0")
  expect_identical(detroit$scope_status[[1]], "offense_rows")

  indy <- list_incident_sources("indianapolis")
  expect_equal(nrow(indy), 1)
  expect_identical(indy$provider[[1]], "arcgis")
  expect_identical(indy$dataset_id[[1]], "2017ad323ea444ea92590254f08629a9")

  minneapolis <- list_incident_sources("minneapolis")
  expect_equal(nrow(minneapolis), 1)
  expect_identical(minneapolis$provider[[1]], "arcgis")
  expect_identical(minneapolis$dataset_id[[1]], "e83a2845d2384759a0a08614fc3fe812")
  expect_identical(minneapolis$source_status[[1]], "rolling_window")

  grand_rapids <- list_incident_sources("grand_rapids")
  expect_equal(nrow(grand_rapids), 1)
  expect_identical(grand_rapids$provider[[1]], "arcgis")
  expect_identical(grand_rapids$dataset_id[[1]], "a7dc3002434d4ab6869feb02ec9f7a30")

  pittsburgh <- list_incident_sources("pittsburgh")
  expect_equal(nrow(pittsburgh), 1)
  expect_identical(pittsburgh$provider[[1]], "ckan")
  expect_identical(pittsburgh$dataset_id[[1]], "bd41992a-987a-4cca-8798-fbe1cd946b07")
  expect_identical(pittsburgh$source_status[[1]], "rolling_window")

  san_antonio <- list_incident_sources("san_antonio")
  expect_equal(nrow(san_antonio), 1)
  expect_identical(san_antonio$provider[[1]], "ckan")
  expect_identical(san_antonio$dataset_id[[1]], "f36bb931-8fb4-481c-83d9-a3589108bb20")

  nyc <- list_incident_sources("new_york")
  expect_equal(nrow(nyc), 2)
  expect_true(any(nyc$dataset_id == "qgea-i56i"))
  expect_true(any(nyc$dataset_id == "5uac-w243"))
})

test_that("source windows are correctly intersected", {
  source <- tidycops:::get_incident_city_spec("cincinnati")$sources[[2]]

  window <- tidycops:::intersect_source_window(
    start_date = as.Date("2024-06-01"),
    end_date = as.Date("2024-06-10"),
    active_from = source$active_from,
    active_to = source$active_to
  )

  expect_identical(window$start_date, as.Date("2024-06-03"))
  expect_identical(window$end_date, as.Date("2024-06-10"))
})

test_that("nyc historic/current source windows hand off cleanly", {
  nyc_sources <- tidycops:::get_incident_city_spec("new_york")$sources

  historic_window <- tidycops:::intersect_source_window(
    start_date = as.Date("2026-01-01"),
    end_date = as.Date("2026-01-15"),
    active_from = nyc_sources[[1]]$active_from,
    active_to = nyc_sources[[1]]$active_to
  )

  current_window <- tidycops:::intersect_source_window(
    start_date = as.Date("2026-01-01"),
    end_date = as.Date("2026-01-15"),
    active_from = nyc_sources[[2]]$active_from,
    active_to = nyc_sources[[2]]$active_to
  )

  expect_null(historic_window)
  expect_identical(current_window$start_date, as.Date("2026-01-01"))
  expect_identical(current_window$end_date, as.Date("2026-01-15"))
})
