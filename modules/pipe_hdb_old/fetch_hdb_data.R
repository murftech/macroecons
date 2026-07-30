# fetch_hdb_data.R
# Pulls HDB resale flat prices (Jan 2017 onwards) from data.gov.sg API
# Returns a dataframe identical to reading the CSV manually
#
# Usage:
#   source("scripts/fetch_hdb_data.R")
#   data_2017 <- fetch_hdb_data()

library(httr2)
library(readr)

# testers
dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
max_poll_attempts = 10
poll_interval_sec = 3

fetch_hdb_data <- function(
    dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
    max_poll_attempts = 10,
    poll_interval_sec = 3
) {
  base_url <- "https://api-open.data.gov.sg/v1/public/api/datasets"

  # Step 1: Initiate download
  cat("Initiating download...\n")
  request(paste0(base_url, "/", dataset_id, "/initiate-download")) |>
    req_method("GET") |>
    req_headers("Content-Type" = "application/json") |>
    req_error(is_error = \(resp) FALSE) |>  # handle errors manually
    req_perform()

  # Step 2: Poll until a download URL is returned
  cat("Polling for download URL")
  poll_url <- paste0(base_url, "/", dataset_id, "/poll-download")
  download_url <- NULL

  for (i in seq_len(max_poll_attempts)) {
    cat(".")
    resp <- request(poll_url) |>
      req_method("GET") |>
      req_headers("Content-Type" = "application/json") |>
      req_perform()

    body <- resp_body_json(resp)

    url_candidate <- body$data$url
    if (!is.null(url_candidate) && nchar(url_candidate) > 0) {
      download_url <- url_candidate
      break
    }

    Sys.sleep(poll_interval_sec)
  }
  cat("\n")

  if (is.null(download_url)) {
    stop("API did not return a download URL after ", max_poll_attempts, " attempts.")
  }

  cat("Download URL obtained. Reading data...\n")

  # Step 3: Read CSV directly into R from the returned URL
  df <- read_csv(download_url, show_col_types = FALSE)

  cat("Done. Loaded", format(nrow(df), big.mark = ","), "rows and",
      ncol(df), "columns.\n")

  df
}
