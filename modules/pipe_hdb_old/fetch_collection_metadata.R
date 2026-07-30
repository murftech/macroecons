# fetch_collection_metadata.R
# Pulls collection metadata from data.gov.sg v2 API
# Equivalent to: GET /v2/public/api/collections/{collection_id}/metadata
#
# Usage:
#   source("scripts/fetch_collection_metadata.R")
#   meta <- fetch_collection_metadata(189)

library(httr2)

fetch_collection_metadata <- function(collection_id = 189) {
  url <- paste0(
    "https://api-production.data.gov.sg/v2/public/api/collections/",
    collection_id,
    "/metadata"
  )

  cat("Fetching metadata for collection", collection_id, "...\n")

  resp <- request(url) |>
    req_method("GET") |>
    req_perform()

  body <- resp_body_json(resp)

  if (body$code != 0) {
    stop("API returned error: ", body$errorMsg)
  }

  meta <- body$data$collectionMetadata

  cat("Collection name  :", meta$name, "\n")
  cat("Managed by       :", meta$managedBy, "\n")
  cat("Last updated     :", meta$lastUpdatedAt, "\n")
  cat("Frequency        :", meta$frequency, "\n")
  cat("Child datasets   :", length(meta$childDatasets), "\n")

  meta
}
