# Packages
suppressMessages({
  library(dplyr)
  library(DBI)
  library(duckdb)
  library(inmetrss)
  library(cli)
  library(lubridate)
})

cli_h1("Get INMET alerts procedure")
cli::cli_alert_info("Job start: {lubridate::now()}")

# Get last feed
cli_alert("Retrieving INMET RSS feed...")
res <- parse_feed()
cli_alert_success("Done!")

# Check number of rows
if (nrow(res) < 0) {
  cli::cli_abort("There is no data in the feed.")
}

# Database connection
cli_alert("Connecting to database...")
con <- dbConnect(duckdb(), "inmetrss.duckdb")
cli_alert_success("Done!")

# First write. Keep it commented.
# dbWriteTable(conn = con, name = "alerts", value = res)

cli_alert("Fetching already available IDs in database...")
database_ids <- tbl(con, "alerts") |>
  select(identifier) |>
  pull(identifier)
cli_alert_success("Done!")
cli_alert_info("There are {length(database_ids)} alerts in the database.")

cli_alert("Filtering new alerts...")
new_data <- res |>
  filter(!(identifier %in% database_ids))
cli_alert_success("Done!")

if (nrow(new_data) > 0) {
  cli_alert("Writing new alerts to database ({nrow(new_data)} alerts)...")
  dbWriteTable(conn = con, name = "alerts", value = new_data, append = TRUE)
} else {
  cli_alert_warning("There is no new alerts to write in the database.")
}

# Database disconnect
cli_alert("Disconnecting database...")
dbDisconnect(conn = con)
cli_alert_success("Done!")

cli_alert_info("Job end: {now()}")
cli_h1("END")
