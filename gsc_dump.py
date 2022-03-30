#!/usr/bin/python3

import csv
import datetime
import logging
import time

import googleapiclient.discovery
import httplib2
from absl import app, flags
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from oauth2client.client import OAuth2Credentials, OAuth2WebServerFlow

import checkpoint

FLAGS = flags.FLAGS

flags.DEFINE_string("csv_file_prefix", "daily-searchconsole",
                    "CSV file to dump everything to.")

flags.DEFINE_string("start_date", "2021-01-01",
                    "ISO8601 format date specifying which day to start fetching data for.")

flags.DEFINE_string("end_date", "2021-01-01",
                    "ISO8601 format date specifying which day to start fetching data up to (inclusive).")

flags.DEFINE_string("service_account_file", "",
                    "Location of service account credentials JSON file. "
                    "Only relevant for service account authentication.")

flags.DEFINE_integer("max_retries_on_error", 2,
                     "If an error occurs, try these many times in addition to the first try.")

flags.DEFINE_integer("retry_backoff_seconds", 5,
                     "Wait these many seconds before retrying if an error occurs.")

flags.DEFINE_string("list_of_sites",
                    "https://en.wikipedia.org/,https://en.m.wikipedia.org/",
                    "A comma-separated list of sites to look up data for. "
                    "Please note that these will all be unified into a single dump.")

flags.DEFINE_bool("query_mode",
                  True,
                  "Whether or not to include the query and page as dimensions.")


SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def auth_with_serviceaccount():
    creds = service_account.Credentials.from_service_account_file(
        FLAGS.service_account_file, scopes=SCOPES)
    return googleapiclient.discovery.build("searchconsole", "v1", credentials=creds)


CSV_HEADER_FIELDS = ["date", "country",
                     "device", "clicks", "impressions", "ctr", "position"]

CSV_HEADER_FIELDS_QUERY_MODE = ["query", "date", "page", "country",
                                "device", "clicks", "impressions", "ctr", "position"]

KEY_ROWS = "rows"

# GSC provides data that it calls "Fresh" v/s "Final". Fresh data can change over
# time whereas final data is unlikely to. The API allows requesting either. However
# querying for a time range very close to the present might actually yield no data in
# the final state.
GSC_FINAL_DATA_DELAY_DAYS = 5

# GSC holds data up to 16 months in the past.
GSC_TOTAL_TIME_WINDOW_DAYS = 16 * 30

def escape_slashes(s):
    return s.replace("\\", "\\\\")


def import_sc_data(startdate, enddate, service, sites, cp):
    # Let's break this down into per-day sessions so we get the top queries
    # on a per-day basis.
    logging.info("Importing %s...", sites)
    currdate = startdate
    while currdate <= enddate:
        currday = currdate.isoformat()[0:10]
        cp.write_checkpoint(currdate)
        for mode in ["query", "noquery"]:
            writer = csv.writer(
                open("%s-%s-%s.csv" % (FLAGS.csv_file_prefix, mode, currday), "w"), csv.QUOTE_NONNUMERIC)
            writer.writerow(
                CSV_HEADER_FIELDS_QUERY_MODE if mode == "query" else CSV_HEADER_FIELDS)
            for site in sites:
                write_site_data(currday, service, site, writer, mode == "query")
        currdate += datetime.timedelta(days=1)


def query_with_retries(service, site, request):
    error_budget = FLAGS.max_retries_on_error
    while error_budget >= 0:
        try:
            return service.searchanalytics().query(siteUrl=site, body=request).execute()
        except HttpError as e:
            logging.warning(
                "Got an http error: %s. %d Retries left.", e, error_budget)
            if error_budget != 0:  # No point sleeping before failing.
                time.sleep(FLAGS.retry_backoff_seconds)
        error_budget -= 1
    raise RuntimeError("Still failing after after max_retries_on_error. ")


DIMENSIONS_WITH_QUERY = ["QUERY", "DATE", "PAGE", "COUNTRY", "DEVICE"]
DIMENSIONS = ["DATE", "COUNTRY", "DEVICE"]


def write_site_data(day, service, site, writer, is_query_mode):
    dim = DIMENSIONS_WITH_QUERY if is_query_mode else DIMENSIONS
    request = {
        "startDate": day,
        "endDate": day,
        "dimensions": dim,
        "dataState": "FINAL",
        "rowLimit": 25000,
        "type": "WEB",
    }
    last_row = 0
    while True:
        logging.info("%s for %s, %s", day, site, "query" if is_query_mode else "noquery")
        response = query_with_retries(service, site, request)
        if "rows" not in response or len(response[KEY_ROWS]) == 0:
            # This is how the API indicates that there are no more pages.
            logging.info("No more pages for %s, stopping.", day)
            break  # Successful end-of-pages response.
        for row in response[KEY_ROWS]:
            # Preserves the order in the request.
            keys = row["keys"]
            if is_query_mode:
                output_row = [
                    escape_slashes(keys[0]),
                    keys[1],
                    escape_slashes(keys[2]),
                    keys[3],
                    keys[4],
                    row["clicks"],
                    row["impressions"],
                    row["ctr"],
                    row["position"]]
            else:
                output_row = [
                    keys[0],
                    keys[1],
                    keys[2],
                    row["clicks"],
                    row["impressions"],
                    row["ctr"],
                    row["position"]]
            writer.writerow(output_row)
        logging.info("Got %s responses, checking next page...",
                     len(response[KEY_ROWS]))
        last_row += len(response[KEY_ROWS])
        request["startRow"] = last_row

def run_site_check(service):
    site_list = service.sites().list().execute()
    logging.info("Got a list of sites, looking for %s in them...",
                 FLAGS.list_of_sites)
    selected_sites = set(FLAGS.list_of_sites.split(","))
    available_sites = set([s["siteUrl"] for s in site_list["siteEntry"]])
    unavailable_sites = selected_sites - available_sites
    if unavailable_sites:
        logging.fatal("ERROR: Some sites not available: %s", unavailable_sites)
    logging.info(
        "All requested sites available in search console. Proceeding...")
    return selected_sites

def main(argv):
    logging.info("Authenticating...")
    service = auth_with_serviceaccount()
    logging.info("Authenticated. Looking up sites ...")
    selected_sites = run_site_check(service)
    startdate = datetime.datetime.fromisoformat(FLAGS.start_date)
    enddate = datetime.datetime.fromisoformat(FLAGS.end_date)
    if (enddate - startdate).days < 0:
        logging.fatal("End date cannot be before start date.")
        return -1
    # Let's see if we need to really start from the start date.
    cp = checkpoint.Checkpoint(startdate, enddate)
    new_startdate = datetime.datetime.fromisoformat(cp.nextdate())
    import_sc_data(new_startdate, enddate, service, selected_sites, cp)
    cp.delete_checkpoint()

if __name__ == "__main__":
    app.run(main)
