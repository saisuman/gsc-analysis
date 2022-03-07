import json
import csv
import datetime
import logging
import time
import os

import googleapiclient.discovery
import httplib2
from absl import app, flags
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from oauth2client.client import OAuth2Credentials, OAuth2WebServerFlow

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

flags.DEFINE_string("oauth_credential_cache_file", "ccache.json",
                    "Location of OAuth2 credential cache JSON file. Only relevant "
                    "for OAuth2 flows.")

flags.DEFINE_bool("use_service_account", False,
                  "Use service account authentication instead of an OAuth2 flow.")

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

flags.DEFINE_string(
    "client_id", "", "The client ID to use with OAuth2 flow authentication.")

flags.DEFINE_string("client_secret", "",
                    "The secret to use with OAuth2 flow authentication.")

flags.DEFINE_string("redirect_uri", "urn:ietf:wg:oauth:2.0:oob",
                    "The redirect URI to use with OAuth2 flow authentication.")

flags.DEFINE_string("checkpoint_filename", "checkpoint",
                    "A checkpoint file which records progress so that the dump "
                    "can resume upon crashing.")


SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


CP_STARTDATE = 'startdate'
CP_ENDDATE = 'enddate'
CP_NEXTDATE = 'nextdate'

def read_checkpoint(startdate, enddate):
    start_str, end_str = startdate.isoformat()[0:10], enddate.isoformat()[0:10]
    try:
        cstate = json.loads(open(FLAGS.checkpoint_filename, "r").read())
        if cstate[CP_STARTDATE] == start_str and cstate[CP_ENDDATE] == end_str:
            logging.info("Found a checkpoint: %s", cstate)
            return cstate
        else:
            logging.warning("Previous checkpoint was for a different time range.")
            delete_checkpoint()
    except FileNotFoundError:
        logging.info("No existing checkpoint.")
    return {
            CP_STARTDATE: start_str,
            CP_ENDDATE: end_str,
            CP_NEXTDATE: start_str
        }

def delete_checkpoint():
    logging.info("Deleting checkpoint.")
    os.unlink(FLAGS.checkpoint_filename)


def write_checkpoint(cstate):
    logging.info("Writing %s to checkpoint.", cstate)
    open(FLAGS.checkpoint_filename, 'w').write(json.dumps(cstate))


def auth_with_serviceaccount():
    creds = service_account.Credentials.from_service_account_file(
        FLAGS.service_account_file, scopes=SCOPES)
    return googleapiclient.discovery.build("searchconsole", "v1", credentials=creds)


def read_cached_credentials(http):
    try:
        contents = open(FLAGS.oauth_credential_cache_file, "r").read()
        credentials = OAuth2Credentials.from_json(contents)
        if credentials.access_token_expired:
            credentials.refresh(http)
        return credentials
    except Exception as e:
        print("Couldn't read cache.", e)
        return None


def auth_with_browser():
    http = httplib2.Http()
    credentials = read_cached_credentials(http)
    if not credentials:
        flow = OAuth2WebServerFlow(
            FLAGS.client_id, FLAGS.client_secret, SCOPES, FLAGS.redirect_uri)
        authorize_url = flow.step1_get_authorize_url()
        print("Go to the following link in your browser: ", authorize_url)
        code = input("Enter verification code: ").strip()
        credentials = flow.step2_exchange(code)
        open(FLAGS.oauth_credential_cache_file,
             "w").write(credentials.to_json())

    # Create an httplib2.Http object and authorize it with our credentials
    http = credentials.authorize(http)
    return googleapiclient.discovery.build("searchconsole", "v1", http=http)


CSV_HEADER_FIELDS = ["date", "country",
                     "device", "clicks", "impressions", "ctr", "position"]

CSV_HEADER_FIELDS_QUERY_MODE = ["query", "date", "page", "country",
                                "device", "clicks", "impressions", "ctr", "position"]

KEY_ROWS = "rows"


def escape_slashes(s):
    return s.replace("\\", "\\\\")


def import_sc_data(startdate, enddate, service, sites, cstate):
    # Let's break this down into per-day sessions so we get the top queries
    # on a per-day basis.
    logging.info("Importing %s...", sites)
    currdate = startdate
    while currdate <= enddate:
        currday = currdate.isoformat()[0:10]
        cstate[CP_NEXTDATE] = currday
        write_checkpoint(cstate)
        writer = csv.writer(
            open("%s-%s.csv" % (FLAGS.csv_file_prefix, currday), "w"), csv.QUOTE_NONNUMERIC)
        writer.writerow(
            CSV_HEADER_FIELDS_QUERY_MODE if FLAGS.query_mode else CSV_HEADER_FIELDS)
        for site in sites:
            write_site_data(currday, service, site, writer)
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


def write_site_data(day, service, site, writer):
    dim = DIMENSIONS_WITH_QUERY if FLAGS.query_mode else DIMENSIONS
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
        logging.info("Querying day: %s for %s", request, site)
        response = query_with_retries(service, site, request)
        if "rows" not in response or len(response[KEY_ROWS]) == 0:
            # This is how the API indicates that there are no more pages.
            logging.info("No more pages for %s, stopping.", day)
            break  # Successful end-of-pages response.
        for row in response[KEY_ROWS]:
            # Preserves the order in the request.
            keys = row["keys"]
            if FLAGS.query_mode:
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


def main(argv):
    logging.info("Authenticating...")
    if FLAGS.use_service_account:
        service = auth_with_serviceaccount()
    else:
        service = auth_with_browser()
    logging.info("Authenticated. Looking up sites ...")
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
    startdate = datetime.datetime.fromisoformat(FLAGS.start_date)
    enddate = datetime.datetime.fromisoformat(FLAGS.end_date)
    if (enddate - startdate).days < 0:
        logging.fatal("End date cannot be before start date.")
        return -1
    # Let's see if we need to really start from the start date.
    cstate = read_checkpoint(startdate, enddate)
    new_startdate = datetime.datetime.fromisoformat(cstate["nextdate"])
    import_sc_data(new_startdate, enddate, service, selected_sites, cstate)
    delete_checkpoint()


if __name__ == "__main__":
    app.run(main)
