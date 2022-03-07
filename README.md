# gsc-analysis
Some code that helps you download and process data from Google's Search Console API.

Here's how to get started:

1. Use pyvenv to set up the libraries you'll need.

$ pyvenv create gsc-analysis
$ pyvenv shell gsc-analysis
Copied source /home/saisuman/.local/share/pyvenv/gsc-analysis/bin/activate to clipboard.
$ source /usr/local/bin/share/pyvenv/gsc-analysis/bin/activate
(gsc-analysis) $ 

2. Install all requried packages through pip3.

(gsc-analysis) $ cat requirements.txt | xargs pip3 install

3. Run this:

(gsc-analysis) $ python3 gsc_dump.py  \
  --client_secret=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  \
  --client_id=757101436847-tvr9t338n07p716gkl1o521ev5vt1kmc.apps.googleusercontent.com  \
  --start_date=2022-01-01  \
  --end_date=2022-01-09  \
  --csv_file_prefix=dump/query-daily-searchconsole  \
  --noquery_mode

The job will retry in case of network failures up to a configurable number of times
per error. It is safe to kill and restart the pipeline with the same arguments; there
is a checkpoint-and-restart mechanism built in.

