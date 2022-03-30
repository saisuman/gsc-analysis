# gsc-analysis
Some code that helps you download and process data from Google's Search Console API.

Here's how to get started:

1. Use pyvenv to set up the libraries you'll need.  

`$ python3 -m venv .`
`$ source bin/activate`

2. Install all requried packages through pip3.  

`(gsc-analysis) $ cat requirements.txt | xargs pip3 install`

3. Run this to get all the data for the last 16 months.  

`(gsc-analysis) $ ./run.sh full`

4. Run the following in a daily cronjob.


You will need to obtain credentials from the Google API Console.

The job will retry in case of network failures up to a configurable number of times
per error. It is safe to kill and restart the pipeline with the same arguments; there
is a checkpoint-and-restart mechanism built in.

