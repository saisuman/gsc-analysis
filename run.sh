#!/bin/bash

# MODIFY THESE AS PER YOUR ENVIRONMENT.
BASE=/home/saisuman/dev/gsc-analysis
GSC_DUMP=${BASE}/gsc_dump.py
DUMP_DIR=${BASE}/dump
AUTH_FILE=${BASE}/wmf-sc-experiments-55c5b8f5a409.json
CHECKPOINT_FILE=${BASE}/checkpoint.json

echo "Today is: $(date -I)"
curr_ts=$(date +%s)
if [ "$1" == "full" ]
then
    startdate=$(date -I -d@$(echo "${curr_ts} - 86400 * 30 * 16" | bc))
    enddate=$(date -I -d@$(echo "${curr_ts} - 86400 * 5" | bc))
    echo "Sixteen months ago is: ${startdate}"
    echo "Today minus five days is: ${enddate}"
elif [ "$1" == "daily" ]
then
    startdate=$(date -I -d@$(echo "${curr_ts} - 86400 * 5" | bc))
    enddate=${startdate}
    echo "Today minus five days is: ${startdate}"
else
    echo "Unknown option."
    echo "Please specify one of 'daily' or 'full'."
    exit 1
fi

/usr/bin/python3 ${GSC_DUMP} \
    --service_account_file=${AUTH_FILE} \
    --start_date=${startdate} \
    --end_date=${enddate} \
    --csv_file_prefix=${DUMP_DIR}/searchconsole \
    --checkpoint_filename=${CHECKPOINT_FILE}
