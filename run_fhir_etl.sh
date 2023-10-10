#!/usr/bin/env bash

JOBNAME="FHIR ETL"
START=$(date +%s)
TIMESTAMP=$(date -d @$START +%y%m%d-%H%M)

# set repository base path
BASE_PATH="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# read log directory from 'config/fhir_cfg.yml'
LOG_DIR="$(grep -Po "log_dir:\s+[\"\']*\K[^\"\';]+" "$BASE_PATH/config/fhir_cfg.yml")"
# prepend BASE_PATH if LOG_DIR path is relative
if [[ ${LOG_DIR:0:1} != "/" ]]; then
  LOG_DIR="$BASE_PATH/$LOG_DIR"
fi
# create log dir if not yet existing
mkdir -p $LOG_DIR
# log file
LOG_FILE="$LOG_DIR/FHIR_ETL_$TIMESTAMP.log"


# run script
echo -e "$JOBNAME started at $TIMESTAMP\n" | tee $LOG_FILE

# supply repository base path as argument to set correct working directory
Rscript "$BASE_PATH/code/fhir_etl.R" $BASE_PATH 2>&1 | tee -a $LOG_FILE

echo -e "\nFinished $JOBNAME: $(date +%y%m%d-%H%M)" | tee -a $LOG_FILE
echo "duration: $(date -d "0 $(($(date +%s)-$START)) sec" +%H:%M:%S)" | tee -a $LOG_FILE

