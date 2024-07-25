#!/usr/bin/env bash

CFG_PATH="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/config"

for CFG_FILE in "fhir_cfg" "fhir_search_cfg" "coverchild_cfg"; do
  cp "$CFG_PATH/EXAMPLE_${CFG_FILE}.yml" "$CFG_PATH/${CFG_FILE}.yml"
done

