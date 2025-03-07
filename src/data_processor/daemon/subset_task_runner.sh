#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate data_processor_env

python -m data_processor.tools.subsetter.subsetter --variables "$VARIABLES" --in-path "$IN_PATH" \
  --out-path $OUT_PATH --output-name-pattern $OUTPUT_NAME_PATTERN --output-format $OUTPUT_FORMAT \
  --lon-min $LON_MIN --lon-max $LON_MAX --lat-min $LAT_MIN --lat-max $LAT_MAX \
  --start-year $START_YEAR --start-month $START_MONTH --start-day $START_DAY \
  --end-year $END_YEAR --end-month $END_MONTH --end-day $END_DAY
