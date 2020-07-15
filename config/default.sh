#!/bin/bash

# Kill existing watchdog process.
pkill --exact tw-wd-default

# Start watchdog process.

# Activate Conda env.
# eval "$(command conda 'shell.bash' 'hook' 2> /dev/null)"

source /opt/anaconda3/etc/profile.d/conda.sh
conda activate twitter-watchdog

# Run program.
python3 ./twitter-watchdog/src/main.py -p tw-wd-default -c ./twitter-watchdog/config/default.config
