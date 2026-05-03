#!/bin/bash
export PYTHONPATH=/home/74h2hfpyj79x/.local/lib/python3.11/site-packages:$PYTHONPATH
cd /home/74h2hfpyj79x/weatherbot
python3 -u bot.py run --paper-on > bot_output.log 2>&1 &
disown $!
echo "Bot started with PID: $!"
