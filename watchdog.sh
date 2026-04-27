#!/bin/bash
if ! pgrep -f "bot.py run" > /dev/null; then
    echo "$(date) - bot died, restarting" >> /home/74h2hfpyj79x/weatherbot/logs/watchdog.log
    sudo systemctl restart weatherbot 2>/dev/null || /home/74h2hfpyj79x/weatherbot/venv/bin/python -u /home/74h2hfpyj79x/weatherbot/bot.py run --paper-on --signal-on --live-off &
fi
