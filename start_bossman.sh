#!/usr/bin/env bash

set -u

BOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$BOT_DIR/.venv/bin/python"
RUN_FILE="$BOT_DIR/run.py"
PID_FILE="$BOT_DIR/bot.pid"
OUT_LOG="$BOT_DIR/bot.out.log"
ERR_LOG="$BOT_DIR/bot.err.log"

if [[ ! -x "$PYTHON" ]]; then
    echo "Could not find the bot virtual environment at $PYTHON"
    exit 1
fi

if [[ -f "$PID_FILE" ]]; then
    existing_pid="$(tr -cd '0-9' < "$PID_FILE")"
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
        echo "BOSSMAN is already running with PID $existing_pid."
        echo "You can close this window."
        exit 0
    fi
    rm -f "$PID_FILE"
fi

existing_pid="$(pgrep -u "$(id -u)" -f "[p]ython.*${RUN_FILE}" | head -n 1 || true)"
if [[ -n "$existing_pid" ]]; then
    echo "$existing_pid" > "$PID_FILE"
    echo "BOSSMAN is already running with PID $existing_pid."
    echo "You can close this window."
    exit 0
fi

touch "$OUT_LOG" "$ERR_LOG"
start_line=$(( $(wc -l < "$OUT_LOG") + 1 ))

cd "$BOT_DIR" || exit 1
nohup "$PYTHON" -u "$RUN_FILE" >> "$OUT_LOG" 2>> "$ERR_LOG" < /dev/null &
bot_pid=$!
echo "$bot_pid" > "$PID_FILE"

echo "Starting BOSSMAN with PID $bot_pid..."

for _ in {1..45}; do
    if ! kill -0 "$bot_pid" 2>/dev/null; then
        echo "BOSSMAN stopped during startup. Recent errors:"
        tail -n 20 "$ERR_LOG"
        rm -f "$PID_FILE"
        exit 1
    fi

    if tail -n "+$start_line" "$OUT_LOG" | grep -Fq "EasyOCR models loaded and ready."; then
        echo "BOSSMAN is running and OCR is ready."
        echo "You can close this window."
        exit 0
    fi

    sleep 1
done

echo "BOSSMAN is running. OCR is still warming up in the background."
echo "You can close this window."
