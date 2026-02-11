#!/bin/bash

APP="main.py"
LOG="billing-etl.log"

start() {
  echo "Starting billing-etl..."

  PIDS=$(ps -ef | grep "$APP" | grep -v grep | awk '{print $2}')

  if [ -n "$PIDS" ]; then
    echo "Already running! PID(s): $PIDS"
    exit 0
  fi

  nohup python3 $APP > $LOG 2>&1 &

  sleep 1

  NEW_PID=$(ps -ef | grep "$APP" | grep -v grep | awk '{print $2}')

  if [ -n "$NEW_PID" ]; then
    echo "Started successfully. PID: $NEW_PID"
    echo "Log file: $LOG"
  else
    echo "Start failed. Check log: $LOG"
  fi
}

stop() {
  echo "Stopping billing-etl..."

  PIDS=$(ps -ef | grep "$APP" | grep -v grep | awk '{print $2}')

  if [ -z "$PIDS" ]; then
    echo "No process found."
    exit 0
  fi

  echo "Found PID(s): $PIDS"
  echo "Sending SIGTERM..."
  kill -15 $PIDS

  sleep 5

  for PID in $PIDS; do
    if ps -p $PID > /dev/null 2>&1; then
      echo "PID $PID still running, sending SIGKILL..."
      kill -9 $PID
    else
      echo "PID $PID stopped successfully."
    fi
  done

  echo "Stopped."
}

status() {
  PIDS=$(ps -ef | grep "$APP" | grep -v grep)

  if [ -z "$PIDS" ]; then
    echo "billing-etl is NOT running."
  else
    echo "billing-etl is running:"
    echo "$PIDS"
  fi
}

log() {
  if [ -f "$LOG" ]; then
    tail -f $LOG
  else
    echo "Log file not found: $LOG"
  fi
}

restart() {
  stop
  sleep 1
  start
}

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    restart
    ;;
  status)
    status
    ;;
  log)
    log
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|log}"
    exit 1
    ;;
esac
