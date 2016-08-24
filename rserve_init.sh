#!/usr/bin/env bash

PIDFILENAME=rserve.pid

if [ -w /var/run ]; then
    PIDFILE=/var/run/${PIDFILENAME}
else
    PIDFILE=~/.rserve/${PIDFILENAME}
    if [ ! -d ~/.rserve ]; then
        mkdir ~/.rserve
    fi
fi

case $1 in

    start)

        if [ ! -s ${PIDFILE} ]; then
            R CMD Rserve --vanilla --slave --RS-conf rserve.conf --RS-pidfile ${PIDFILE}
        else
            echo "PID File ${PIDFILE} exists. Perhaps Rserve is already running."
        fi

        ;;

    stop)

        if  [ -s ${PIDFILE} ]; then

            PID=`cat ${PIDFILE}`

            echo "Shutting down Rserve. PID ${PID}"

            kill -s TERM ${PID}

        else

            echo "PID File not found. Perhaps Rserve is not running."

        fi
        ;;

    *)
        echo "Usage: rserve.sh <start|stop>"
        ;;

esac
