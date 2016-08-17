#!/usr/bin/env bash

PIDFILE=/tmp/rserve.pid

case $1 in

    start)

        R CMD Rserve --vanilla --slave --RS-conf rserve.conf --RS-pidfile ${PIDFILE}
        ;;

    stop)

        if  [ -s ${PIDFILE}  ]; then

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