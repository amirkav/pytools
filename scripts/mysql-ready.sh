#!/bin/bash

# bash script to verify that mysql is ready to accept connections

shopt -s extglob

mysql_host="${MYSQL_HOST}"
mysql_port="${MYSQL_TCP_PORT}"
mysql_user="${MYSQL_USER}"
mysql_pwd="${MYSQL_PWD}"

interval_secs=1
while [[ "$1" == -* ]]; do
    case "$1" in
      --help)
        echo "Usage: mysql-ready [OPTIONS]" >&2
        echo >&2
        echo "Options:" >&2
        echo "    --help" >&2
        echo "        This help message." >&2
        echo "    -q, --quiet" >&2
        echo "        Do not print messages." >&2
        echo "    -w, --wait TIMEOUT" >&2
        echo "    -w, --wait TIMEOUT/INTERVAL" >&2
        echo "        Wait for MySQL to become available, for up to TIMEOUT seconds," >&2
        echo "        retrying every INTERVAL seconds (default: 1)." >&2
        echo "    -h, --host HOST" >&2
        echo "        Hostname of MySQL server to connect to." >&2
        echo "    -P, --port PORT" >&2
        echo "        TCP port of MySQL server to connect to." >&2
        echo "    -u, --user USER" >&2
        echo "        MySQL user to connect as." >&2
        echo "    -p, --password PASSWORD" >&2
        echo "        Password to authenticate with." >&2
        echo "    --version" >&2
        echo "        Print version." >&2
        exit 0
        ;;
      --version)
        echo "mysql-ready 1.0.0"
        exit 0
        ;;
      -q|--quiet)
        quiet=1
        ;;
      -w|--wait)
        do_wait=1
        shift
        if [[ "$1" == +([0-9])/+([0-9]) ]]; then
            timeout_secs="${1%%/*}"   # first part
            interval_secs="${1##*/}"  # last part
        elif [[ "$1" == +([0-9]) ]]; then
            timeout_secs="$1"
        else
            echo "ERROR: Invalid timeout/interval: $1" >&2
            exit 1
        fi
        ;;
      -h|--host)
        shift
        mysql_host="$1"
        ;;
      -P|--port)
        shift
        mysql_port="$1"
        ;;
      -u|--user)
        shift
        mysql_user="$1"
        ;;
      -p|--password)
        shift
        mysql_pwd="$1"
        ;;
    esac
    shift
done

try_connect () {
    mysql \
        ${mysql_host:+-h "${mysql_host}"} \
        ${mysql_port:+-P "${mysql_port}"} \
        ${mysql_user:+-u "${mysql_user}"} \
        ${mysql_pwd:+-p"${mysql_pwd}"} \
        -e exit >/dev/null 2>&1
}

if [ "${do_wait}" ]; then
    # Retry until timeout:
    if try_connect; then
        [ "${quiet}" ] || echo "MySQL is running." >&2
        exit 0
    fi
    [ "${quiet}" ] || echo -n "Waiting for MySQL to start: " >&2
    for (( i = 0; i < ${timeout_secs}; i += ${interval_secs} )); do
        if try_connect; then
            [ "${quiet}" ] || echo " done." >&2
            exit 0
        else
            sleep "${interval_secs}"
            [ "${quiet}" ] || echo -n "." >&2
        fi
    done
    [ "${quiet}" ] || echo " giving up after ${timeout_secs}s." >&2
    exit 1
else
    # Try once:
    if try_connect; then
        [ "${quiet}" ] || echo "yes"
        exit 0
    else
        [ "${quiet}" ] || echo "no"
        exit 1
    fi
fi
