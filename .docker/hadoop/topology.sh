#!/bin/bash
# Rack awareness: el NameNode llama a este script con IPs o nombres de DataNodes y espera un rack por cada uno.
# worker1 y worker2 comparten el rack-a; worker3 está solo en el rack-b.
for host in "$@"; do
  name=$(getent hosts "$host" | awk '{print $2}' | cut -d. -f1)
  name=${name:-$host}
  case "$name" in
    *orker1*|*orker2*) echo "/rack-a" ;;
    *orker3*) echo "/rack-b" ;;
    *) echo "/default-rack" ;;
  esac
done
