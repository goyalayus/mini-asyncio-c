#!/bin/sh
# load-test.sh — fire N parallel connections at the server, count replies.
#
# Usage:
#   ./load-test.sh [count] [host] [port]
#   default: 50 connections to 127.0.0.1:8080
#
# Each connection: send "req-X", expect a "hi" back.
# Prints OK count, FAIL count, and elapsed time.

COUNT=${1:-50}
HOST=${2:-127.0.0.1}
PORT=${3:-8080}

TMP=$(mktemp -d)
ok=0
fail=0
start=$(date +%s%N)

i=1
while [ "$i" -le "$COUNT" ]; do
  (
    out=$(head -c 100 /dev/zero | tr '\0' 'r' | nc -w 2 "$HOST" "$PORT" 2>/dev/null)
    case "$out" in
      hi*) echo "ok" > "$TMP/r$i" ;;

      *)   echo "fail:$(echo "$out" | tr -d '\n')" > "$TMP/r$i" ;;
    esac
  ) &
  i=$((i + 1))
done

wait

for f in "$TMP"/r*; do
  [ -e "$f" ] || continue
  case "$(cat "$f")" in
    ok)             ok=$((ok + 1)) ;;
    fail:*)         fail=$((fail + 1)) ;;
  esac
done

end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))

rm -rf "$TMP"

echo "----------------------------------------"
echo "connections : $COUNT"
echo "ok          : $ok"
echo "fail        : $fail"
echo "elapsed     : ${ms} ms"