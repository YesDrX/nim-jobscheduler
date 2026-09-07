#!/bin/bash
trap 'kill -TERM 0' EXIT HUP INT TERM
sleep 3000 && echo ABC && exit 1 &
wait $!
ERR=$?
trap - EXIT HUP INT TERM
exit $ERR
