#!/bin/sh
# librespot's --onevent: power the DAC on whenever playback starts.
case $PLAYER_EVENT in
    playing) echo "$(date) $PLAYER_EVENT" >> ./dacon.log; ./dacon 70 >> ./dacon.log 2>&1 ;;
esac
