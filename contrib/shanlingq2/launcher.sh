#!/bin/sh
# Installed as /mnt/mmc/.rockbox/rockbox: the boot hook runs it in Rockbox's
# place. Backgrounded so demo still starts and brings up Wi-Fi; librespot
# restarts until Wi-Fi is up (mDNS needs an interface) and after any exit.
cd /mnt/mmc/.rockbox
while :; do
    { date; cat /proc/asound/cards; } >> ./cards.log 2>&1
    ./librespot -n Q2 -b 160 --backend pipe --cache ./cache --disable-audio-cache \
        --onevent ./onevent.sh 2>> ./spot.log \
        | aplay -D plughw:0,0 -f S16_LE -r 44100 -c 2 2>> ./aplay.log
    echo "exit $(date)" >> ./spot.log
    sleep 5
done > /dev/null 2>&1 &
exit 0
