#!/bin/sh
# Installed as /mnt/mmc/.spotify/run. Q2 Pod starts it once per boot, from the Spotify row, or at
# boot once cache/ holds a login. librespot restarts after any exit (mDNS needs Wi-Fi up), backing
# off to a minute while it keeps failing. /tmp/q2-librespot, the restart loop's pid, makes a second
# run exit and lets Q2 Pod stop it. With a file named debug here, load.log gets the load and
# librespot's memory every 10 s.
cd /mnt/mmc/.spotify || exit 1
[ -e /tmp/q2-librespot ] && exit 0
wait=5
while :; do
    start=$(date +%s)
    ./librespot -n Q2 -b 160 --cache ./cache --disable-audio-cache \
        --backend subprocess --device "/bin/sh ./aplay.sh" \
        --volume-ctrl fixed --initial-volume 100 \
        --control-socket /tmp/q2-librespot.sock --status-file /tmp/q2-librespot.state 2>> ./spot.log
    echo "exit $? $(date)" >> ./spot.log
    if [ $(($(date +%s) - start)) -gt 60 ]; then wait=5; elif [ $wait -lt 60 ]; then wait=$((wait * 2)); fi
    sleep $wait
done > /dev/null 2>&1 &
echo $! > /tmp/q2-librespot # the loop, which Q2 Pod's Rockbox shortcut kills
[ -e debug ] && while :; do
    pid=$(pidof librespot)
    echo "$(date +%T) $(cat /proc/loadavg) $(grep VmRSS /proc/$pid/status 2>/dev/null)" >> ./load.log
    sleep 10
done > /dev/null 2>&1 &
exit 0
