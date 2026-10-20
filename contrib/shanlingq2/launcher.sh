#!/bin/sh
# Installed as /mnt/mmc/.spotify/run. Q2 Pod starts it once per boot, when Streaming's Spotify row
# is first opened. librespot restarts after any exit (mDNS needs Wi-Fi up), backing
# off to a minute while it keeps failing. /tmp/q2-librespot, the restart loop's pid, makes a second
# run exit and lets Q2 Pod stop it. With a file named debug here, load.log gets the load,
# librespot's memory and the free RAM every 10 s. .cache-size, which Q2 Pod's Streaming >
# Configuration writes ("4096M"), caps the audio cache, read at each start; 2G when it is missing
# or malformed. .quality (96, 160 or 320) is the bitrate in kbit/s, read the same way; 320 by
# default. Streams download to /tmp/q2-librespot.tmp, in RAM, or to ./tmp when RAM is short; both
# are emptied at each start, as a killed librespot leaves its files there.
cd /mnt/mmc/.spotify || exit 1
[ -e /tmp/q2-librespot ] && exit 0
wait=5
while :; do
    start=$(date +%s)
    limit=$(cat .cache-size 2>/dev/null)
    case $limit in '' | [!0-9]* | *[!0-9KMG]* | *[KMG]?*) limit=2G ;; esac
    q=$(tr -cd 0-9 < .quality 2>/dev/null)
    case $q in 96 | 160 | 320) ;; *) q=320 ;; esac
    rm -rf /tmp/q2-librespot.tmp ./tmp
    ./librespot -n Q2 -b "$q" --cache ./cache --cache-size-limit "$limit" \
        --tmp /tmp/q2-librespot.tmp --tmp-fallback ./tmp \
        --backend subprocess --device "/bin/sh ./aplay.sh" \
        --volume-ctrl fixed --initial-volume 100 \
        --control-socket /tmp/q2-librespot.sock --status-file /tmp/q2-librespot.state \
        --browse-socket /tmp/q2-librespot.browse.sock 2>> ./spot.log
    echo "exit $? $(date)" >> ./spot.log
    if [ $(($(date +%s) - start)) -gt 60 ]; then wait=5; elif [ $wait -lt 60 ]; then wait=$((wait * 2)); fi
    sleep $wait
done > /dev/null 2>&1 &
echo $! > /tmp/q2-librespot # the loop, which Q2 Pod's Rockbox shortcut kills
[ -e debug ] && while :; do
    pid=$(pidof librespot)
    echo "$(date +%T) $(cat /proc/loadavg) $(grep VmRSS /proc/$pid/status 2>/dev/null)" \
        $(grep -E '^(MemAvailable|Shmem):' /proc/meminfo) >> ./load.log
    sleep 10
done > /dev/null 2>&1 &
exit 0
