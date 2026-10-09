#!/bin/sh
# librespot's sink, started when playback starts and killed when it stops, so the DAC's PCM is free
# for local music meanwhile. Q2 Pod stops hciplayer as Spotify starts, but it can hold the PCM a
# moment longer: aplay opens the PCM before reading stdin, so retry for 3 s. With Bluetooth or a
# USB DAC as the Q2's output, Q2 Pod leaves the device and its volume in /tmp/q2-librespot.out,
# read at each try, and its q2video plays there at that volume; without the file (older Q2 Pod
# too), aplay plays on the headphone DAC.
play() {
    if read -r dev vol 2> /dev/null < /tmp/q2-librespot.out; then
        /usr/bin/q2video -s "$dev" "$vol"
    else
        aplay -q -D plughw:0,0 -f S16_LE -r 44100 -c 2
    fi
}
n=0
until play 2>> ./aplay.log; do
    n=$((n + 1))
    [ $n -ge 3 ] && exit 1
    sleep 1
done
