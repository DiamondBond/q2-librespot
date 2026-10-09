#!/bin/sh
# librespot's sink, started when playback starts and killed when it stops, so the DAC's PCM is free
# for local music meanwhile. Q2 Pod stops hciplayer as Spotify starts, but it can hold the PCM a
# moment longer: aplay opens the PCM before reading stdin, so retry for 3 s.
n=0
until aplay -q -D plughw:0,0 -f S16_LE -r 44100 -c 2 2>> ./aplay.log; do
    n=$((n + 1))
    [ $n -ge 3 ] && exit 1
    sleep 1
done
