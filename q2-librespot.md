# librespot on the Shanling Q2: Q2 Pod integration plan

## Context

librespot already plays on the Q2. It runs as a Spotify Connect receiver, launched from the card's Rockbox slot, and nothing is flashed. On the device, the phone discovers **Q2**, authenticates, and audio plays through the headphones at 160 kbit/s. Soft-float decoding keeps up. How it's built and run: `contrib/shanlingq2/README` (this repo, branch `shanlingq2`).

The stock player (`demo`, patched by Q2 Pod) doesn't know librespot exists. That causes everything still wrong:

1. **The Q2's own controls do nothing.** Play/Pause, skip, the wheel and volume reach demo, not librespot. Only the phone controls playback.
2. **Playback stops when the screen goes off.** demo sees nothing playing, so its idle and standby logic runs.
3. **Volume is fixed.** `dacon 70` puts the DAC at -15 dB, under librespot's software volume. The Q2's volume isn't involved.
4. **A headphone replug was needed once** before sound came through. demo normally sets up the output path when it plays.
5. **Local music conflicts with librespot.** `launcher.sh` pipes librespot into an `aplay` that holds `plughw:0,0` the whole time it runs, even while idle. hciplayer allows one client per PCM, with no dmix, so local music probably can't open the DAC while the spike is on the card.

**Goal:** librespot behaves like a source of Q2 Pod's own playback. While it plays, the Q2 doesn't go to standby, the DAC stays on and routed, the Q2's media keys and volume control it, and local music and librespot hand the DAC over cleanly.

The pattern already exists: Videos runs the external helper `q2video` this way (`patch/books.c`, `video_*`). Copy its shape; don't invent a new one.

## Repos and conventions

- **Firmware payload:** `~/git/q2/q2-pod` ([DiamondBond/q2-pod](https://github.com/DiamondBond/q2-pod), branch `main`). It's MIT C and MIPS, patched into demo by `tools/build.py`. Read `docs/building.md`, `docs/internals.md` (Videos, Low power) and `docs/boot.md` first.
- **This fork:** `~/git/q2/librespot` ([DiamondBond/q2-librespot](https://github.com/DiamondBond/q2-librespot), branch `shanlingq2`, cut from v0.8.0, remote `upstream` = librespot-org). Keep changes small and in upstream's style (`cargo fmt`). Q2-only things go in `contrib/shanlingq2/`.
- **Rockbox reference:** `~/git/q2/rockbox`, branch `shanlingq2`. `firmware/target/hosted/shanling/shanlingq2_codec.c` documents the DAC ioctls, and `tools/shanlingq2/README` the device.
- **Commits:** `area: summary` titles (`shanlingq2: …`, `discovery: …`) and a body that explains why. Each commit ends with the `Co-Authored-By` line its agent uses.
- **q2-pod checks:** run `tools/format.sh` and the tests in `docs/building.md`. New payload logic gets a check in the existing test files (`test/patch.py` runs the MIPS payload against mocks).
- **Rootfs space:** the payload is tight on space, about 58 KB left in Stock (`docs/building.md`), so keep additions small.

## Design

The two processes talk through two files in `/tmp`:

- **State (librespot → Q2 Pod):** `contrib/shanlingq2/onevent.sh` already runs on every librespot player event. It writes the state to `/tmp/q2-librespot.state`, one word: `playing`, `paused` or `stopped`. It writes to a temp file, then renames, so the payload never reads half a write. The payload reads this file (it doesn't own it) and throttles reads to about once a second, the way `power_poll` paces its work.
- **Control (Q2 Pod → librespot):** the fork binds a Unix datagram socket, `/tmp/q2-librespot.sock`, and maps one-byte commands to the `Spirc` handle in `src/main.rs`. `connect/src/spirc.rs` already exposes `play_pause`, `pause`, `next`, `prev` and `set_volume`. The payload sends with `MSG_DONTWAIT`, exactly as `video_key` does (`patch/books.c` `video_key`, `socket(1, 1, 0)` is AF_UNIX/SOCK_DGRAM in MIPS numbering).

### librespot fork (`~/git/q2/librespot`)

1. **Audio on demand.** Switch `launcher.sh` from `--backend pipe | aplay` to `--backend subprocess --device "aplay -D plughw:0,0 -f S16_LE -r 44100 -c 2"`. `playback/src/audio_backend/subprocess.rs` spawns the command when the sink starts and kills it when it stops (pause, end of playback). That releases the PCM, which fixes problem 5 on librespot's side. Check that the subprocess writes the same S16 format the pipe did.
2. **Control socket.** Add it in `src/main.rs`, behind a `--control-socket PATH` option so upstream behaviour doesn't change. Spawn a task that receives datagrams and calls the Spirc handle when there is one: `p` play/pause, `s` pause, `n` next, `b` previous. Volume stays the DAC's (below), so no volume command is needed. Recreate it when Spirc is recreated: the loop in `main.rs` around `Spirc::new` replaces `spirc` on reconnect. Keep the patch small and generic, so it could go upstream later.
3. **State file.** Extend `onevent.sh` to write the state file for `playing`, `paused`, `stopped`, `end_of_track`, `unavailable` and `session_disconnected` (check the exact `PLAYER_EVENT` names in `src/player_event_handler.rs`). It must never write to stdout: with the pipe backend, stdout was the audio stream, and librespot doesn't redirect the event program's stdout.
4. **dacon.** Once Q2 Pod powers the DAC (below), `onevent.sh` doesn't need to call `dacon`. Keep it as the fallback for firmware without the integration, but stop it setting the volume: drop the `v` argument path, or pass no volume, so it doesn't override the DAC volume Q2 Pod sets.
5. **Docs.** Update `contrib/shanlingq2/README` (files, status).

### q2-pod payload (`~/git/q2/q2-pod`)

Put the new code next to Videos' in `patch/books.c` (or a small new `patch/spotify.c` if that reads better; check how `tools/build.py` compiles sources). Call it from the existing hooks:

1. **`spot_poll()` from `ringnav_sleep`** (`patch/navigation.c`, next to `video_poll()`). It reads the state file, throttled. When the state changes to `playing`:
   - powers the DAC on through demo's own path, as `play_video` does: `mclSetDacPwr(1)` when `g_dacoff_time < 0` (`patch/books.c:737`). Also check what demo does for its own playback start on the DAC way (`config_outputchannel`, the headset mode). That's probably the fix for problem 4: the replug. Find the call by reading how `player_play`, or the headset insert handler `check_headset_status` (`0x4e8e14`), sets up the output, and call the same thing;
   - applies the current volume with `device_set_volume(g_volume, 1)`, the same call `video_key` uses (problem 3).

   While it stays `playing`, every pass resets the standby and auto-power-off counters, as `video_poll` does: `reset_poweroptions_timer(...)` (`0x4f43dc`) and `g_dacoff_time = 0`. Unlike Videos, the **screen must still be allowed to turn off**: check the three arguments (screen, standby, auto-power-off per `docs/internals.md`) and pass the ones that leave the screen's own timer alone. That fixes problem 2. Confirm on the device which mechanism actually stopped playback; see Verification.
2. **Keys.** In `ringnav_input` (`patch/navigation.c`), while librespot is `playing` or `paused`, send the **physical media keys** to librespot instead of hciplayer: `KEY_PLAY` → `p`, `KEY_FWD_BTN` → `n`, `KEY_BACK_BTN` → `b`. Swallow those keys, as the Videos branch does. Everything else, the wheel, Centre, touch and navigation, stays with the UI. Volume has to work everywhere demo changes volume (the volume dialog, screen-off wheel), and it already does: those paths go through `device_set_volume`, which sets the DAC librespot plays through. Check that on the device. If demo only applies volume while hciplayer plays, hook the volume path the way `video_key` does.
3. **Handover with local music.** When the user starts local playback while librespot is active, send `s` first, so librespot pauses and its `aplay` exits before hciplayer opens the PCM. Find the payload's existing hook on playback start (`playback_poll`, or the play paths in `navigation.c`). In the other direction, when librespot starts playing while local music plays, stop hciplayer the way `play_video` does (`player_stop`, `0x515b94`).
4. **Low power.** `cpu_poll` takes CPU1 offline with the screen off unless `video_on()`. Measure librespot's CPU with the screen off and Low power on (see Verification). Only if it underruns on one core, add the librespot state to that condition.
5. **Docs and tests.** Add a librespot section to `docs/internals.md` (files, the state and socket contract, keys, power), update `docs/librespot.md`, and add the mocked checks in `test/patch.py` that Videos has an equivalent of: key mapping, swallowing, the throttled poll, the power calls.

## Device testing

There is no shell on the Q2: no adb, telnet or ssh, only a UART on `ttyS3`. Everything runs from the card, and results come back as log files in `/mnt/mmc/.rockbox/` (`spot.log`, `aplay.log`, `dacon.log`, `cards.log`).

- **Build librespot and the card files:** `contrib/shanlingq2/build.sh ~/x-tools/mipsel-linux-muslsf-cross` writes `target/shanlingq2/.rockbox/`. That folder becomes the card's `/.rockbox`, keeping its `cache/` (saved credentials). The user's real Rockbox is in `/.rockbox.bak`.
- **Build the firmware:** see `docs/building.md` (`tools/build.py … --ipod --dev` for a test build). The user flashes it from the card.
- **Card safety, learned the hard way:** copy, `sync`, then unmount and remount and `cmp` against the source. Then unmount and `udisksctl power-off`. The card is FAT and nearly full. The Q2 corrupted it twice when it was swapped while the Q2 was on or asleep: stale directory entries were written back and cluster chains broke. The user must fully shut the Q2 down before removing or inserting the card, and power on cold. If the kernel logs `FAT-fs … invalid cluster chain`, stop and have the user run `sudo fsck.fat -a /dev/sdb` (it needs their terminal).
- **mDNS:** the first librespot start after boot fails until Wi-Fi is up (`No such device`); the launcher's restart loop covers that.

## Verification

On the device, with Q2 Pod built from this work:

1. **Cast and keys:** cast from the phone. Sound comes out without replugging. The Q2's Play/Pause, next and previous control Spotify, and the phone's UI follows.
2. **Volume:** Q2 volume changes are audible, both with the screen on and off. Volume isn't at full when playback starts.
3. **Screen off:** let the screen time out. Playback continues for at least 10 minutes. Repeat with Low power on. To get CPU numbers without a shell, log `/proc/loadavg` and librespot's `/proc/<pid>/status` (VmRSS) from `launcher.sh` every 10 s to a card file for one test run.
4. **Handover:** play local music, then cast: local music stops and Spotify plays. Cast, then start local music: Spotify pauses and local music plays.
5. **Idle:** with no session, the Q2 still goes to standby and powers the DAC off as stock does.

Then run the q2-pod test suite (`docs/building.md`) and `tools/format.sh`.

## Out of scope

- Bluetooth output (`plug:bluealsa`; see `shanlingq2_codec.c` for how Rockbox picks it).
- Showing what's playing on the Q2 (Now Playing for Spotify).
- Browsing Spotify on the device.
- Packaging librespot into the firmware: it's 17.7 MB, and the rootfs has about 58 KB free. It stays on the card.
