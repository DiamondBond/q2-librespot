#!/usr/bin/env python3
"""Run the launcher with fake process/time commands, optionally under Q2 BusyBox/QEMU.
Usage: test_launcher.py [QEMU BUSYBOX SYSROOT]
"""
import pathlib
import subprocess
import sys
import tempfile

source = pathlib.Path(__file__).with_name("launcher.sh").read_text()
shell = ([sys.argv[1], "-L", sys.argv[3], sys.argv[2], "sh"]
         if len(sys.argv) == 4 else ["/bin/sh"])
for quality, expected in [(None, "320"), ("96\n", "96"), ("160\n", "160"),
                          ("320\n", "320"), ("1x60", "320"), (" 160", "320"),
                          ("160x", "320"), ("", "320")]:
    with tempfile.TemporaryDirectory(prefix="q2-launcher-") as tmp:
        root = pathlib.Path(tmp)
        (root / "cache").mkdir()
        (root / "cache/login").write_text("saved login")
        (root / "cache/audio").write_text("saved download")
        (root / ".cache-size").write_text("4096M")
        if quality is not None:
            (root / ".quality").write_text(quality)
        commands = root / "bin"
        commands.mkdir()
        for name, body in {
            "date": "echo 0",
            "sleep": 'echo "$1" >> "$Q2_LAUNCH_TEST/backoff"; [ "$1" -lt 60 ] || : > "$Q2_LAUNCH_TEST/test-done"',
        }.items():
            path = commands / name
            path.write_text("#!/bin/sh\n" + body + "\n")
            path.chmod(0o755)
        binary = root / "librespot"
        binary.write_text('#!/bin/sh\nprintf "%s\\n" "$@" >> "$Q2_LAUNCH_TEST/args"\n')
        binary.chmod(0o755)
        script = source.replace("/mnt/mmc/.spotify", tmp).replace("/tmp/q2-librespot", tmp + "/pid")
        script = script.replace("while :; do", "while [ ! -e ./test-done ]; do", 1)
        script = script.rsplit("exit 0", 1)[0] + "wait\n"
        launch = root / "run"
        launch.write_text(script)
        import os
        env = dict(os.environ, Q2_LAUNCH_TEST=tmp, PATH=str(commands) + ":/usr/bin:/bin")
        subprocess.run(shell + [str(launch)], env=env, check=True, timeout=10)
        args = (root / "args").read_text().splitlines()
        starts = [i for i, value in enumerate(args) if value == "-n"]
        assert len(starts) == 4, (quality, starts)
        for start in starts:
            argv = args[start:start + (starts[1] - starts[0])]
            for option, value in [("-b", expected), ("--cache-size-limit", "4096M"),
                                  ("--tmp", tmp + "/pid.tmp"), ("--tmp-fallback", "./tmp")]:
                assert argv[argv.index(option) + 1] == value, (quality, option, argv)
        assert (root / "backoff").read_text().splitlines() == ["10", "20", "40", "60"]
        assert (root / "cache/login").read_text() == "saved login"
        assert (root / "cache/audio").read_text() == "saved download"
        assert (root / ".cache-size").read_text() == "4096M"
print("Launcher: bitrate overrides, malformed values, backoff cap and saved data preservation passed.")
