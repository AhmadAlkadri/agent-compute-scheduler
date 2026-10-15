#!/usr/bin/env python3
"""Harmless test workload: records its argv, env and timing; optional
children, grandchildren, sleep and exit code.  Never allocates memory."""
import json
import os
import subprocess
import sys
import time

args = sys.argv[1:]
out = args[args.index("--out") + 1]
sleep = float(args[args.index("--sleep") + 1]) if "--sleep" in args else 0
code = int(args[args.index("--exit") + 1]) if "--exit" in args else 0


def event(kind, **extra):
    with open(out, "a") as handle:
        handle.write(json.dumps({"event": kind, "t": time.time(),
                                 "pid": os.getpid(), **extra}) + "\n")


event("start", argv=sys.argv, cwd=os.getcwd(),
      env={k: v for k, v in os.environ.items()
           if k.startswith(("SMC_", "ACS_", "OMP_", "FAKE_", "PATH"))},
      pgid=os.getpgid(0))
if "--children" in args:
    # a child that itself spawns a grandchild; all stay in this group
    child = subprocess.Popen([sys.executable, "-c",
        "import subprocess,sys,time;"
        "g=subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)']);"
        "open(sys.argv[1],'a').write('grandchild %d\\n'%g.pid);time.sleep(600)",
        out + ".pids"])
    with open(out + ".pids", "a") as handle:
        handle.write(f"child {child.pid}\n")
time.sleep(sleep)
event("end", code=code)
sys.exit(code)
