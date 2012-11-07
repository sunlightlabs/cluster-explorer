#!/usr/bin/env python

import lz4
import os
import subprocess

lz4_path = os.path.dirname(lz4.__file__)
current = os.path.dirname(os.path.abspath(__file__))
for cfile in ["lz4.c", "lz4.h"]:
    os.symlink(os.path.join(lz4_path, cfile), os.path.join(current, cfile))

subprocess.Popen("make").communicate()

for cfile in ["lz4.c", "lz4.h"]:
    os.unlink(os.path.join(current, cfile))