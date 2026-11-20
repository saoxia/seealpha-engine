"""Start feed and pair-data workers. Callers outside the engine must not fork them."""

import subprocess


def popen(command, **kwargs):
    return subprocess.Popen(command, **kwargs)
