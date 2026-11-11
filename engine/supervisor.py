"""Start feed and pair-data workers. Callers outside the engine must not fork them."""

import os
import subprocess

from core.paths import PROJECT_ROOT


def popen(command, *, env=None, **kwargs):
    # Worker cwd may point to a data volume outside the source checkout.
    worker_env = dict(os.environ if env is None else env)
    inherited_path = worker_env.get("PYTHONPATH")
    worker_env["PYTHONPATH"] = (
        os.pathsep.join((str(PROJECT_ROOT), inherited_path)) if inherited_path else str(PROJECT_ROOT)
    )
    return subprocess.Popen(command, env=worker_env, **kwargs)
