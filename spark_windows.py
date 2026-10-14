"""Configure the local Hadoop helper required by PySpark on Windows."""

from __future__ import annotations

import os
import platform
import urllib.error
import urllib.request
from pathlib import Path

WINUTILS_URL = (
    "https://github.com/cdarlint/winutils/raw/master/" "hadoop-3.3.6/bin/winutils.exe"
)
HADOOP_DLL_URL = (
    "https://github.com/cdarlint/winutils/raw/master/" "hadoop-3.3.6/bin/hadoop.dll"
)


def configure_windows_hadoop(project_dir: Path | None = None) -> None:
    """Set Hadoop environment variables and download winutils when necessary."""
    if platform.system() != "Windows":
        return

    root = project_dir or Path(__file__).resolve().parent
    hadoop_home = Path(os.environ.get("HADOOP_HOME", root / ".hadoop"))
    winutils = hadoop_home / "bin" / "winutils.exe"
    hadoop_dll = hadoop_home / "bin" / "hadoop.dll"
    if not winutils.exists() or not hadoop_dll.exists():
        winutils.parent.mkdir(parents=True, exist_ok=True)
        try:
            if not winutils.exists():
                urllib.request.urlretrieve(WINUTILS_URL, winutils)
            if not hadoop_dll.exists():
                urllib.request.urlretrieve(HADOOP_DLL_URL, hadoop_dll)
        except urllib.error.URLError as error:
            raise RuntimeError(
                "Could not download winutils.exe. Check your internet connection "
                "or install it manually under HADOOP_HOME/bin."
            ) from error

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    bin_dir = str(hadoop_home / "bin")
    path_entries = os.environ.get("PATH", "").split(os.pathsep)
    if bin_dir not in path_entries:
        os.environ["PATH"] = os.pathsep.join([bin_dir, *path_entries])

    # Ensure the JVM can find hadoop.dll via java.library.path
    os.environ["JAVA_LIBRARY_PATH"] = bin_dir
