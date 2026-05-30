import os
import platform
import sys


def configure_pyspark_for_windows():
    """Configure required Spark/Hadoop environment settings for local Windows execution."""
    if platform.system().lower() != "windows":
        return

    python_exec = os.getenv("PYSPARK_DRIVER_PYTHON") or os.getenv("PYSPARK_PYTHON") or sys.executable
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)

    hadoop_home = os.getenv("HADOOP_HOME") or os.getenv("hadoop.home.dir") or r"C:\hadoop"
    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ.setdefault("hadoop.home.dir", hadoop_home)

    hadoop_bin = os.path.join(hadoop_home, "bin")
    current_path = os.environ.get("PATH", "")
    if hadoop_bin not in current_path:
        os.environ["PATH"] = hadoop_bin + os.pathsep + current_path

    winutils_path = os.path.join(hadoop_bin, "winutils.exe")
    if not os.path.exists(winutils_path):
        raise FileNotFoundError(
            "winutils.exe not found in HADOOP_HOME\\bin. "
            "Download a compatible winutils.exe for your Hadoop/Spark version and set HADOOP_HOME to its parent folder. "
            f"Current HADOOP_HOME={hadoop_home}."
        )
