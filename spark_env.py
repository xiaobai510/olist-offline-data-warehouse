from pathlib import Path
import os
import sys

PROJECT_DIR = Path(__file__).resolve().parent
HADOOP_HOME = PROJECT_DIR / ".hadoop"
HADOOP_BIN = HADOOP_HOME / "bin"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
os.environ["PATH"] = (
    str(HADOOP_BIN)
    + os.pathsep
    + os.environ.get("PATH", "")
)
os.environ["JAVA_TOOL_OPTIONS"] = (
    f"-Djava.library.path={HADOOP_BIN}"
)

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    work_dir = PROJECT_DIR / "work"
    local_dir = work_dir / "spark-temp"
    warehouse_dir = work_dir / "spark-warehouse"

    local_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder
        .master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )