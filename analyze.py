import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = r"D:\2026-2027研究生\2026-2027_fall\CS5224云计算\lab2"
LOCAL_INPUT = os.path.join(BASE, "data", "twitter_combined.txt")
LOCAL_OUTPUT = os.path.join(BASE, "output")

# 已准备好的 winutils 目录（仅 Windows 本地需要）
WINUTILS_DIR = r"D:\workbuddy workspace\2026-10-18-22-36-31\tools\hadoop-3.5.0"

REQUIRED_USERS = ["214328887", "107830991"]
IS_WINDOWS = os.name == "nt"


def to_s3a(path):
    """Spark/Hadoop 读写 S3 只认 s3a://（s3:// 是废弃的块存储协议），自动转换。"""
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path


def setup_winutils():
    if not IS_WINDOWS:
        return
    winutils = os.path.join(WINUTILS_DIR, "bin", "winutils.exe")
    if os.path.isfile(winutils):
        os.environ.setdefault("HADOOP_HOME", WINUTILS_DIR)
        os.environ["hadoop.home.dir"] = WINUTILS_DIR
        os.environ["PATH"] = os.path.join(WINUTILS_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")


def build_counts(spark, input_path):
    # 1. 读入并按空白切分成边 (a, b)：a 关注 b
    lines = spark.read.text(input_path)
    tokens = F.split(F.trim(F.col("value")), r"\s+")
    edges = (lines
             .select(tokens.getItem(0).alias("a"), tokens.getItem(1).alias("b"))
             .where(F.col("a").isNotNull() & F.col("b").isNotNull())
             .where(F.col("a") != F.col("b"))          # 去自环
             .distinct())                              # 边去重

    # 2. 每条边产生两条记录：(a, followee) / (b, follower)
    records = edges.select(F.col("a").alias("user"), F.lit("followee").alias("role")).unionByName(
        edges.select(F.col("b").alias("user"), F.lit("follower").alias("role"))
    )

    # 3. 透视成宽表
    return (records.groupBy("user", "role").count()
                   .groupBy("user").pivot("role").sum("count")
                   .fillna(0)
                   .select(F.col("user").cast("string").alias("user"),
                           F.col("follower").cast("long").alias("follower"),
                           F.col("followee").cast("long").alias("followee")))


def collect_rows(df):
    it = df._jdf.toLocalIterator()
    out = []
    while it.hasNext():
        r = it.next()
        out.append((r.getAs(0), r.getAs(1), r.getAs(2)))
    return out


def main():
    setup_winutils()

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    standard = "--standard" in sys.argv
    input_path = to_s3a(args[0] if len(args) > 0 else LOCAL_INPUT)
    output_path = to_s3a(args[1] if len(args) > 1 else LOCAL_OUTPUT)

    cloud = not IS_WINDOWS or input_path.startswith(("s3", "s3a", "gs://", "hdfs"))

    spark = SparkSession.builder.appName("TwitterFollowerCount").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    counts = build_counts(spark, input_path)

    if cloud or standard:
        # 云端 / Linux：标准写法，output_path 是目录，产出 part-* 文件
        counts.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"[OK] 输出目录: {output_path}")
        all_rows = []
    else:
        # Windows 本地：取回全部结果，Python 直接写单文件
        os.makedirs(output_path, exist_ok=True)
        target = os.path.join(output_path, "twitter_counts.csv")
        all_rows = collect_rows(counts)
        with open(target, "w", encoding="utf-8", newline="") as fh:
            fh.write("user,follower,followee\n")
            for u, fo, fe in all_rows:
                fh.write(f"{u},{int(fo)},{int(fe)}\n")
        print(f"[OK] 输出文件: {target}  ({len(all_rows)} 行)")

    print("\n----------- 提交要求的结果 -----------")
    if cloud or standard:
        table = {r["user"]: (r["follower"], r["followee"])
                 for r in counts.where(F.col("user").isin(REQUIRED_USERS)).collect()}
    else:
        table = {u: (fo, fe) for u, fo, fe in all_rows if u in REQUIRED_USERS}

    for uid in REQUIRED_USERS:
        if uid in table:
            fo, fe = table[uid]
            print(f"{uid} has {int(fo)} followers")
            print(f"{uid} has {int(fe)} followees")
        else:
            print(f"{uid} 不在数据集中")

    spark.stop()


if __name__ == "__main__":
    main()
