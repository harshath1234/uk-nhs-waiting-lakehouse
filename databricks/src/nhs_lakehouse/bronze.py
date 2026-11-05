"""Bronze: typed raw. One row per source row, original values as strings, plus lineage.

Landing layout (written by ADF, see config/manifest.json):
    landing/<dataset>/period=YYYY-MM/published=YYYY-MM/<file>
RTT arrives zipped; Spark cannot read ZIP, so ``extract_zips`` unpacks each one into a
staging folder with the same partition path first.
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import AE_COLUMNS, MONTHS, ODS_ETR_COLUMNS, RTT_COLUMNS


def _reader(spark: SparkSession, header: bool):
    # recursiveFileLookup turns off partition discovery. Without it Spark adds the folder names
    # (period=..., published=...) as columns, and "period" clashes with the file's own "Period"
    # column (Spark is case-insensitive). Lineage comes from _metadata.file_path instead.
    # On Databricks Auto Loader the equivalent is cloudFiles.partitionColumns = "".
    return (
        spark.read.option("header", header)
        .option("recursiveFileLookup", True)
        .option("encoding", "ISO-8859-1")
    )


class SchemaDriftError(ValueError):
    """The source header no longer matches the schema we validated against."""


def extract_zips(landing_root: str, staging_root: str) -> list[str]:
    """Unzip every landed RTT ZIP into staging, keeping the partition path. Idempotent."""
    written = []
    for zpath in sorted(Path(landing_root).rglob("*.zip")):
        rel = zpath.parent.relative_to(landing_root)
        out_dir = Path(staging_root) / rel
        with zipfile.ZipFile(zpath) as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv") or member.endswith("/"):
                    continue
                target = out_dir / os.path.basename(member)  # never trust paths inside a ZIP
                if target.exists() and target.stat().st_size == zf.getinfo(member).file_size:
                    continue
                out_dir.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as src, open(target, "wb") as dst:
                    dst.write(src.read())
                written.append(str(target))
    return written


def _check_header(actual: list[str], expected: list[tuple[str, str]], dataset: str) -> None:
    exp = [src for src, _ in expected]
    actual = [c.strip() for c in actual]
    if actual != exp:
        missing = [c for c in exp if c not in actual]
        extra = [c for c in actual if c not in exp]
        raise SchemaDriftError(f"{dataset}: header changed. missing={missing[:5]} extra={extra[:5]}")


def _lineage(df: DataFrame) -> DataFrame:
    path = F.col("_metadata.file_path")
    return (
        df.withColumn("_source_file", path)
        .withColumn("_published", F.regexp_extract(path, r"published=(\d{4}-\d{2})", 1))
        .withColumn("_ingest_ts", F.current_timestamp())
    )


def period_to_month(label_col: str) -> F.Column:
    """'RTT-August-2026' / 'RTT-APRIL-2021' / 'MSitAE-SEPTEMBER-2026' -> first day of month."""
    parts = F.split(F.upper(F.col(label_col)), "-")
    month_name = F.element_at(parts, -2)
    year = F.element_at(parts, -1)
    mapping = F.create_map(*[x for k, v in MONTHS.items() for x in (F.lit(k), F.lit(v))])
    return F.make_date(year.cast("int"), F.element_at(mapping, month_name), F.lit(1))


def read_rtt(spark: SparkSession, path: str) -> DataFrame:
    raw = _reader(spark, True).csv(path)
    _check_header(raw.columns, RTT_COLUMNS, "rtt")
    df = raw.select([F.col(f"`{src}`").alias(dst) for src, dst in RTT_COLUMNS] + ["_metadata"])
    return _lineage(df).withColumn("period_month", period_to_month("period_label")).drop("_metadata")


def read_ae(spark: SparkSession, path: str) -> DataFrame:
    raw = _reader(spark, True).csv(path)
    _check_header(raw.columns, AE_COLUMNS, "ae")
    df = raw.select([F.col(f"`{src}`").alias(dst) for src, dst in AE_COLUMNS] + ["_metadata"])
    return (
        _lineage(df)
        # file names carry the revision date as ddmmyy, e.g. April-2021-revised-120522-ABC456.csv
        .withColumn(
            "_revised_on",
            F.to_date(F.nullif(F.regexp_extract("_source_file", r"revised-(\d{6})", 1), F.lit("")), "ddMMyy"),
        )
        .drop("_metadata")
    )


def read_ods_etr(spark: SparkSession, path: str) -> DataFrame:
    raw = _reader(spark, False).csv(path)
    if len(raw.columns) != len(ODS_ETR_COLUMNS):
        raise SchemaDriftError(f"ods etr: expected {len(ODS_ETR_COLUMNS)} columns, got {len(raw.columns)}")
    df = raw.toDF(*ODS_ETR_COLUMNS).select("*", "_metadata")
    path_col = F.col("_metadata.file_path")
    return (
        df.withColumn("_source_file", path_col)
        .withColumn(
            "snapshot_date", F.to_date(F.regexp_extract(path_col, r"snapshot=(\d{4}-\d{2}-\d{2})", 1))
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .drop("_metadata")
    )
