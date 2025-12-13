import argparse
from pyspark.sql import SparkSession, functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="NYC Taxi Analytics (Big Data Project 1)")

    parser.add_argument(
        "--mode",
        choices=["filter", "group"],
        required=True,
        help="Operation mode: 'filter' for filtering + stats, 'group' for group-by + stats.",
    )
    parser.add_argument(
        "--input",
        default="hdfs:///data/taxi",
        help="Input path to Parquet data (default: hdfs:///data/taxi)",
    )
    parser.add_argument(
        "--attribute",
        required=True,
        help="Numeric attribute/column to compute statistics on (e.g. trip_distance, total_amount).",
    )

    # Filter options (used in both modes)
    parser.add_argument("--min_value", type=float, help="Minimum value for the attribute filter.")
    parser.add_argument("--max_value", type=float, help="Maximum value for the attribute filter.")
    parser.add_argument(
        "--start",
        help="Start datetime (inclusive) for filter, e.g. '2016-01-01 00:00:00'. Uses tpep_pickup_datetime.",
    )
    parser.add_argument(
        "--end",
        help="End datetime (inclusive) for filter, e.g. '2016-01-31 23:59:59'. Uses tpep_pickup_datetime.",
    )

    # Group-by option (for group mode)
    parser.add_argument(
        "--group_by",
        help="Column to group by in 'group' mode (e.g. passenger_count, VendorID, etc.).",
    )

    # Optional output
    parser.add_argument(
        "--output",
        help="Optional output path on HDFS to write results (Parquet). If omitted, only prints to console.",
    )

    return parser.parse_args()


def build_spark():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Analytics Project")
        # if you run Spark outside Hadoop container, you may need this:
        # .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .getOrCreate()
    )
    return spark


def apply_filters(df, args):
    """
    Apply optional filters:
    - time window on tpep_pickup_datetime
    - min/max on chosen attribute
    """
    attribute = args.attribute

    # Time filter (only if column exists and args provided)
    if "tpep_pickup_datetime" in df.columns:
        if args.start:
            df = df.filter(F.col("tpep_pickup_datetime") >= F.lit(args.start))
        if args.end:
            df = df.filter(F.col("tpep_pickup_datetime") <= F.lit(args.end))

    # Min/max filter on the chosen attribute
    if args.min_value is not None:
        df = df.filter(F.col(attribute) >= F.lit(args.min_value))
    if args.max_value is not None:
        df = df.filter(F.col(attribute) <= F.lit(args.max_value))

    return df


def mode_filter(df, args):
    """
    MODE: filter
    - apply filters
    - count matching rows
    - compute min/max/avg/stddev for the selected attribute
    """
    attribute = args.attribute

    df_filtered = apply_filters(df, args)

    count = df_filtered.count()
    print(f"\n=== FILTER MODE ===")
    print(f"Attribute: {attribute}")
    print(f"Rows matching filters: {count}")

    if count == 0:
        print("No rows match the given filters.")
        return

    stats = (
        df_filtered.agg(
            F.min(attribute).alias("min"),
            F.max(attribute).alias("max"),
            F.avg(attribute).alias("avg"),
            F.stddev(attribute).alias("stddev"),
        )
    )

    print("\nStatistics for filtered rows:")
    stats.show(truncate=False)

    if args.output:
        print(f"\nWriting filtered data to: {args.output}")
        df_filtered.write.mode("overwrite").parquet(args.output)


def mode_group(df, args):
    """
    MODE: group
    - apply optional filters
    - group by chosen column
    - compute min/max/avg/stddev for the selected attribute
    """
    attribute = args.attribute
    group_by = args.group_by

    if not group_by:
        raise ValueError("--group_by is required in 'group' mode.")

    df_filtered = apply_filters(df, args)

    print(f"\n=== GROUP MODE ===")
    print(f"Grouping by: {group_by}")
    print(f"Attribute: {attribute}")

    grouped = (
        df_filtered.groupBy(group_by)
        .agg(
            F.count("*").alias("count"),
            F.min(attribute).alias("min"),
            F.max(attribute).alias("max"),
            F.avg(attribute).alias("avg"),
            F.stddev(attribute).alias("stddev"),
        )
        .orderBy(group_by)
    )

    print("\nGrouped statistics (showing first 50 rows):")
    grouped.show(50, truncate=False)

    if args.output:
        print(f"\nWriting grouped results to: {args.output}")
        grouped.write.mode("overwrite").parquet(args.output)


def main():
    args = parse_args()
    spark = build_spark()

    print(f"\nReading data from: {args.input}")
    df = spark.read.parquet(args.input)
    print("Schema:")
    df.printSchema()

    if args.attribute not in df.columns:
        raise ValueError(f"Attribute '{args.attribute}' not found in columns: {df.columns}")

    if args.mode == "filter":
        mode_filter(df, args)
    elif args.mode == "group":
        mode_group(df, args)
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    spark.stop()


if __name__ == "__main__":
    main()
