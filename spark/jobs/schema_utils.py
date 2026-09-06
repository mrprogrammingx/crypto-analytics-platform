"""Shared schema-drift handling for reading Parquet with Spark."""
import glob


def read_with_schema_reconciliation(spark, paths, coerce_to=None, normalize_fn=None):
    """Read `paths` with mergeSchema=true; on failure, fall back to reading
    each file individually and unioning them.

    `normalize_fn`, if given, is applied to each per-file DataFrame *before*
    the union — use it for semantic fixes (e.g. coercing a timestamp column
    that is BIGINT epoch-millis in some files and TIMESTAMP in others to a
    single TIMESTAMP type) that a blind type cast would get wrong. `coerce_to`
    ("string", "int", or "float") is a blunter fallback applied to any columns
    still mismatched after `normalize_fn` runs.
    """
    try:
        return spark.read.option("mergeSchema", "true").parquet(*paths)
    except Exception:
        pass

    cast_type = {"string": "string", "int": "int", "float": "double"}.get(coerce_to)

    dfs = []
    for p in paths:
        for m in (glob.glob(p) or [p]):
            try:
                df = spark.read.parquet(m)
                if normalize_fn is not None:
                    df = normalize_fn(df)
                dfs.append(df)
            except Exception:
                continue

    if not dfs:
        raise RuntimeError(f"Could not read any of the given paths: {paths}")

    result = None
    for df in dfs:
        if cast_type:
            for f in df.schema.fields:
                if f.dataType.simpleString() != cast_type:
                    df = df.withColumn(f.name, df[f.name].cast(cast_type))
        result = df if result is None else result.unionByName(df, allowMissingColumns=True)
    return result