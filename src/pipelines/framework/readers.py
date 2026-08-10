"""Stream readers, one per ingestion type.

Each reader turns a validated config into a streaming DataFrame. They are kept
separate from the table-generation logic so a new source type means adding one
function here plus one entry in the dispatch table -- not touching the
generator.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame


def read_kafka(spark: Any, dbutils: Any, cfg: dict[str, Any]) -> DataFrame:
    """Read a Confluent Cloud topic over SASL_SSL.

    Connection details come from a secret scope as a JSON blob rather than
    individual keys, so rotating the cluster means updating one secret.
    """
    conn = json.loads(
        dbutils.secrets.get(scope=cfg["secret_scope"], key=cfg["secret_key"])
    )

    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="{conn["api_key"]}" password="{conn["api_secret"]}";'
    )

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", conn["bootstrap_servers"])
        .option("subscribe", conn["topic"])
        .option("kafka.security.protocol", cfg.get("security_protocol", "SASL_SSL"))
        .option("kafka.sasl.mechanism", cfg.get("sasl_mechanism", "PLAIN"))
        .option("kafka.sasl.jaas.config", jaas)
        .option("startingOffsets", cfg.get("starting_offsets", "earliest"))
        .load()
    )


def read_autoloader(spark: Any, dbutils: Any, cfg: dict[str, Any]) -> DataFrame:
    """Incrementally read files from a volume/path using Auto Loader."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", cfg["format"])
    )

    for key, value in cfg.get("options", {}).items():
        reader = reader.option(key, value)

    return reader.load(cfg["path"])


def read_rest_api(spark: Any, dbutils: Any, cfg: dict[str, Any]) -> DataFrame:
    """Pull a paginated REST endpoint into a batch DataFrame.

    This is a *batch* reader, unlike the streaming readers above: the driver
    fetches pages and parallelises the resulting records. That is appropriate
    for reference data measured in thousands of rows -- merchant catalogues,
    FX rates, sanctions lists -- and wrong for high-volume event data, which
    belongs on Kafka where ordering and replay are preserved.

    Retries use exponential backoff and only cover transient failures (429 and
    5xx). A 4xx other than 429 means the request itself is wrong, so retrying
    would just repeat the same mistake.
    """
    import json as _json
    import time
    import urllib.error
    import urllib.parse
    import urllib.request

    api_cfg = cfg
    base_url = api_cfg["url"]
    headers = dict(api_cfg.get("headers", {}))

    # Token comes from a secret scope, never from config.
    if "secret_scope" in api_cfg and "secret_key" in api_cfg:
        token = dbutils.secrets.get(
            scope=api_cfg["secret_scope"], key=api_cfg["secret_key"]
        )
        header_name = api_cfg.get("auth_header", "Authorization")
        template = api_cfg.get("auth_template", "Bearer {token}")
        headers[header_name] = template.format(token=token)

    page_param = api_cfg.get("page_param", "page")
    size_param = api_cfg.get("page_size_param", "page_size")
    page_size = int(api_cfg.get("page_size", 100))
    records_path = api_cfg.get("records_path", "")
    max_pages = int(api_cfg.get("max_pages", 1000))
    timeout = int(api_cfg.get("timeout_seconds", 30))
    max_retries = int(api_cfg.get("max_retries", 3))

    def _extract(payload: Any) -> list[dict[str, Any]]:
        """Walk a dotted path to the record list, e.g. 'data.merchants'."""
        node = payload
        for part in filter(None, records_path.split(".")):
            try:
                node = node[part]
            except (KeyError, TypeError):
                raise ValueError(
                    f"records_path '{records_path}' does not match the response "
                    f"shape: no key '{part}'. Top-level keys were "
                    f"{sorted(payload) if isinstance(payload, dict) else type(payload).__name__}"
                ) from None
        return node if isinstance(node, list) else [node]

    def _fetch(page: int) -> list[dict[str, Any]]:
        query = dict(api_cfg.get("params", {}))
        query[page_param] = page
        query[size_param] = page_size
        url = f"{base_url}?{urllib.parse.urlencode(query)}"

        for attempt in range(max_retries):
            request = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return _extract(_json.loads(response.read().decode("utf-8")))
            except urllib.error.HTTPError as exc:
                transient = exc.code == 429 or exc.code >= 500
                if not transient or attempt == max_retries - 1:
                    raise
                time.sleep(2**attempt)
            except urllib.error.URLError:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2**attempt)
        return []

    records: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        batch = _fetch(page)
        if not batch:
            break
        records.extend(batch)
        # A short page means the last page.
        if len(batch) < page_size:
            break

    if not records:
        raise ValueError(
            f"REST source '{base_url}' returned no records; refusing to "
            "materialise an empty table over existing data"
        )

    # json.loads gives Python dicts; round-trip through RDD[str] so Spark
    # infers a proper schema rather than guessing from dict ordering.
    rdd = spark.sparkContext.parallelize([_json.dumps(r) for r in records])
    return spark.read.json(rdd)


# Dispatch table: ingestion type -> reader. Adding a source type (kinesis,
# delta share, JDBC snapshot) means one function above and one line here.
READERS: dict[str, Callable[[Any, Any, dict[str, Any]], DataFrame]] = {
    "kafka": read_kafka,
    "autoloader": read_autoloader,
    "rest_api": read_rest_api,
}

# Readers that produce a bounded DataFrame rather than a stream. The bronze
# factory registers these as materialised views instead of streaming tables,
# because @dp.table over a batch source would re-read everything each update
# without incremental semantics.
BATCH_TYPES = {"rest_api"}


def get_reader(ingestion_type: str) -> Callable[[Any, Any, dict[str, Any]], DataFrame]:
    try:
        return READERS[ingestion_type]
    except KeyError:
        raise ValueError(
            f"no reader for ingestion type '{ingestion_type}'; "
            f"known types: {sorted(READERS)}"
        ) from None
