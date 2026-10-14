"""
TEMPLATE - custom PySpark streaming data source for the OMS REST API.

Registered and used by bronze_sources.read_oms_api():

    spark.readStream.format("oms_api")
        .option("base_url", "...")
        .option("endpoint", "sales_orders")
        .option("columns", "sales_order_id,customer_id,...")
        .load()

Until the real connector is implemented, read() returns no records, so the
OMS realtime flows run but stay empty. Replace the TODOs below.

Output schema: every column listed in the "columns" option as STRING, plus
_raw_payload (the raw JSON record) and _request_url (the request that returned it).
"""

import json
import sys

from pyspark import cloudpickle
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader


RAW_PAYLOAD_COLUMN = "_raw_payload"
REQUEST_URL_COLUMN = "_request_url"

# Ship this module's code with the pickled data source, so the Python
# workers don't need the pipeline root folder on their sys.path
cloudpickle.register_pickle_by_value(sys.modules[__name__])


class OmsApiDataSource(DataSource):

    @classmethod
    def name(cls):
        return "oms_api"

    def _columns(self):
        return [c for c in self.options["columns"].split(",") if c]

    def schema(self):
        columns = self._columns() + [RAW_PAYLOAD_COLUMN, REQUEST_URL_COLUMN]
        return ", ".join(f"`{c}` STRING" for c in columns)

    def simpleStreamReader(self, schema):
        return OmsApiStreamReader(self.options, self._columns())


class OmsApiStreamReader(SimpleDataSourceStreamReader):
    """
    Incremental reader driven by a high-watermark cursor (e.g. updated_at).

    Offsets must be JSON-serializable dicts; Spark checkpoints them so the
    stream resumes from the last committed cursor after a restart.
    """

    def __init__(self, options, columns):
        self.base_url = options["base_url"].rstrip("/")
        self.endpoint = options["endpoint"]
        self.page_size = int(options.get("page_size", "1000"))
        self.start_cursor = options.get("start_cursor", "1970-01-01T00:00:00Z")
        self.columns = columns

        # TODO: authentication, e.g. pass a token resolved on the driver from
        #       a secret scope as an option, and send it as a request header

    def initialOffset(self):
        return {"cursor": self.start_cursor}

    def read(self, start):
        """
        Fetch every record changed after start["cursor"] and return
        (iterator of rows, next offset).
        """

        request_url = f"{self.base_url}/{self.endpoint}"

        # TODO: call the API and page through the results, e.g.
        #   GET {request_url}?updated_after={start['cursor']}&limit={self.page_size}
        records = []

        if not records:
            return iter([]), start

        # TODO: advance the cursor to the highest change timestamp returned
        end = {"cursor": max(r["updated_at"] for r in records)}

        return iter(self._to_rows(records, request_url)), end

    def readBetweenOffsets(self, start, end):
        """
        Deterministically re-read the records between two committed offsets
        (used when a micro-batch is replayed after a failure).
        """

        # TODO: same request as read(), bounded by updated_before={end['cursor']}
        return iter([])

    def _to_rows(self, records, request_url):
        for record in records:
            values = [
                None if record.get(c) is None else str(record.get(c))
                for c in self.columns
            ]
            yield tuple(values + [json.dumps(record), request_url])
