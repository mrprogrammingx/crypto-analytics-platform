import unittest
from google.api_core.exceptions import NotFound

import loaders.bigquery_loader as bq_loader


class FakeClient:
    def __init__(self, project="fake-project", table_exists=True, query_result=None, query_raises=False):
        self.project = project
        self._table_exists = table_exists
        self._query_result = query_result
        self._query_raises = query_raises

    def get_table(self, table_id):
        if not self._table_exists:
            raise NotFound("not found")
        return object()

    class _FakeQuery:
        def __init__(self, rows):
            self._rows = rows

        def result(self):
            return self._rows

    def query(self, sql):
        if self._query_raises:
            raise Exception("query failed")
        # return an object with .result() returning iterable of rows
        rows = []
        if self._query_result:
            # emulate rows with attribute file_name
            rows = [type("R", (), {"file_name": v}) for v in self._query_result]
        return bq_loader.FakeQuery(rows) if hasattr(bq_loader, 'FakeQuery') else self._FakeQuery(rows)


class TestBigQueryLoaderHelpers(unittest.TestCase):
    def test_get_tracking_table_id_full(self):
        # project.dataset.table
        dest = "proj1.ds1.tbl1"
        fake = FakeClient(project="ignored")
        tid = bq_loader._get_tracking_table_id(dest, fake)
        self.assertEqual(tid, "proj1.ds1.loaded_files")

    def test_get_tracking_table_id_dataset_only(self):
        # dataset.table -> uses client.project
        dest = "ds1.tbl1"
        fake = FakeClient(project="myproj")
        tid = bq_loader._get_tracking_table_id(dest, fake)
        self.assertEqual(tid, "myproj.ds1.loaded_files")

    def test_get_tracking_table_id_invalid(self):
        with self.assertRaises(SystemExit):
            bq_loader._get_tracking_table_id("invalid", FakeClient())

    def test_ensure_tracking_table_exists(self):
        fake = FakeClient(table_exists=True)
        # should not raise
        bq_loader._ensure_tracking_table("proj.ds.loaded_files", fake)

    def test_ensure_tracking_table_missing_exits(self):
        fake = FakeClient(table_exists=False)
        with self.assertRaises(SystemExit):
            bq_loader._ensure_tracking_table("proj.ds.loaded_files", fake)

    def test_get_already_loaded_files_on_exception_returns_empty(self):
        fake = FakeClient(query_raises=True)
        res = bq_loader._get_already_loaded_files("proj.ds.loaded_files", fake)
        self.assertEqual(res, set())

    def test_get_already_loaded_files_returns_set(self):
        fake = FakeClient(query_result=["gs://a/1.parquet", "gs://a/2.parquet"])
        res = bq_loader._get_already_loaded_files("proj.ds.loaded_files", fake)
        self.assertEqual(res, {"gs://a/1.parquet", "gs://a/2.parquet"})


if __name__ == "__main__":
    unittest.main()
