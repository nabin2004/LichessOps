from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq


class ParquetStreamWriter:
    """
    Buffers parsed game records and writes them to a Parquet file in batches.
 
    Usage:
        with ParquetStreamWriter(output_path, batch_size) as writer:
            for record in records:
                writer.add(record)
        print(writer.total)
    """
 
    def __init__(self, output_path: str, batch_size: int):
        self.output_path = output_path
        self.batch_size = batch_size
        self._batch: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self._schema: pa.Schema | None = None
        self.total = 0
 
    def __enter__(self):
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        self.close()
        return False  # don't suppress exceptions
 
    def add(self, record: dict) -> None:
        """Add a single record; flushes automatically when the batch is full."""
        self._batch.append(record)
        self.total += 1
        if len(self._batch) >= self.batch_size:
            self._flush_batch()
 
    def flush(self) -> None:
        """Write any remaining buffered records."""
        if self._batch:
            self._flush_batch()
 
    def close(self) -> None:
        """Close the underlying Parquet writer."""
        if self._writer:
            self._writer.close()
            self._writer = None
 
    def _flush_batch(self) -> None:
        table = pa.Table.from_pylist(self._batch)
        if self._writer is None:
            self._schema = table.schema
            self._writer = pq.ParquetWriter(self.output_path, self._schema)
        else:
            table = table.cast(self._schema)
        self._writer.write_table(table)
        self._batch.clear()
