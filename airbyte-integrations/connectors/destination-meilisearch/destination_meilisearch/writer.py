#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections import defaultdict
from collections.abc import Mapping
from logging import getLogger
from uuid import uuid4

from meilisearch import Client

logger = getLogger("airbyte")


class MeiliWriter:
    flush_interval = 50000
    write_buffer: list[tuple[str, Mapping]] = []

    def __init__(self, client: Client, primary_key: str):
        self.client = client
        self.primary_key = primary_key

    def queue_write_operation(self, stream_name: str, data: Mapping):
        random_key = str(uuid4())
        self.write_buffer.append(
            (stream_name, {**data, self.primary_key: random_key}))
        if len(self.write_buffer) == self.flush_interval:
            self.flush()

    def flush(self):
        buffer_size = len(self.write_buffer)
        if buffer_size == 0:
            return
        logger.info(f"flushing {buffer_size} records: {self.write_buffer}")

        grouped_by_stream: defaultdict[str, list] = defaultdict(list)
        for k, v in self.write_buffer:
            grouped_by_stream[k].append(v)

        for (stream_name, data) in grouped_by_stream.items():
            response = self.client.index(
                stream_name).add_documents(data)
            self.client.wait_for_task(response.task_uid, 1800000, 1000)
        self.write_buffer.clear()
