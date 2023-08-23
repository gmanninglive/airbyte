#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections.abc import Iterable, Mapping
from logging import getLogger
from uuid import uuid4

from meilisearch import Client
from airbyte_cdk.models import AirbyteRecordMessage

logger = getLogger("airbyte")


class MeiliWriter:
    flush_interval = 50000

    def __init__(self, client: Client, streams: Iterable[str], primary_key: str):
        self.client = client
        self.primary_key = primary_key
        self.streams = streams
        self.write_buffer = dict((s, []) for s in streams)

    def queue_write_operation(self, message: AirbyteRecordMessage):
        random_key = str(uuid4())
        self.write_buffer[message.stream].append(
            {**message.data, self.primary_key: random_key})
        if len(sum(self.write_buffer.values(), [])) == self.flush_interval:
            self.flush()

    def flush(self):
        buffer_size = len(sum(self.write_buffer.values(), []))
        if buffer_size == 0:
            return
        logger.info(f"flushing {buffer_size} records: {self.write_buffer}")
        for stream_name in self.streams:
            response = self.client.index(
                stream_name).add_documents(self.write_buffer[stream_name])
            self.client.wait_for_task(response.task_uid, 1800000, 1000)
            self.write_buffer[stream_name].clear()
