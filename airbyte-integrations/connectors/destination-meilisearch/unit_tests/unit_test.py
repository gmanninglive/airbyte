#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import patch

from destination_meilisearch.writer import MeiliWriter
from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, Type


@patch("meilisearch.Client")
def test_queue_write_operation(client):
    stream_name = "airbyte-testing"
    writer = MeiliWriter(client, [stream_name], "primary_key")
    writer.queue_write_operation(_record(stream_name))
    assert len(writer.write_buffer.get("airbyte-testing")) == 1


@patch("meilisearch.Client")
def test_flush(client):
    stream_name = "airbyte-testing"
    writer = MeiliWriter(client, [stream_name], "primary_key")
    writer.queue_write_operation(_record(stream_name))
    writer.flush()
    client.index.assert_called_once_with("airbyte-testing")
    client.wait_for_task.assert_called_once()


def _record(stream: str) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD, record=AirbyteRecordMessage(
            stream=stream, data={"str_col": "a", "int_col": 1}, emitted_at=0)
    ).record
