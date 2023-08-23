#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import patch

from destination_meilisearch.writer import MeiliWriter
from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, Type


@patch("meilisearch.Client")
def test_queue_write_operation(client):
    stream_name = "airbyte-testing"
    writer = MeiliWriter(client, "primary_key")
    writer.queue_write_operation(stream_name, {"a": "a"})
    assert len(writer.write_buffer) == 1


@patch("meilisearch.Client")
def test_flush(client):
    stream_name = "airbyte-testing"
    writer = MeiliWriter(client, "primary_key")
    writer.queue_write_operation(stream_name, {"a": "a"})
    writer.flush()
    client.index.assert_called_once_with("airbyte-testing")
    client.wait_for_task.assert_called_once()
