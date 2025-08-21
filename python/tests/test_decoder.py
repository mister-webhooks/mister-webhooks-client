from copy import deepcopy, replace
from multiprocessing import Value
from kafka.record.default_records import DefaultRecordBase
import pytest
from mister_webhooks.types import ConsumerRecord
from mister_webhooks.client import FormatError, decode_message
import time

from mister_webhooks.webhook_event import EnvelopeDecodeError

fixture_event = ConsumerRecord(
    "xyz",
    0,
    123,
    2,
    int(time.time()) * 1000,
    DefaultRecordBase.CREATE_TIME,
    bytes(),
    bytes(),
    [],
    None,
    5,
    5,
    0,
)

missing_envelope_header: ConsumerRecord = deepcopy(fixture_event)

two_byte_envelope_header: ConsumerRecord = replace(
    fixture_event, headers=[("envelope", [0x01, 0x02])]
)

unhandled_system_event: ConsumerRecord = replace(
    fixture_event, headers=[("envelope", [0x00])]
)
unsupported_webhook_event: ConsumerRecord = replace(
    fixture_event, headers=[("envelope", [0x81])]
)
invalid_v1_event: ConsumerRecord = replace(
    fixture_event,
    headers=[("envelope", [0x80])],
    key=bytes("hello", "utf8"),
    value=bytes("world", "utf8"),
)


def test_decode_message_missing_header():
    with pytest.raises(FormatError):
        decode_message(missing_envelope_header)


def test_decode_message_two_byte_header():
    with pytest.raises(FormatError):
        decode_message(two_byte_envelope_header)


def test_decode_message_unhandled_system_event():
    assert decode_message(unhandled_system_event) == None


def test_decode_message_unsupported_webhook_event():
    with pytest.raises(ValueError):
        decode_message(unsupported_webhook_event)


def test_decode_message_invalid_v1_event():
    with pytest.raises(EnvelopeDecodeError):
        decode_message(invalid_v1_event)
