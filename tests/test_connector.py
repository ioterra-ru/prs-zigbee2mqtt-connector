import asyncio
import json
from types import SimpleNamespace

import pytest

from prs_zigbee2mqtt_connector.connector import (
    DISCOVERY_ACTION,
    Zigbee2MqttConnector,
    _flatten_exposes,
    _jsonata_property,
    _payload_to_value,
)


class DummyLogger:
    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class FakeMqttClient:
    def __init__(self):
        self.publishes = []
        self.subscriptions = []

    async def publish(self, topic, payload=None, **kwargs):
        self.publishes.append({"topic": topic, "payload": payload, **kwargs})

    async def subscribe(self, topic):
        self.subscriptions.append(topic)


def make_connector(source=None):
    conn = Zigbee2MqttConnector.__new__(Zigbee2MqttConnector)
    conn._config_from_file = SimpleNamespace(id="11111111-1111-1111-1111-111111111111")
    conn._config_from_platfrom = SimpleNamespace(
        prsJsonConfigString=SimpleNamespace(source=source or {}),
        tags={},
    )
    conn._mqtt_client = FakeMqttClient()
    conn._mqtt_connected = asyncio.Event()
    conn._mqtt_connected.set()
    conn._logger = DummyLogger()
    conn._discovery_fingerprints = {}
    conn._topic_to_tag_ids = {}
    conn._tag_cache = {}
    conn._data_queue = asyncio.Queue()
    conn._canceled = False
    return conn


def test_payload_to_value_parses_json_and_plain_text():
    assert _payload_to_value(b'{"temperature": 21.5}') == {"temperature": 21.5}
    assert _payload_to_value(b"ON") == "ON"
    assert _payload_to_value(b"") is None


def test_flatten_exposes_keeps_readable_properties_once():
    exposes = [
        {"type": "numeric", "property": "temperature", "access": 1, "unit": "C"},
        {"type": "numeric", "property": "temperature", "access": 1},
        {"type": "binary", "property": "occupancy", "access": 7},
        {"type": "numeric", "property": "write_only", "access": 2},
        {
            "type": "composite",
            "name": "color",
            "features": [
                {"type": "numeric", "property": "x", "access": 1},
                {"type": "numeric", "property": "y", "access": 1},
            ],
        },
    ]

    features = _flatten_exposes(exposes)

    assert [feature["property"] for feature in features] == ["temperature", "occupancy", "x", "y"]
    assert features[2]["parent_name"] == "color"


def test_jsonata_property_quotes_non_identifier_properties():
    assert _jsonata_property("temperature") == "temperature"
    assert _jsonata_property("battery-low") == '$."battery-low"'


@pytest.mark.asyncio
async def test_bridge_devices_publishes_discovery_payload_once():
    conn = make_connector(
        {
            "base_topic": "zigbee2mqtt",
            "autoCreate": {
                "enabled": True,
                "parentId": "22222222-2222-2222-2222-222222222222",
                "tagCnTemplate": "{device}_{property}",
                "objectAttributes": {"prsActive": True},
            },
        }
    )
    device = {
        "friendly_name": "kitchen_sensor",
        "ieee_address": "0x00158d0000000001",
        "model": "WSDCGQ11LM",
        "vendor": "Aqara",
        "definition": {
            "exposes": [
                {"type": "numeric", "property": "temperature", "access": 1, "unit": "C"},
                {"type": "binary", "property": "occupancy", "access": 1, "value_on": True, "value_off": False},
            ]
        },
    }

    await conn._handle_bridge_devices([device])
    await conn._handle_bridge_devices([device])

    assert len(conn._mqtt_client.publishes) == 1
    published = conn._mqtt_client.publishes[0]
    assert published["topic"] == "conn2prs/11111111-1111-1111-1111-111111111111"

    payload = json.loads(published["payload"])
    assert payload["action"] == DISCOVERY_ACTION
    assert payload["data"]["device"]["friendlyName"] == "kitchen_sensor"
    assert payload["data"]["model"]["object"]["parentId"] == "22222222-2222-2222-2222-222222222222"

    tags = payload["data"]["model"]["tags"]
    assert [tag["property"] for tag in tags] == ["temperature", "occupancy"]
    assert tags[0]["tag"]["attributes"]["cn"] == "kitchen_sensor_temperature"
    assert tags[0]["tag"]["attributes"]["prsValueTypeCode"] == 1
    assert tags[0]["link"]["attributes"]["prsJsonConfigString"]["source"]["topic"] == "zigbee2mqtt/kitchen_sensor"
    assert tags[0]["link"]["attributes"]["prsJsonConfigString"]["JSONata"] == "temperature"
    assert tags[1]["tag"]["attributes"]["prsValueTypeCode"] == 0


@pytest.mark.asyncio
async def test_device_state_puts_one_point_for_each_tag_on_topic():
    conn = make_connector()
    conn._topic_to_tag_ids = {"zigbee2mqtt/kitchen_sensor": ["tag-1", "tag-2", "tag-3"]}
    conn._tag_cache = {"tag-1": {}, "tag-2": {}}

    await conn._handle_device_state(
        topic="zigbee2mqtt/kitchen_sensor",
        value={"temperature": 21.5, "humidity": 44},
    )

    queued = await conn._data_queue.get()
    assert [item["tagId"] for item in queued["data"]] == ["tag-1", "tag-2"]
    assert queued["data"][0]["data"][0][1] == {"temperature": 21.5, "humidity": 44}


@pytest.mark.asyncio
async def test_subscribe_client_adds_bridge_and_configured_device_topics():
    conn = make_connector({"baseTopic": "z2m"})
    conn._device_topics = set()
    conn._config_from_platfrom.tags = {
        "tag-1": SimpleNamespace(
            prsActive=True,
            prsJsonConfigString=SimpleNamespace(source={"topic": "z2m/sensor_1"}),
        ),
        "tag-2": SimpleNamespace(
            prsActive=True,
            prsJsonConfigString=SimpleNamespace(source={"device": "sensor_2"}),
        ),
    }

    await conn._subscribe_client()

    assert conn._topic_to_tag_ids == {
        "z2m/sensor_1": ["tag-1"],
        "z2m/sensor_2": ["tag-2"],
    }
    assert set(conn._mqtt_client.subscriptions) == {
        "z2m/bridge/devices",
        "z2m/bridge/event",
        "z2m/sensor_1",
        "z2m/sensor_2",
    }
