"""
Коннектор Zigbee2MQTT для платформы Peresvet.

Коннектор подписывается на служебные топики Zigbee2MQTT, получает описание
устройств из ``bridge/devices`` и может публиковать в платформу discovery-событие
с планом создания объекта модели и тегов по ``exposes`` устройства.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from prs_connector_core import (
    BaseConnector,
    CN_Q_GOOD,
    CN_Q_SOURCE_ERROR,
    main as core_main,
    now_int,
)

DISCOVERY_ACTION = "prsConnector.zigbee2mqtt.device_discovered"
DEFAULT_BASE_TOPIC = "zigbee2mqtt"
READ_ACCESS_BIT = 1


def _payload_to_value(payload: bytes | str | Any) -> Any:
    """Преобразует MQTT payload в JSON/строку для дальнейшей обработки базовым классом."""
    if isinstance(payload, bytes):
        try:
            text = payload.decode("utf-8").strip()
        except Exception:
            return None
    elif isinstance(payload, str):
        text = payload.strip()
    else:
        return payload

    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _config_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on", "да"}
    return default


def _jsonata_property(property_name: str) -> str:
    """Возвращает JSONata-выражение для поля payload."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", property_name):
        return property_name
    return '$."' + property_name.replace('"', '\\"') + '"'


def _safe_format(template: str, default: str, **values: Any) -> str:
    try:
        return template.format(**values)
    except Exception:
        return default


def _hash_json(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _topic_join(*parts: str | None) -> str:
    clean = [str(part).strip("/") for part in parts if part is not None and str(part).strip("/")]
    return "/".join(clean)


def _is_readable_expose(expose: dict[str, Any]) -> bool:
    access = expose.get("access")
    if access is None:
        return True
    try:
        return bool(int(access) & READ_ACCESS_BIT)
    except (TypeError, ValueError):
        return True


def _flatten_exposes(exposes: Any) -> list[dict[str, Any]]:
    """Возвращает плоский список читаемых свойств из Zigbee2MQTT ``exposes``."""
    result: list[dict[str, Any]] = []
    seen: set[str] = set()

    def walk(items: Any, parent: dict[str, Any] | None = None) -> None:
        if isinstance(items, list):
            for item in items:
                walk(item, parent=parent)
            return
        if not isinstance(items, dict):
            return

        prop = items.get("property")
        if prop and _is_readable_expose(items):
            prop_s = str(prop)
            if prop_s not in seen:
                merged = dict(items)
                if parent:
                    merged.setdefault("parent_type", parent.get("type"))
                    merged.setdefault("parent_name", parent.get("name") or parent.get("property"))
                result.append(merged)
                seen.add(prop_s)

        features = items.get("features")
        if isinstance(features, list):
            walk(features, parent=items)

    walk(exposes)
    return result


def _feature_value_type(feature: dict[str, Any], overrides: dict[str, Any] | None = None) -> int:
    prop = str(feature.get("property") or "")
    if overrides and prop in overrides:
        try:
            return int(overrides[prop])
        except (TypeError, ValueError):
            pass

    expose_type = str(feature.get("type") or "").lower()
    if expose_type == "numeric":
        return 1
    if expose_type == "binary":
        values = {feature.get("value_on"), feature.get("value_off"), feature.get("value_toggle")}
        values.discard(None)
        if values and all(isinstance(v, (bool, int, float)) for v in values):
            return 0
        return 2
    if expose_type in {"enum", "text"}:
        return 2
    if expose_type in {"composite", "list"}:
        return 4
    return 4


class Zigbee2MqttConnector(BaseConnector):
    """Коннектор, получающий данные и discovery-события из Zigbee2MQTT."""

    def __init__(self, config_file: str = "config.json") -> None:
        super().__init__(config_file=config_file)
        self._device_topics: set[str] = set()
        self._topic_to_tag_ids: dict[str, list[str]] = {}
        self._discovery_fingerprints: dict[str, str] = {}

    def _source_config(self) -> dict[str, Any]:
        return _as_dict(self._config_from_platfrom.prsJsonConfigString.source)

    def _base_topic(self) -> str:
        source = self._source_config()
        return str(source.get("base_topic") or source.get("baseTopic") or DEFAULT_BASE_TOPIC).strip("/")

    def _bridge_devices_topic(self) -> str:
        return _topic_join(self._base_topic(), "bridge", "devices")

    def _bridge_event_topic(self) -> str:
        return _topic_join(self._base_topic(), "bridge", "event")

    def _device_topic(self, friendly_name: str | None) -> str | None:
        if not friendly_name:
            return None
        return _topic_join(self._base_topic(), str(friendly_name))

    def _auto_create_config(self) -> dict[str, Any]:
        source = self._source_config()
        raw = (
            source.get("autoCreate")
            or source.get("auto_create")
            or source.get("autoDiscovery")
            or source.get("auto_discovery")
            or source.get("createObjects")
        )
        if isinstance(raw, bool):
            return {"enabled": raw}
        return _as_dict(raw)

    def _auto_create_enabled(self) -> bool:
        return _config_bool(self._auto_create_config().get("enabled"), default=False)

    def _build_topic_to_tags(self) -> dict[str, list[str]]:
        """Актуальный маппинг MQTT topic -> список tag_id из конфигурации тегов."""
        mapping: defaultdict[str, list[str]] = defaultdict(list)
        for tag_id, attrs in self._config_from_platfrom.tags.items():
            if not getattr(attrs, "prsActive", True):
                continue
            source = _as_dict(getattr(attrs.prsJsonConfigString, "source", None))
            topic = source.get("topic") or source.get("Topic")
            if not topic:
                device = source.get("device") or source.get("friendly_name") or source.get("friendlyName")
                topic = self._device_topic(str(device)) if device else None
            if topic:
                mapping[str(topic).strip()].append(tag_id)
        return dict(mapping)

    async def _subscribe_client(self) -> None:
        """Подписаться на служебные топики Zigbee2MQTT и топики устройств из конфигурации тегов."""
        if not self._mqtt_client:
            return

        new_mapping = self._build_topic_to_tags()
        service_topics = {self._bridge_devices_topic(), self._bridge_event_topic()}
        new_topics = set(new_mapping.keys()) | service_topics
        to_subscribe = new_topics - self._device_topics
        self._device_topics = new_topics
        self._topic_to_tag_ids = new_mapping

        for topic in sorted(to_subscribe):
            try:
                await self._mqtt_client.subscribe(topic)
                self._logger.info("Подписка на топик Zigbee2MQTT: %s", topic)
            except Exception as ex:
                self._logger.error("Ошибка подписки на топик %s: %s", topic, ex)

    async def _read_tags(self) -> None:
        """Zigbee2MQTT работает по подписке; периодического опроса тегов нет."""
        while not self._canceled:
            await asyncio.sleep(3600)

    async def _get_connector_configuration_from_platform(self, mes: dict) -> None:
        await super()._get_connector_configuration_from_platform(mes)
        await self._subscribe_client()

    async def _get_full_configuration_from_platform(self, mes: dict) -> None:
        await super()._get_full_configuration_from_platform(mes)
        await self._subscribe_client()

    async def _tags_add_or_changed(self, mes: dict, full_list: bool = False) -> None:
        await super()._tags_add_or_changed(mes, full_list=full_list)
        await self._subscribe_client()

    async def _tags_deleted(self, mes: dict) -> None:
        await super()._tags_deleted(mes)
        await self._subscribe_client()

    async def _process_message(self, message: Any) -> None:
        topic = str(getattr(message, "topic", None) or "")
        payload = getattr(message, "payload", b"") or b""
        value = _payload_to_value(payload)

        if topic == self._bridge_devices_topic():
            await self._handle_bridge_devices(value)
            return
        if topic == self._bridge_event_topic():
            await self._handle_bridge_event(value)
            return

        await self._handle_device_state(topic=topic, value=value)

    async def _handle_device_state(self, topic: str, value: Any) -> None:
        tag_ids = self._topic_to_tag_ids.get(topic) or []
        if not tag_ids:
            return

        ts_us = now_int()
        quality = CN_Q_GOOD if value is not None else CN_Q_SOURCE_ERROR
        data_items = []
        for tag_id in tag_ids:
            if tag_id in self._tag_cache:
                data_items.append({"tagId": tag_id, "data": [[ts_us, value, quality]]})

        if not data_items:
            return

        try:
            self._data_queue.put_nowait({"data": data_items})
        except asyncio.QueueFull:
            self._logger.warning("Очередь данных переполнена, сообщение по топику %s пропущено.", topic)

    async def _handle_bridge_devices(self, value: Any) -> None:
        devices: Iterable[Any]
        if isinstance(value, list):
            devices = value
        elif isinstance(value, dict) and isinstance(value.get("devices"), list):
            devices = value["devices"]
        else:
            return

        for device in devices:
            if isinstance(device, dict):
                await self._register_discovered_device(device)

    async def _handle_bridge_event(self, value: Any) -> None:
        if not isinstance(value, dict):
            return

        event_type = str(value.get("type") or "")
        if event_type not in {
            "device_joined",
            "device_announce",
            "device_interview",
            "device_renamed",
            "device_options_changed",
        }:
            return

        data = _as_dict(value.get("data"))
        device = _as_dict(data.get("device")) or data
        if event_type == "device_interview":
            status = str(data.get("status") or "").lower()
            if status and status != "successful":
                return

        await self._register_discovered_device(device)

    async def _register_discovered_device(self, device: dict[str, Any]) -> None:
        if not self._auto_create_enabled():
            return

        features = self._device_features(device)
        auto_cfg = self._auto_create_config()
        if not features and not _config_bool(auto_cfg.get("publishEmptyDevices"), default=False):
            self._logger.debug("Устройство Zigbee2MQTT без exposes пропущено: %s", device)
            return

        payload = self._build_discovery_payload(device=device, features=features)
        device_key = payload["data"]["device"]["id"]
        fingerprint = payload["data"]["deduplicationKey"]
        if self._discovery_fingerprints.get(device_key) == fingerprint:
            return

        self._discovery_fingerprints[device_key] = fingerprint
        await self._publish_discovery_payload(payload)

    def _device_features(self, device: dict[str, Any]) -> list[dict[str, Any]]:
        definition = _as_dict(device.get("definition"))
        exposes = device.get("exposes") or definition.get("exposes") or []
        features = _flatten_exposes(exposes)

        auto_cfg = self._auto_create_config()
        allow = {str(v) for v in auto_cfg.get("includeProperties") or auto_cfg.get("include_properties") or []}
        deny = {str(v) for v in auto_cfg.get("excludeProperties") or auto_cfg.get("exclude_properties") or []}
        if allow:
            features = [feature for feature in features if str(feature.get("property")) in allow]
        if deny:
            features = [feature for feature in features if str(feature.get("property")) not in deny]
        return features

    def _build_discovery_payload(self, device: dict[str, Any], features: list[dict[str, Any]]) -> dict[str, Any]:
        auto_cfg = self._auto_create_config()
        friendly_name = self._device_friendly_name(device)
        ieee_address = self._device_ieee_address(device)
        device_id = ieee_address or friendly_name
        topic = self._device_topic(friendly_name)
        parent_id = auto_cfg.get("parentId") or auto_cfg.get("parent_id")
        object_attrs = dict(auto_cfg.get("objectAttributes") or auto_cfg.get("object_attributes") or {})
        object_cn_default = friendly_name or device_id or "zigbee-device"
        object_cn_template = str(auto_cfg.get("objectCnTemplate") or auto_cfg.get("object_cn_template") or "{friendly_name}")
        object_attrs.setdefault(
            "cn",
            _safe_format(
                object_cn_template,
                object_cn_default,
                friendly_name=friendly_name or "",
                ieee_address=ieee_address or "",
                model=device.get("model") or "",
                vendor=device.get("vendor") or "",
            ),
        )
        object_attrs.setdefault(
            "description",
            f"Zigbee2MQTT device {friendly_name or device_id}",
        )
        object_attrs.setdefault(
            "prsJsonConfigString",
            {
                "source": "zigbee2mqtt",
                "zigbee2mqtt": {
                    "friendlyName": friendly_name,
                    "ieeeAddress": ieee_address,
                    "topic": topic,
                    "model": device.get("model"),
                    "vendor": device.get("vendor"),
                },
            },
        )

        object_payload: dict[str, Any] = {"attributes": object_attrs}
        if parent_id:
            object_payload["parentId"] = parent_id

        tags = [self._build_feature_payload(device, feature, topic) for feature in features]
        dedup_source = {
            "connectorId": self._config_from_file.id,
            "deviceId": device_id,
            "topic": topic,
            "features": [tag["property"] for tag in tags],
            "object": object_payload,
        }

        return {
            "action": DISCOVERY_ACTION,
            "data": {
                "id": self._config_from_file.id,
                "connectorId": self._config_from_file.id,
                "deduplicationKey": _hash_json(dedup_source),
                "device": {
                    "id": device_id,
                    "friendlyName": friendly_name,
                    "ieeeAddress": ieee_address,
                    "topic": topic,
                    "model": device.get("model"),
                    "vendor": device.get("vendor"),
                    "manufacturer": device.get("manufacturer"),
                    "definition": device.get("definition"),
                },
                "model": {
                    "object": object_payload,
                    "tags": tags,
                },
                "raw": device,
            },
        }

    def _build_feature_payload(
        self,
        device: dict[str, Any],
        feature: dict[str, Any],
        topic: str | None,
    ) -> dict[str, Any]:
        auto_cfg = self._auto_create_config()
        value_type_overrides = _as_dict(auto_cfg.get("valueTypeOverrides") or auto_cfg.get("value_type_overrides"))
        property_name = str(feature.get("property"))
        friendly_name = self._device_friendly_name(device)
        ieee_address = self._device_ieee_address(device)
        feature_name = str(feature.get("name") or property_name)

        tag_cn_template = str(
            auto_cfg.get("tagCnTemplate")
            or auto_cfg.get("tag_cn_template")
            or "{property}"
        )
        tag_attrs = dict(auto_cfg.get("tagAttributes") or auto_cfg.get("tag_attributes") or {})
        tag_attrs.setdefault(
            "cn",
            _safe_format(
                tag_cn_template,
                property_name,
                device=friendly_name or "",
                friendly_name=friendly_name or "",
                ieee_address=ieee_address or "",
                property=property_name,
                name=feature_name,
            ),
        )
        tag_attrs.setdefault("description", feature.get("description") or f"{friendly_name}: {property_name}")
        tag_attrs.setdefault("prsValueTypeCode", _feature_value_type(feature, value_type_overrides))
        if feature.get("unit") and "prsMeasureUnits" not in tag_attrs:
            tag_attrs["prsMeasureUnits"] = feature["unit"]

        max_dev = auto_cfg.get("maxDev", auto_cfg.get("max_dev", 0))
        link_cfg = dict(auto_cfg.get("linkAttributes") or auto_cfg.get("link_attributes") or {})
        link_json = dict(link_cfg.get("prsJsonConfigString") or {})
        link_json.setdefault(
            "source",
            {
                "topic": topic,
                "device": friendly_name,
                "ieeeAddress": ieee_address,
                "property": property_name,
            },
        )
        link_json.setdefault("maxDev", max_dev)
        link_json.setdefault("JSONata", _jsonata_property(property_name))
        link_cfg["prsJsonConfigString"] = link_json

        return {
            "property": property_name,
            "name": feature_name,
            "expose": feature,
            "tag": {
                "parentRef": "device",
                "attributes": tag_attrs,
            },
            "link": {
                "connectorId": self._config_from_file.id,
                "tagRef": "createdTag",
                "attributes": link_cfg,
            },
        }

    def _device_friendly_name(self, device: dict[str, Any]) -> str | None:
        value = device.get("friendly_name") or device.get("friendlyName") or device.get("name")
        return str(value) if value else self._device_ieee_address(device)

    def _device_ieee_address(self, device: dict[str, Any]) -> str | None:
        value = device.get("ieee_address") or device.get("ieeeAddress") or device.get("ieeeAddr")
        return str(value) if value else None

    async def _publish_discovery_payload(self, payload: dict[str, Any]) -> None:
        if not self._mqtt_client or not self._mqtt_connected.is_set():
            self._logger.debug("Discovery Zigbee2MQTT подготовлен, но связь с платформой ещё не установлена.")
            return

        try:
            await self._mqtt_client.publish(
                f"conn2prs/{self._config_from_file.id}",
                payload=json.dumps(payload, ensure_ascii=False),
                retain=False,
            )
            device = payload["data"]["device"]
            self._logger.info(
                "Отправлено discovery-событие Zigbee2MQTT для устройства %s.",
                device.get("friendlyName") or device.get("id"),
            )
        except Exception as ex:
            self._logger.error("Не удалось отправить discovery-событие Zigbee2MQTT: %s", ex)


def main_entry() -> None:
    """Точка входа для консольной команды."""
    core_main(Zigbee2MqttConnector)


if __name__ == "__main__":
    main_entry()
