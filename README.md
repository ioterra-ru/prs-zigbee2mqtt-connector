# prs-zigbee2mqtt-connector

Коннектор для платформы Пересвет, который получает данные от
[Zigbee2MQTT](https://github.com/Koenkk/zigbee2mqtt) и передаёт их в теги
платформы через базовый класс
[prs-connector-core](https://github.com/mp-co-ru/prs-connector-core).

Главное отличие от обычного MQTT-коннектора: Zigbee2MQTT публикует описание
устройств и их параметров в служебных топиках `bridge/devices` и `bridge/event`.
Коннектор умеет читать эти сообщения и, если включён параметр `autoCreate`,
публиковать в платформу структурированное discovery-событие с планом создания
объекта модели и тегов по параметрам устройства.

## Установка

```bash
pip install .
```

После установки доступна команда:

```bash
prs-zigbee2mqtt-connector --config config.json
```

Файл `config.json` содержит стандартные настройки коннектора Пересвет:

```json
{
  "id": "uuid-коннектора-из-платформы",
  "url": "mqtt://user:password@host:1883"
}
```

## Конфигурация коннектора в платформе

Настройки источника задаются в `prsJsonConfigString.source` коннектора.

```json
{
  "source": {
    "base_topic": "zigbee2mqtt",
    "autoCreate": {
      "enabled": true,
      "parentId": "uuid-родительского-объекта",
      "objectCnTemplate": "{friendly_name}",
      "tagCnTemplate": "{device}_{property}",
      "maxDev": 0,
      "includeProperties": [],
      "excludeProperties": [],
      "valueTypeOverrides": {
        "action": 2
      },
      "objectAttributes": {
        "prsActive": true
      },
      "tagAttributes": {
        "prsActive": true
      },
      "linkAttributes": {}
    }
  },
  "log": {
    "level": "INFO"
  }
}
```

Поля `autoCreate`:

| Поле | Описание |
| --- | --- |
| `enabled` | Включает публикацию discovery-событий. По умолчанию выключено. |
| `parentId` | Родительский объект в модели Пересвет, под которым нужно создать объект устройства. |
| `objectCnTemplate` | Шаблон имени объекта. Доступны `{friendly_name}`, `{ieee_address}`, `{model}`, `{vendor}`. |
| `tagCnTemplate` | Шаблон имени тегов. Доступны `{device}`, `{friendly_name}`, `{ieee_address}`, `{property}`, `{name}`. |
| `includeProperties` | Если задано, создавать теги только для перечисленных свойств Zigbee2MQTT. |
| `excludeProperties` | Свойства, для которых не нужно создавать теги. |
| `valueTypeOverrides` | Явное сопоставление свойства с `prsValueTypeCode`. |
| `objectAttributes` | Дополнительные атрибуты создаваемого объекта. |
| `tagAttributes` | Дополнительные атрибуты создаваемых тегов. |
| `linkAttributes` | Дополнительные атрибуты привязки тегов к коннектору. |

Коннектор публикует discovery-событие в `conn2prs/<connector_id>`:

```json
{
  "action": "prsConnector.zigbee2mqtt.device_discovered",
  "data": {
    "id": "connector_id",
    "connectorId": "connector_id",
    "deduplicationKey": "sha256",
    "device": {
      "friendlyName": "kitchen_sensor",
      "ieeeAddress": "0x00158d0000000001",
      "topic": "zigbee2mqtt/kitchen_sensor"
    },
    "model": {
      "object": {
        "parentId": "uuid-родительского-объекта",
        "attributes": {
          "cn": "kitchen_sensor"
        }
      },
      "tags": [
        {
          "property": "temperature",
          "tag": {
            "parentRef": "device",
            "attributes": {
              "cn": "kitchen_sensor_temperature",
              "prsValueTypeCode": 1,
              "prsMeasureUnits": "C"
            }
          },
          "link": {
            "connectorId": "connector_id",
            "tagRef": "createdTag",
            "attributes": {
              "prsJsonConfigString": {
                "source": {
                  "topic": "zigbee2mqtt/kitchen_sensor",
                  "device": "kitchen_sensor",
                  "property": "temperature"
                },
                "JSONata": "temperature",
                "maxDev": 0
              }
            }
          }
        }
      ]
    }
  }
}
```

Обработчик на стороне платформы должен принять это действие, создать объект,
создать теги под ним и привязать теги к коннектору с указанным
`prsJsonConfigString`. Сам базовый класс `prs-connector-core` не меняется.

## Привязка существующих тегов

Если объект и теги уже созданы вручную, для каждого тега достаточно указать
источник в `prsJsonConfigString.source` привязки тега к коннектору.

Вариант с явным топиком:

```json
{
  "source": {
    "topic": "zigbee2mqtt/kitchen_sensor"
  },
  "JSONata": "temperature",
  "maxDev": 0
}
```

Вариант через имя устройства и `base_topic` коннектора:

```json
{
  "source": {
    "device": "kitchen_sensor",
    "property": "temperature"
  },
  "JSONata": "temperature",
  "maxDev": 0
}
```

Payload устройства передаётся в базовый класс целиком; значение конкретного тега
извлекается через `JSONata`.

## Подписки

Коннектор подписывается на:

- `<base_topic>/bridge/devices` — список устройств и `exposes`;
- `<base_topic>/bridge/event` — события добавления/интервью/переименования;
- топики устройств из конфигурации привязанных тегов.

## Лицензия

Apache-2.0.
