# SMT DebeziumJsonTransform

## Назначение

Переписывает значения полей, указанных в targetFields переданных в виде экранированной JSON-строки в JSON.

Вход:
```
{
...
    "before" : { "a1" : "{ \"nested1\" : { \"nested11\": 80 }}" },
    "after"  : { "a1" : "{ \"nested1\" : { \"nested11\": 80 }}" },
...
}

```

Выход:
```
...
{
    "before" : { "a1" : {
        "nested1" : {
            "nested11" : 80
        }
    }
    },
    "after" : {}
}
...

```
## Установка

```
plugin.path=/opt/kafka/connect
```

```
/opt/kafka/connect/connectors
/opt/kafka/connect/connectors/debezium
/opt/kafka/connect/connectors/debezium/debezium-smt-json
/opt/kafka/connect/plugins

|
| - 

```

## Конфигурация SMT к модулю Kafka Connect (Debezium):

```
...
"transforms":"debeziumJson",
"transforms.debeziumJson.type":"org.mtq.kafka.connect.transforms.debezium.debeziumJsonParser",
"transforms.debeziumJson.targetFields":"val_obj_jsonb,val_arr_jsonb,val_obj_json,val_arr_json"
...
```

## Схема записи Debezium (ConnectSchema)

```
/*
 * Debezium ConnectSchema fields:
 *
 * - Field: 'before'
 *   type: STRUCT
 *   optional: true
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'after'
 *   type: STRUCT
 *   optional: true
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'source'
 *   type: STRUCT
 *   optional: false
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'transaction'
 *   type: STRUCT
 *   optional: true
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'op'
 *   type: STRING
 *   optional: false
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'ts_ms'
 *   type: INT64
 *   optional: true
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'ts_us'
 *   type: INT64
 *   optional: true
 *   class: org.apache.kafka.connect.data.ConnectSchema
 *
 * - Field: 'ts_ns'
 *   type: INT64
 *   optional: true
 *   class: org.apache.kafka.connect.data.ConnectSchema
 */
```
