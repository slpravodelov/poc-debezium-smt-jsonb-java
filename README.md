# JsonStringValueParser: Парсер JSON-строк в JSON

## Назначение

Переписывает значения полей, переданных, как экроанированная JSON-строка в вид JSON.

было:

```
{
    "a1" : "{ \"nested1\" : { \"nested11\": 80 }}"
}

```

стало:

```
{
    "a1" : {
        "nested1" : {
            "nested11" : 80
        }
    }
}
```

## Конфигурация к модулю Kafka Connect (Debezium):

```
"transforms": "jsonStringParser",
"transforms.jsonStringParser.type": "org.mtq.kafka.connect.transforms.debezium.JsonStringValueParser",
"transforms.jsonStringParser.process.fields": "my_db_json_field1, my_db_json_field2",
```
