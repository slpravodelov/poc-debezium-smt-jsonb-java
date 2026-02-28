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
"transforms":"jsonStringValueParser",
"transforms.jsonStringValueParser.type":"org.mtq.kafka.connect.transforms.debezium.JsonStringValueParser",
"transforms.jsonStringValueParser.targetFields":"val_obj_jsonb,val_arr_jsonb,val_obj_json,val_arr_json"
```
