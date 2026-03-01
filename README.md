# SMT DebeziumJsonParser

## Назначение

Этот SMT трансформирует указанные вложенные поля полей before и after исходной записи. 
Если значением поля является валидная JSON-строка, то она будет преобразована в JSON.

Вход:
```json lines
{
// ...
    "before" : { "a1" : "{ \"nested1\" : { \"nested11\": 80 }}" },
    "after"  : { "a1" : "{ \"nested1\" : { \"nested11\": 80 }}" },
// ...
}

```

Выход:
```json lines
// ...
{
// ...
    "before" : { "a1" : {
        "nested1" : {
            "nested11" : 80
        }
    }
    },
    "after" : {}
// ...
}
// ...

```
## Установка SMT

> [!IMPORTANT]
> Структура каталогов Apache Kafka может отличаться от примера.

**Файл: /opt/kafka/connect/config/connect-distributed.properties**
```
...
plugin.path=/opt/kafka/connect
...
```

> [!IMPORTANT]
> Важно, чтобы JAR с SMT находился в собственном каталоге, как подкаталог коннектора, к которому он относится.
```
/opt/kafka/
├── connect/                                 # Корневая директория Kafka Connect
│   ├── config/                              # Конфигурационные файлы (обычно)
│   │   ├── connect-distributed.properties
│   │   └── connect-log4j.properties
│   │
│   └── connectors/                          # Директория для плагинов
│       └── debezium-connector-postgres/     # Директория PostgreSQL коннектора
│           ├── debezium-connector-postgres-3.4.1.Final.jar
│           ├── debezium-core-3.4.1.Final.jar
│           ├── postgresql-42.7.7.jar
│           ├── ...
│           └── debezium-smt-json/                      # <----- SMT DebeziumJsonParser
│               └── debezium-smt-json-1.6.8-bundle.jar  # <----- FAT JAR со всеми зависимостями
```

## Конфигурация коннектора c SMT

```
...
"transforms":"debeziumJson",
"transforms.debeziumJson.type":"org.mtq.kafka.connect.transforms.debezium.DebeziumJsonParser",
"transforms.debeziumJson.targetFields":"val_obj_jsonb,val_arr_jsonb,val_obj_json,val_arr_json",
"transforms.debeziumJson.fail.on.error": "true"
...
```
| Свойство конфигурации                 | Значение в примере                                           | Описание                                                                                                                                                         |
|---------------------------------------|--------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| transforms                            | debeziumJson                                                 | Псевдоним SMT (любое свободное). Используется для дальнейшей настрйки.                                                                                           |
| transforms.debeziumJson.type          | org.mtq.kafka.connect.transforms.debezium.DebeziumJsonParser | Java-тип SMT.                                                                                                                                                    |
| transforms.debeziumJson.targetFields  | val_obj_jsonb,val_arr_jsonb,val_obj_json,val_arr_json        | Список полей в before и after (разделенных зяпятыми) к которым применятеся SMT.                                                                                  |
| transforms.debeziumJson.fail.on.error | "true"                                                       | Реакция на Exception. "true" - вызовет удаление регистрации коннектора при ошибке; "false" - запись останется без трансформации с записью ошибки в журнал событий. |

## Приложение: Дополнительные сведения

### Схема конверта Debezium для событий CDC (ConnectSchema)

```yaml
# Схема Debezium Envelope
debezium_envelope_schema:
  description: "Схема конверта Debezium для событий CDC"
  fields:
    - name: "before"
      type: "STRUCT"
      optional: true
      description: "Состояние строки ДО изменения (null для INSERT)"
      
    - name: "after"
      type: "STRUCT"
      optional: true
      description: "Состояние строки ПОСЛЕ изменения (null для DELETE)"
      
    - name: "source"
      type: "STRUCT"
      optional: false
      description: "Метаданные источника (БД, таблица, позиция в логе)"
      
    - name: "transaction"
      type: "STRUCT"
      optional: true
      description: "Информация о транзакции (если включено)"
      
    - name: "op"
      type: "STRING"
      optional: false
      description: "Тип операции: c(create), u(update), d(delete), r(read)"
      
    - name: "ts_ms"
      type: "INT64"
      optional: true
      description: "Время обработки события в миллисекундах"
      
    - name: "ts_us"
      type: "INT64"
      optional: true
      description: "Время обработки события в микросекундах"
      
    - name: "ts_ns"
      type: "INT64"
      optional: true
      description: "Время обработки события в наносекундах"
```
