package org.mtq.kafka.connect.transforms.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonStringValueParser<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(JsonStringValueParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String FIELD_CONFIG = "process.fields";
    private static final String FAIL_ON_ERROR_CONFIG = "fail.on.error";

    private Set<String> targetFields;
    private boolean failOnError;

    @Override
    public void configure(Map<String, ?> props) {
        Object fieldsObj = props.get(FIELD_CONFIG);
        if (fieldsObj instanceof List) {
            this.targetFields = new HashSet<>((List<String>) fieldsObj);
        } else if (fieldsObj instanceof String) {
            String[] fields = ((String) fieldsObj).split(",");
            this.targetFields = new HashSet<>();
            for (String field : fields) {
                this.targetFields.add(field.trim());
            }
        } else {
            this.targetFields = new HashSet<>();
        }

        Object failObj = props.get(FAIL_ON_ERROR_CONFIG);
        this.failOnError = failObj == null || Boolean.parseBoolean(failObj.toString());

        log.info("JsonStringValueParser configured for fields: {}", targetFields);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }

        try {
            Struct originalValue = (Struct) record.value();
            Schema originalSchema = record.valueSchema();

            SchemaBuilder outSchemaBuilder = SchemaBuilder.struct();

            if (originalSchema.name() != null) {
                outSchemaBuilder.name(originalSchema.name());
            }

            for (Field field : originalSchema.fields()) {
                if (shouldTransform(field.name())) {
                    String jsonString = originalValue.get(field).toString();
                    Schema jsonSchema = new JsonStringSchemaBuilder().convert(jsonString);
                    outSchemaBuilder.field(field.name(), jsonSchema);
                } else {
                    outSchemaBuilder.field(field.name(), field.schema());
                }
            }

            Schema outSchema = outSchemaBuilder.build();
            Struct outValue = new Struct(outSchema);
            
            for (Field field : originalSchema.fields()) {
                Object fieldValue = originalValue.get(field);

                if (shouldTransform(field.name()) && fieldValue != null) {
                    try {
                        Object parsedValue = parseJsonToConnect(fieldValue.toString(), outSchema.field(field.name()).schema());
                        outValue.put(field.name(), parsedValue);
                    } catch (Exception e) {
                        log.warn("Failed to parse JSON for field {}: {}", field.name(), e.getMessage());

                        if (failOnError) {
                            throw new DataException("Failed to parse JSON for field:" + field.name(), e);
                        }

                        outValue.put(field.name(), fieldValue);
                    }
                } else {
                    outValue.put(field.name(), fieldValue);
                }
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    outSchema,
                    outValue,
                    record.timestamp(),
                    record.headers()
            );

        } catch (Exception e) {
            log.error("Error in transformation: {}", e.getMessage(), e);

            if (failOnError) {
                throw new DataException("Failed to transform record", e);
            }

            return record;
        }
    }

    // Новый метод для парсинга JSON в Connect типы
    private Object parseJsonToConnect(String jsonString, Schema targetSchema) throws Exception {
        JsonNode rootNode = OBJECT_MAPPER.readTree(jsonString);
        return convertJsonNodeToConnect(rootNode, targetSchema);
    }

    private Object convertJsonNodeToConnect(JsonNode node, Schema schema) {
        if (node == null || node.isNull()) {
            return null;
        }

        switch (schema.type()) {
            case STRUCT:
                Struct struct = new Struct(schema);
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String fieldName = field.getKey();
                    JsonNode fieldValue = field.getValue();
                    Schema fieldSchema = schema.field(fieldName).schema();
                    struct.put(fieldName, convertJsonNodeToConnect(fieldValue, fieldSchema));
                }
                return struct;

            case ARRAY:
                List<Object> list = new ArrayList<>();
                Schema elementSchema = schema.valueSchema();
                for (JsonNode element : node) {
                    list.add(convertJsonNodeToConnect(element, elementSchema));
                }
                return list;

            case INT32:
                return node.asInt();

            case INT64:
                return node.asLong();

            case FLOAT64:
                return node.asDouble();

            case BOOLEAN:
                return node.asBoolean();

            default:
                return node.asText();
        }
    }

    private boolean shouldTransform(String fieldName) {
        return targetFields != null && targetFields.contains(fieldName);
    }



    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        "List of JSON field names to parse")
                .define(FAIL_ON_ERROR_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.MEDIUM,
                        "JSON parsing error");
    }

    @Override
    public void close() {
    }
}