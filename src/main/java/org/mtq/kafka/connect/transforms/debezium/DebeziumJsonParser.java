package org.mtq.kafka.connect.transforms.debezium;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DebeziumJsonParser<R extends org.apache.kafka.connect.connector.ConnectRecord<R>>
        implements org.apache.kafka.connect.transforms.Transformation<R>, org.apache.kafka.connect.components.Versioned {

    private static final String VERSION = "1.6.8";
    private static final String DEBEZIUM_DATA_BEFORE_FIELD = "before";
    private static final String DEBEZIUM_DATA_AFTER_FIELD = "after";

    private static final String FIELD_CONFIG = "targetFields";
    private static final String FAIL_ON_ERROR_CONFIG = "fail.on.error";

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumJsonParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
        this.failOnError = (failObj == null) || Boolean.parseBoolean(failObj.toString());
    }

    @Override
    public R apply(R record) {

        java.time.Instant digEvtStart = java.time.Instant.now();

        if (record.value() == null) {
            return record;
        }

        if (record.valueSchema() == null) {
            LOG.warn("record.valueSchema() is NULL:: topic:{}, key:{}", record.topic(), record.key());
            return record;
        } else {
            try {
                return rebuildRecord(record);
            } catch (JsonProcessingException e) {
                if(this.failOnError){
                    throw new org.apache.kafka.connect.errors.DataException(e);
                }
                LOG.error("Unable to transform record:: topic:{}, key:{}", record.topic(), record.key());
                return record;
            } finally {
                java.time.Instant digEvtEnd = java.time.Instant.now();
                java.time.Duration diagEvtStartDuration = java.time.Duration.between(digEvtStart, digEvtEnd);

                LOG.debug("record:: topic:{}, key:{} transformed in: {} ms",
                        record.topic(),record.key(),diagEvtStartDuration.toMillis());
            }
        }
    }

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return new org.apache.kafka.common.config.ConfigDef()
                .define(FIELD_CONFIG,
                        org.apache.kafka.common.config.ConfigDef.Type.LIST,
                        org.apache.kafka.common.config.ConfigDef.Importance.HIGH,
                        "List of JSON field names to parse")
                .define(FAIL_ON_ERROR_CONFIG,
                        org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN,
                        false,
                        org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM,
                        "JSON parsing error");
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return VERSION;
    }

    private R rebuildRecord(R origRecord) throws JsonProcessingException {

        var recordSchemaBuilder = org.apache.kafka.connect.data.SchemaBuilder
                .struct()
                .name(origRecord.valueSchema().name() + "_parsed_json");

        var origRecordValue = (org.apache.kafka.connect.data.Struct) origRecord.value();

        for (var origRecordField : origRecord.valueSchema().fields()) {
            if (origRecordField.name().equals(DEBEZIUM_DATA_BEFORE_FIELD)) {
                recordSchemaBuilder.field(origRecordField.name(),
                        rebuildBeforeSchema(origRecordField, origRecordValue));
            } else if (origRecordField.name().equals(DEBEZIUM_DATA_AFTER_FIELD)) {
                recordSchemaBuilder.field(origRecordField.name(),
                        rebuildAfterSchema(origRecordField, origRecordValue));
            } else {
                recordSchemaBuilder.field(origRecordField.name(), origRecordField.schema());
            }
        }

        var transformedRecordSchema = recordSchemaBuilder.build();
        var transformedRecordValue = new org.apache.kafka.connect.data.Struct(transformedRecordSchema);

        // Populate record values
        for (var f : origRecord.valueSchema().fields()) {
            // Skip transformed fields containers (i.e. before & after in Debezium schema)
            if (f.name().equals(DEBEZIUM_DATA_BEFORE_FIELD) || f.name().equals(DEBEZIUM_DATA_AFTER_FIELD)) {
                continue;
            }
            transformedRecordValue.put(f.name(), ((org.apache.kafka.connect.data.Struct) origRecord.value()).get(f.name()));
        }

        // Populate record transformed values
        var origBeforeValue = origRecordValue.get(DEBEZIUM_DATA_BEFORE_FIELD);
        var origAfterValue = origRecordValue.get(DEBEZIUM_DATA_AFTER_FIELD);

        if (origBeforeValue != null) {
            var newRecordBeforeValue =
                    rebuildFieldValue(
                            (org.apache.kafka.connect.data.Struct) origBeforeValue,
                            transformedRecordSchema.field(DEBEZIUM_DATA_BEFORE_FIELD).schema(),
                            DEBEZIUM_DATA_BEFORE_FIELD
                    );

            transformedRecordValue.put(DEBEZIUM_DATA_BEFORE_FIELD, newRecordBeforeValue);
        } else {
            transformedRecordValue.put(DEBEZIUM_DATA_BEFORE_FIELD, null);
        }

        if (origAfterValue != null) {
            var newRecordAfterValue =
                    rebuildFieldValue(
                            (org.apache.kafka.connect.data.Struct) origAfterValue,
                            transformedRecordSchema.field(DEBEZIUM_DATA_AFTER_FIELD).schema(),
                            DEBEZIUM_DATA_AFTER_FIELD
                    );

            transformedRecordValue.put(DEBEZIUM_DATA_AFTER_FIELD, newRecordAfterValue);
        } else {
            transformedRecordValue.put(DEBEZIUM_DATA_AFTER_FIELD, null);
        }

        return origRecord.newRecord(
                origRecord.topic(),
                origRecord.kafkaPartition(),
                origRecord.keySchema(),
                origRecord.key(),
                transformedRecordSchema,
                transformedRecordValue,
                origRecord.timestamp()
        );
    }

    private org.apache.kafka.connect.data.Struct rebuildFieldValue(org.apache.kafka.connect.data.Struct origRecordFieldValue, org.apache.kafka.connect.data.Schema fieldSchema, String fieldName)
            throws JsonProcessingException {

        var recordFieldValue = new org.apache.kafka.connect.data.Struct(fieldSchema);

        for (var f : fieldSchema.fields()) {
            if (isTargetField(f.name())) {
                recordFieldValue.put(f.name(), jsonToFieldValue(OBJECT_MAPPER.readTree(origRecordFieldValue.getString(f.name())), f.schema()));
            } else {
                recordFieldValue.put(f.name(), origRecordFieldValue.get(f.name()));
            }
        }

        return recordFieldValue;
    }

    private org.apache.kafka.connect.data.Schema rebuildBeforeSchema(org.apache.kafka.connect.data.Field origBeforeField, org.apache.kafka.connect.data.Struct origRecordValue) {
        var origRecordBeforeValue = (org.apache.kafka.connect.data.Struct) origRecordValue.get(DEBEZIUM_DATA_BEFORE_FIELD);

        if (origRecordBeforeValue == null) {
            // CREATE EVENT
            return org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
        }

        var origBeforeFieldSchema = origBeforeField.schema();
        var schemaBuilder = org.apache.kafka.connect.data.SchemaBuilder
                .struct()
                .name(origBeforeField.name() + "_before_field_parsed");

        for (var f : origBeforeFieldSchema.fields()) {
            if (isTargetField(f.name()) && (f.schema().type() == org.apache.kafka.connect.data.Schema.Type.STRING)) {
                var origRecordBeforeFieldValue = (String) origRecordBeforeValue.get(f.name());
                schemaBuilder.field(f.name(), DebeziumJsonSchemaBuilder.buildSchema(origRecordBeforeFieldValue));
            } else {
                schemaBuilder.field(f.name(), f.schema());
            }
        }

        return schemaBuilder.build();
    }

    private org.apache.kafka.connect.data.Schema rebuildAfterSchema(org.apache.kafka.connect.data.Field origAfterField, org.apache.kafka.connect.data.Struct origRecordValue) {
        var origRecordAfterValue = (org.apache.kafka.connect.data.Struct) origRecordValue.get(DEBEZIUM_DATA_AFTER_FIELD);

        if (origRecordAfterValue == null) {
            // DELETE EVENT
            return org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
        }

        var origBeforeFieldSchema = origAfterField.schema();
        var schemaBuilder = org.apache.kafka.connect.data.SchemaBuilder
                .struct()
                .name(origAfterField.name() + "_after_field_parsed");

        for (var f : origBeforeFieldSchema.fields()) {
            if (isTargetField(f.name()) && (f.schema().type() == org.apache.kafka.connect.data.Schema.Type.STRING)) {
                var origRecordBeforeFieldValue = (String) origRecordAfterValue.get(f.name());
                schemaBuilder.field(f.name(), DebeziumJsonSchemaBuilder.buildSchema(origRecordBeforeFieldValue));
            } else {
                schemaBuilder.field(f.name(), f.schema());
            }
        }

        return schemaBuilder.build();
    }

    private boolean isTargetField(String fieldName) {
        return targetFields.contains(fieldName);
    }

    private Object jsonToFieldValue(JsonNode node, org.apache.kafka.connect.data.Schema targetSchema) {
        if (node == null || node.isNull()) {
            return null;
        }

        return switch (targetSchema.type()) {
            case BOOLEAN -> node.asBoolean();
            case INT32 -> node.asInt();
            case INT64 -> node.asLong();
            case FLOAT64 -> node.asDouble();
            case STRING -> node.asText();
            case STRUCT -> jsonNodeToStruct(node, targetSchema);
            case ARRAY -> jsonNodeToList(node, targetSchema.valueSchema());
            case MAP -> jsonNodeToMap(node);
            default -> node.toString();
        };
    }

    private org.apache.kafka.connect.data.Struct jsonNodeToStruct(JsonNode node, org.apache.kafka.connect.data.Schema schema) {
        if (!node.isObject()) {
            throw new org.apache.kafka.connect.errors.DataException("Expected JSON object, got: " + node.getNodeType());
        }

        var struct = new org.apache.kafka.connect.data.Struct(schema);
        node.fields().forEachRemaining(entry -> {
            org.apache.kafka.connect.data.Field field = schema.field(entry.getKey());
            if (field != null) {
                Object value = jsonToFieldValue(entry.getValue(), field.schema());
                struct.put(field.name(), value);
            }
        });

        return struct;
    }

    private List<Object> jsonNodeToList(JsonNode node, org.apache.kafka.connect.data.Schema elementSchema) {
        List<Object> list = new ArrayList<>();
        node.forEach(item -> list.add(jsonToFieldValue(item, elementSchema)));
        return list;
    }

    private Map<String, Object> jsonNodeToMap(JsonNode node) {
        Map<String, Object> map = new HashMap<>();
        node.fields().forEachRemaining(entry ->
                map.put(entry.getKey(), convertJsonNodeToJava(entry.getValue()))
        );
        return map;
    }

    private Object convertJsonNodeToJava(JsonNode node) {
        if (node.isNull()) return null;
        if (node.isBoolean()) return node.asBoolean();
        if (node.isInt()) return node.asInt();
        if (node.isLong()) return node.asLong();
        if (node.isDouble()) return node.asDouble();
        if (node.isTextual()) return node.asText();
        if (node.isArray()) {
            List<Object> list = new ArrayList<>();
            node.forEach(n -> list.add(convertJsonNodeToJava(n)));
            return list;
        }
        if (node.isObject()) {
            Map<String, Object> map = new HashMap<>();
            node.fields().forEachRemaining(entry ->
                    map.put(entry.getKey(), convertJsonNodeToJava(entry.getValue()))
            );
            return map;
        }
        return node.toString();
    }
}