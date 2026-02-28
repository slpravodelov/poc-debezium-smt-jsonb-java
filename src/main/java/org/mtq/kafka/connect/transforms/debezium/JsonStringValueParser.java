package org.mtq.kafka.connect.transforms.debezium;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonStringValueParser<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final String VERSION = "1.6.7";
    private static final String DEBEZIUM_DATA_BEFORE_FIELD = "before";
    private static final String DEBEZIUM_DATA_AFTER_FIELD = "after";

    private static final Logger log = LoggerFactory.getLogger(JsonStringValueParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String FIELD_CONFIG = "targetFields";
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
        if (record.value() == null) {
            return record;
        }

        // Always NOT NULL in success case
        if (record.valueSchema() == null){
            log.warn("");
            return record;
        } else {
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

            // TODO: Schema mode (ConnectSchema)

            try {
                return rebuildRecord(record);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
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

    @Override
    public String version() {
        return VERSION;
    }

    private R rebuildRecord(R origRecord) throws JsonProcessingException {

        long startTime = System.nanoTime();

        var recordSchemaBuilder = SchemaBuilder
                .struct()
                .name(origRecord.valueSchema().name() + "_parsed_json");

        var origRecordValue = (Struct) origRecord.value();

        for (var origRecordField : origRecord.valueSchema().fields()) {
            if (origRecordField.name().equals(DEBEZIUM_DATA_BEFORE_FIELD)) {
                recordSchemaBuilder.field(origRecordField.name(), rebuildBeforeSchema(origRecordField, origRecordValue));
            } else if (origRecordField.name().equals(DEBEZIUM_DATA_AFTER_FIELD)) {
                recordSchemaBuilder.field(origRecordField.name(), rebuildAfterSchema(origRecordField, origRecordValue));
            } else {
                recordSchemaBuilder.field(origRecordField.name(), origRecordField.schema());
            }
        }

        var newRecordSchema = recordSchemaBuilder.build();
        var newRecordValue = new Struct(newRecordSchema);

        var origBeforeValue = origRecordValue.get(DEBEZIUM_DATA_BEFORE_FIELD);

        if(origBeforeValue != null) {
            var newRecordBeforeValue =
                    buildNewFieldData(origRecordValue, newRecordSchema.field(DEBEZIUM_DATA_BEFORE_FIELD).schema(), DEBEZIUM_DATA_BEFORE_FIELD);

            newRecordValue.put(DEBEZIUM_DATA_BEFORE_FIELD, newRecordBeforeValue);
        } else {
            newRecordValue.put(DEBEZIUM_DATA_BEFORE_FIELD, null);
        }

        var origAfterValue = origRecordValue.get(DEBEZIUM_DATA_AFTER_FIELD);
        if(origAfterValue != null) {
            var newRecordAfterValue =
                    buildNewFieldData(origRecordValue, newRecordSchema.field(DEBEZIUM_DATA_AFTER_FIELD).schema(), DEBEZIUM_DATA_AFTER_FIELD);

            newRecordValue.put(DEBEZIUM_DATA_AFTER_FIELD, newRecordAfterValue);
        } else {
            newRecordValue.put(DEBEZIUM_DATA_AFTER_FIELD, null);
        }

        long endTime = System.nanoTime();

        long duration = (endTime-startTime)/1000_000;

        return origRecord.newRecord(
                origRecord.topic(),
                origRecord.kafkaPartition(),
                origRecord.keySchema(),
                origRecord.key(),
                newRecordSchema,
                newRecordValue,
                origRecord.timestamp()
        );
    }

    private Struct buildNewFieldData(org.apache.kafka.connect.data.Struct origRecordValue, Schema newFieldSchema, String newFieldName)
            throws JsonProcessingException {

        var old = (Struct)origRecordValue.get(newFieldName);
        var value = new Struct(newFieldSchema);

        for (var f:newFieldSchema.fields()){
            if(isTargetField(f.name())){
                value.put(f.name(), jsonNodeToConnectValue(OBJECT_MAPPER.readTree(old.getString(f.name())), newFieldSchema.field(f.name()).schema()));
            } else {
                value.put(f.name(), old.get(f.name()));
            }
        }

        return value;
    }

    private org.apache.kafka.connect.data.Schema rebuildBeforeSchema(Field origBeforeField, org.apache.kafka.connect.data.Struct origRecordValue){
        var origRecordBeforeValue = (org.apache.kafka.connect.data.Struct) origRecordValue.get(DEBEZIUM_DATA_BEFORE_FIELD);

        if(origRecordBeforeValue == null){
            // CREATE EVENT
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        var origBeforeFieldSchema = origBeforeField.schema();
        var schemaBuilder = SchemaBuilder
                .struct()
                .name(origBeforeField.name() + "_before_field_parsed");

        for (var f : origBeforeFieldSchema.fields()){
            if(isTargetField(f.name()) && (f.schema().type() == Schema.Type.STRING)){
                var origRecordBeforeFieldValue = (String) origRecordBeforeValue.get(f.name());
                schemaBuilder.field(f.name(), JsonSchemaBuilder.buildSchema(origRecordBeforeFieldValue));
            } else {
                schemaBuilder.field(f.name(), f.schema());
            }
        }

        return schemaBuilder.build();
    }

    private org.apache.kafka.connect.data.Schema rebuildAfterSchema(Field origAfterField, org.apache.kafka.connect.data.Struct origRecordValue){
        var origRecordAfterValue = (org.apache.kafka.connect.data.Struct) origRecordValue.get(DEBEZIUM_DATA_AFTER_FIELD);

        if(origRecordAfterValue == null){
            // DELETE EVENT
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        var origBeforeFieldSchema = origAfterField.schema();
        var schemaBuilder = SchemaBuilder
                .struct()
                .name(origAfterField.name() + "_after_field_parsed");

        for (var f : origBeforeFieldSchema.fields()){
            if(isTargetField(f.name()) && (f.schema().type() == Schema.Type.STRING)){
                var origRecordBeforeFieldValue = (String) origRecordAfterValue.get(f.name());
                schemaBuilder.field(f.name(), JsonSchemaBuilder.buildSchema(origRecordBeforeFieldValue));
            } else {
                schemaBuilder.field(f.name(), f.schema());
            }
        }

        return schemaBuilder.build();
    }

    private boolean isTargetField(String fieldName){
        return targetFields.contains(fieldName);
    }

    private Object jsonNodeToConnectValue(JsonNode node, Schema targetSchema) {
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

    private Struct jsonNodeToStruct(JsonNode node, Schema schema) {
        if (!node.isObject()) {
            throw new DataException("Expected JSON object, got: " + node.getNodeType());
        }

        Struct struct = new Struct(schema);
        node.fields().forEachRemaining(entry -> {
            org.apache.kafka.connect.data.Field field = schema.field(entry.getKey());
            if (field != null) {
                Object value = jsonNodeToConnectValue(entry.getValue(), field.schema());
                struct.put(field.name(), value);
            }
        });

        return struct;
    }

    private List<Object> jsonNodeToList(JsonNode node, Schema elementSchema) {
        List<Object> list = new ArrayList<>();
        node.forEach(item -> list.add(jsonNodeToConnectValue(item, elementSchema)));
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