package org.mtq.kafka.connect.transforms.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
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
    
    private Set<String> jsonbFields;
    private boolean failOnError;
    
    @Override
    public void configure(Map<String, ?> props) {
        Object fieldsObj = props.get(FIELD_CONFIG);
        if (fieldsObj instanceof List) {
            this.jsonbFields = new HashSet<>((List<String>) fieldsObj);
        } else if (fieldsObj instanceof String) {
            String[] fields = ((String) fieldsObj).split(",");
            this.jsonbFields = new HashSet<>();
            for (String field : fields) {
                this.jsonbFields.add(field.trim());
            }
        } else {
            this.jsonbFields = new HashSet<>();
        }
        
        Object failObj = props.get(FAIL_ON_ERROR_CONFIG);
        this.failOnError = failObj == null || Boolean.parseBoolean(failObj.toString());
        
        log.info("JsonStringValueParser configured for fields: {}", jsonbFields);
    }
    
    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }
        
        try {
            Struct originalValue = (Struct) record.value();
            Schema originalSchema = record.valueSchema();
            
            SchemaBuilder newSchemaBuilder = SchemaBuilder.struct();
            
            if (originalSchema.name() != null) {
                newSchemaBuilder.name(originalSchema.name());
            }
            
            for (Field field : originalSchema.fields()) {
                if (shouldTransform(field.name())) {
                    newSchemaBuilder.field(field.name(), Schema.OPTIONAL_STRING_SCHEMA);
                } else {
                    newSchemaBuilder.field(field.name(), field.schema());
                }
            }
            
            Schema newSchema = newSchemaBuilder.build();
            Struct newValue = new Struct(newSchema);
            
            for (Field field : originalSchema.fields()) {
                Object fieldValue = originalValue.get(field);
                
                if (shouldTransform(field.name()) && fieldValue != null) {
                    try {
                        Object parsedValue = parseJson(fieldValue.toString());
                        newValue.put(field.name(), parsedValue);
                    } catch (Exception e) {
                        log.warn("Failed to parse JSON for field {}: {}", field.name(), e.getMessage());
                        
                        if (failOnError) {
                            throw new DataException("Failed to parse JSON for field:" + field.name(), e);
                        }
                        
                        newValue.put(field.name(), fieldValue);
                    }
                } else {
                    newValue.put(field.name(), fieldValue);
                }
            }
            
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
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
    
    private boolean shouldTransform(String fieldName) {
        return jsonbFields != null && jsonbFields.contains(fieldName);
    }
    
    private Object parseJson(String jsonString) throws Exception {
        JsonNode rootNode = OBJECT_MAPPER.readTree(jsonString);
        return convertJsonNode(rootNode);
    }
    
    private Object convertJsonNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        
        if (node.isObject()) {
            Map<String, Object> result = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                result.put(field.getKey(), convertJsonNode(field.getValue()));
            }
            
            return result;
            
        } else if (node.isArray()) {
            List<Object> result = new ArrayList<>();
            for (JsonNode element : node) {
                result.add(convertJsonNode(element));
            }
            return result;
            
        } else if (node.isTextual()) {
            return node.asText();
        } else if (node.isNumber()) {
            if (node.isInt()) {
                return node.asInt();
            } else if (node.isLong()) {
                return node.asLong();
            } else if (node.isDouble()) {
                return node.asDouble();
            } else {
                return node.asText();
            }
        } else if (node.isBoolean()) {
            return node.asBoolean();
        }
        
        return node.asText();
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
    public void close() {}
}