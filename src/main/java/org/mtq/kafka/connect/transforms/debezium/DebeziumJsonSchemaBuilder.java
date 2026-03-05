package org.mtq.kafka.connect.transforms.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumJsonSchemaBuilder {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumJsonSchemaBuilder.class);

    public static Schema buildSchema(String jsonString) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(jsonString);
            return buildSchemaFromNode(node);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build schema from JSON", e);
        }
    }

    private static Schema buildSchemaFromNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        if (node.isBoolean()) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        }

        if (node.isInt()) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        }

        if (node.isLong()) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        }

        if (node.isDouble()) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        }

        if (node.isTextual()) {
            String nodeText = node.asText();
            if (nodeText.startsWith("{") && nodeText.endsWith("}")) {
                try {
                    JsonNode nested = OBJECT_MAPPER.readTree(nodeText);
                    return buildSchemaFromNode(nested);
                } catch (Exception e) {
                    LOG.warn("Unable to build JSON Schema from textual node (fallback to Schema.OPTIONAL_STRING_SCHEMA):: node:{}, error:{}", nodeText, e.getMessage());
                    return Schema.OPTIONAL_STRING_SCHEMA;
                }
            }
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        if (node.isArray()) {
            if (node.isEmpty()) {
                return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
            }

            Schema elementSchema = buildSchemaFromNode(node.get(0));
            return SchemaBuilder.array(elementSchema).optional().build();
        }

        if (node.isObject()) {
            var builder = SchemaBuilder.struct().optional();
            node.properties().iterator().forEachRemaining(entry -> {
                String fieldName = entry.getKey();
                JsonNode fieldValue = entry.getValue();
                Schema fieldSchema = buildSchemaFromNode(fieldValue);
                builder.field(fieldName, fieldSchema);
            });

            return builder.build();
        }

        return Schema.OPTIONAL_STRING_SCHEMA;
    }
}