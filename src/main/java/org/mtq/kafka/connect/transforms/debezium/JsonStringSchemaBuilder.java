package org.mtq.kafka.connect.transforms.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.*;

public class JsonStringSchemaBuilder {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Schema> schemaCache = new HashMap<>();
    private boolean useOptionalFields = true;
    private boolean inferSchemaFromData = true;

    public Schema convert(String jsonString) {
        try {
            JsonNode rootNode = mapper.readTree(jsonString);
            return buildSchema(rootNode, "root", new HashSet<>());
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON", e);
        }
    }

    private Schema buildSchema(JsonNode node, String path, Set<String> visitedPaths) {
        if (visitedPaths.contains(path)) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
        visitedPaths.add(path);

        try {
            JsonNodeType nodeType = node.getNodeType();

            switch (nodeType) {
                case OBJECT:
                    return buildObjectSchema(node, path, visitedPaths);

                case ARRAY:
                    return buildArraySchema(node, path, visitedPaths);

                case STRING:
                    return useOptionalFields ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;

                case NUMBER:
                    return buildNumberSchema(node);

                case BOOLEAN:
                    return useOptionalFields ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;

                case BINARY:
                    return Schema.OPTIONAL_BYTES_SCHEMA;

                default:
                    return Schema.OPTIONAL_STRING_SCHEMA;
            }
        } finally {
            visitedPaths.remove(path);
        }
    }

    private Schema buildObjectSchema(JsonNode node, String path, Set<String> visitedPaths) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name("struct_" + path.replace(".", "_"))
                .doc("Auto-generated schema from JSON at " + path);

        if (useOptionalFields) {
            builder.optional();
        }

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            String fieldPath = path + "." + fieldName;

            try {
                Schema fieldSchema = buildSchema(fieldValue, fieldPath, visitedPaths);
                builder.field(fieldName, fieldSchema);
            } catch (Exception e) {
                builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }

        return builder.build();
    }

    private Schema buildArraySchema(JsonNode node, String path, Set<String> visitedPaths) {
        if (node.isEmpty()) {
            return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)
                    .name("array_" + path.replace(".", "_"))
                    .optional()
                    .build();
        }

        Set<Schema> elementSchemas = new HashSet<>();
        for (JsonNode element : node) {
            Schema elementSchema = buildSchema(element, path + "[*]", visitedPaths);
            elementSchemas.add(elementSchema);
        }

        if (elementSchemas.size() == 1) {
            Schema elementSchema = elementSchemas.iterator().next();
            return SchemaBuilder.array(elementSchema)
                    .name("array_" + path.replace(".", "_"))
                    .optional()
                    .build();
        } else {
            Schema commonSchema = findCommonSchema(elementSchemas);
            return SchemaBuilder.array(commonSchema)
                    .name("array_" + path.replace(".", "_"))
                    .optional()
                    .build();
        }
    }

    private Schema buildNumberSchema(JsonNode node) {
        if (node.isInt() || node.isLong() || node.isShort()) {
            if (node.canConvertToLong()) {
                return useOptionalFields ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            }
        }

        if (!node.isFloat() && !node.isDouble()) {
            node.isBigDecimal();
        }

        return useOptionalFields ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
    }

    private Schema findCommonSchema(Set<Schema> schemas) {
        boolean allNumbers = schemas.stream().allMatch(s ->
                s.type() == Schema.Type.INT8 ||
                        s.type() == Schema.Type.INT16 ||
                        s.type() == Schema.Type.INT32 ||
                        s.type() == Schema.Type.INT64 ||
                        s.type() == Schema.Type.FLOAT32 ||
                        s.type() == Schema.Type.FLOAT64);

        if (allNumbers) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        }

        boolean allStrings = schemas.stream().allMatch(s -> s.type() == Schema.Type.STRING);
        if (allStrings) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        boolean allBooleans = schemas.stream().allMatch(s -> s.type() == Schema.Type.BOOLEAN);
        if (allBooleans) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        }

        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    public void setUseOptionalFields(boolean useOptionalFields) {
        this.useOptionalFields = useOptionalFields;
    }

    public void setInferSchemaFromData(boolean inferSchemaFromData) {
        this.inferSchemaFromData = inferSchemaFromData;
    }
}