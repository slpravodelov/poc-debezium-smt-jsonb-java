package org.mtq.kafka.connect.transforms.debezium;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class JsonStringValueParserTest {

    private JsonStringValueParser<SinkRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new JsonStringValueParser<>();
    }

    // ==================== КОНФИГУРАЦИЯ ====================

    @Test
    void testConfigureWithList() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("process.fields", Arrays.asList("data", "metadata"));
        config.put("fail.on.error", true);

        // When
        transform.configure(config);

        // Then - проверим через отражение или тестовый вызов
        SinkRecord record = createTestRecord("data", "{\"key\":\"value\"}", null, "other");
        SinkRecord result = transform.apply(record);

        // Должен обработать поле data
        assertNotNull(result);
        Struct value = (Struct) result.value();
        assertNotNull(value.get("data"));
    }

    @Test
    void testConfigureWithString() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("process.fields", "data, metadata, config");
        config.put("fail.on.error", false);

        // When
        transform.configure(config);

        // Then
        SinkRecord record = createTestRecord("data", "{\"key\":\"value\"}", null, "other");
        SinkRecord result = transform.apply(record);
        assertNotNull(result);
    }

    @Test
    void testConfigureWithEmptyFields() {
        // Given
        Map<String, Object> config = new HashMap<>();

        // When
        transform.configure(config);

        // Then - не должен ничего обрабатывать
        SinkRecord record = createTestRecord("data", "{\"key\":\"value\"}");
        SinkRecord result = transform.apply(record);

        Struct value = (Struct) result.value();
        assertEquals("{\"key\":\"value\"}", value.get("data"));
    }

    // ==================== ОСНОВНЫЕ ТЕСТЫ ====================

    @Test
    void testTransformJsonStringToStruct() {
        // Given
        configureTransform(Arrays.asList("data", "metadata"), true);

        String jsonData = "{\"id\": 123, \"name\": \"test\", \"active\": true, \"tags\": [\"a\", \"b\"]}";
        SinkRecord record = createTestRecord("data", jsonData, "metadata", null);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        assertNotNull(result);
        assertNotNull(result.valueSchema());
        assertEquals(Schema.Type.STRUCT, result.valueSchema().type());

        Struct value = (Struct) result.value();

        // Проверяем преобразованное поле data
        assertTrue(value.get("data") instanceof Map);
        Map<String, Object> dataMap = (Map<String, Object>) value.get("data");
        assertEquals(123, dataMap.get("id"));
        assertEquals("test", dataMap.get("name"));
        assertEquals(true, dataMap.get("active"));
        assertEquals(Arrays.asList("a", "b"), dataMap.get("tags"));

        // Проверяем metadata (null)
        assertNull(value.get("metadata"));
    }

    @Test
    void testTransformNestedJson() {
        // Given
        configureTransform(Collections.singletonList("data"), true);

        String jsonData = "{\"user\": {\"id\": 456, \"profile\": {\"email\": \"test@test.com\"}}, \"items\": [1, 2, 3]}";
        SinkRecord record = createTestRecord("data", jsonData);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        Struct value = (Struct) result.value();
        Map<String, Object> dataMap = (Map<String, Object>) value.get("data");

        // Проверяем вложенную структуру
        Map<String, Object> userMap = (Map<String, Object>) dataMap.get("user");
        assertEquals(456, userMap.get("id"));

        Map<String, Object> profileMap = (Map<String, Object>) userMap.get("profile");
        assertEquals("test@test.com", profileMap.get("email"));

        assertEquals(Arrays.asList(1, 2, 3), dataMap.get("items"));
    }

    @Test
    void testTransformJsonArray() {
        // Given
        configureTransform(Collections.singletonList("data"), true);

        String jsonData = "[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]";
        SinkRecord record = createTestRecord("data", jsonData);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        Struct value = (Struct) result.value();
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) value.get("data");

        assertEquals(3, dataList.size());
        assertEquals(1, dataList.get(0).get("id"));
        assertEquals(2, dataList.get(1).get("id"));
        assertEquals(3, dataList.get(2).get("id"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"int\": 123}",
            "{\"double\": 123.45}",
            "{\"boolean\": true}",
            "{\"string\": \"text\"}",
            "{\"null\": null}",
            "{}",
            "[]"
    })
    void testTransformVariousJsonTypes(String jsonData) {
        // Given
        configureTransform(Collections.singletonList("data"), true);
        SinkRecord record = createTestRecord("data", jsonData);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        assertNotNull(result);
        Struct value = (Struct) result.value();
        assertNotNull(value.get("data"));
    }

    // ==================== ТЕСТЫ ОШИБОК ====================

    @Test
    void testFailOnErrorEnabled() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("process.fields", Collections.singletonList("data"));
        config.put("fail.on.error", true);
        transform.configure(config);

        String invalidJson = "{invalid json}";
        SinkRecord record = createTestRecord("data", invalidJson);

        // When/Then
        assertThrows(DataException.class, () -> {
            transform.apply(record);
        });
    }

    @Test
    void testFailOnErrorDisabled() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("process.fields", Collections.singletonList("data"));
        config.put("fail.on.error", false);
        transform.configure(config);

        String invalidJson = "{invalid json}";
        SinkRecord record = createTestRecord("data", invalidJson);

        // When
        SinkRecord result = transform.apply(record);

        // Then - исходное значение сохраняется
        Struct value = (Struct) result.value();
        assertEquals(invalidJson, value.get("data"));
    }

    @Test
    void testTransformWithNullValue() {
        // Given
        configureTransform(Collections.singletonList("data"), true);
        SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);

        // When
        SinkRecord result = transform.apply(record);

        // Then - запись не должна измениться
        assertNull(result.value());
        assertNull(result.valueSchema());
    }

    @Test
    void testTransformWithNonStructValue() {
        // Given
        configureTransform(Collections.singletonList("data"), true);
        SinkRecord record = new SinkRecord("test", 0, null, null, Schema.STRING_SCHEMA, "string value", 0);

        // When
        SinkRecord result = transform.apply(record);

        // Then - запись не должна измениться
        assertEquals("string value", result.value());
        assertEquals(Schema.STRING_SCHEMA, result.valueSchema());
    }

    @ParameterizedTest
    @MethodSource("provideEdgeCases")
    void testTransformWithEdgeCases(String fieldName, Object fieldValue, boolean shouldFail) {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("process.fields", Collections.singletonList(fieldName));
        config.put("fail.on.error", shouldFail);
        transform.configure(config);

        SinkRecord record = createTestRecord(fieldName, fieldValue, null, null);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        assertNotNull(result);
    }

    static Stream<Arguments> provideEdgeCases() {
        return Stream.of(
                Arguments.of("data", 123, false),           // Число - не строка
                Arguments.of("data", true, false),          // Булево - не строка
                Arguments.of("data", new ArrayList<>(), false), // Список - не строка
                Arguments.of("data", null, false),          // Null
                Arguments.of("data", "{\"key\": \"value\"}", true)  // Валидный JSON с fail.on.error=true
        );
    }

    // ==================== ТЕСТЫ СХЕМ ====================

    @Test
    void testSchemaStructure() {
        // Given
        configureTransform(Arrays.asList("data", "metadata"), true);

        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.STRING_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Schema.INT64_SCHEMA)
                .build();

        Struct originalValue = new Struct(originalSchema)
                .put("id", 1)
                .put("data", "{\"key\":\"value\"}")
                .put("timestamp", System.currentTimeMillis());

        SinkRecord record = new SinkRecord("test", 0, null, null, originalSchema, originalValue, 0);

        // When
        SinkRecord result = transform.apply(record);

        // Then - проверяем новую схему
        Schema resultSchema = result.valueSchema();

        // Поля должны быть
        assertNotNull(resultSchema.field("id"));
        assertNotNull(resultSchema.field("data"));
        assertNotNull(resultSchema.field("metadata"));
        assertNotNull(resultSchema.field("timestamp"));

        // data должен быть OPTIONAL_STRING_SCHEMA
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA.type(), resultSchema.field("data").schema().type());

        // id должен сохранить исходную схему (INT32)
        assertEquals(Schema.INT32_SCHEMA.type(), resultSchema.field("id").schema().type());
    }

    // ==================== ТЕСТЫ КОНФИГУРАЦИИ ====================

    @Test
    void testConfigDef() {
        // Given
        ConfigDef configDef = transform.config();

        // Then
        assertNotNull(configDef);
        assertNotNull(configDef.configKeys().get("process.fields"));
        assertNotNull(configDef.configKeys().get("fail.on.error"));

        assertEquals(ConfigDef.Type.LIST, configDef.configKeys().get("process.fields").type);
        assertEquals(ConfigDef.Type.BOOLEAN, configDef.configKeys().get("fail.on.error").type);
    }

    @Test
    void testMultipleFields() {
        // Given
        configureTransform(Arrays.asList("field1", "field2", "field3"), true);

        Schema originalSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", Schema.STRING_SCHEMA)
                .field("field4", Schema.STRING_SCHEMA) // не в списке обработки
                .build();

        Struct originalValue = new Struct(originalSchema)
                .put("field1", "{\"a\":1}")
                .put("field2", "{\"b\":2}")
                .put("field3", "{\"c\":3}")
                .put("field4", "{\"d\":4}");

        SinkRecord record = new SinkRecord("test", 0, null, null, originalSchema, originalValue, 0);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        Struct value = (Struct) result.value();

        // field1,2,3 должны быть преобразованы
        assertTrue(value.get("field1") instanceof Map);
        assertTrue(value.get("field2") instanceof Map);
        assertTrue(value.get("field3") instanceof Map);

        // field4 должен остаться строкой
        assertTrue(value.get("field4") instanceof String);
        assertEquals("{\"d\":4}", value.get("field4"));
    }

    // ==================== HELPER МЕТОДЫ ====================

    private void configureTransform(List<String> fields, boolean failOnError) {
        Map<String, Object> config = new HashMap<>();
        config.put("process.fields", fields);
        config.put("fail.on.error", failOnError);
        transform.configure(config);
    }

    private SinkRecord createTestRecord(String jsonFieldName, Object jsonFieldValue, String otherFieldName, Object otherFieldValue) {
        SchemaBuilder builder = SchemaBuilder.struct();
        List<Field> fields = new ArrayList<>();

        builder.field(jsonFieldName, jsonFieldValue != null ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);

        if (otherFieldName != null) {
            if (otherFieldValue instanceof String) {
                builder.field(otherFieldName, Schema.STRING_SCHEMA);
            } else {
                builder.field(otherFieldName, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }

        Schema schema = builder.build();
        Struct value = new Struct(schema);

        value.put(jsonFieldName, jsonFieldValue);
        if (otherFieldName != null) {
            value.put(otherFieldName, otherFieldValue);
        }

        return new SinkRecord("test", 0, null, null, schema, value, 0);
    }

    private SinkRecord createTestRecord(String jsonFieldName, Object jsonFieldValue) {
        return createTestRecord(jsonFieldName, jsonFieldValue, null, null);
    }
}