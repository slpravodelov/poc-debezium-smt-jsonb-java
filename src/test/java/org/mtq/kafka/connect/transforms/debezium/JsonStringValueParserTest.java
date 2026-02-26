package org.mtq.kafka.connect.transforms.debezium;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
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
    void testTransformNestedJson() {
        // Given
        configureTransform(Collections.singletonList("data"), true);

        String jsonData = "{\"user\": {\"id\": 456, \"profile\": {\"email\": \"test@test.com\"}}, \"items\": [1, 2, 3]}";
        SinkRecord record = createTestRecord("data", jsonData);

        // When
        SinkRecord result = transform.apply(record);

        // Then
        Struct value = (Struct) result.value();
        Struct dataMap = (Struct) value.get("data");

        // Проверяем вложенную структуру
        Struct userMap = (Struct) dataMap.get("user");
        assertEquals(456L, userMap.get("id"));

        Struct profileMap = (Struct) userMap.get("profile");
        assertEquals("test@test.com", profileMap.get("email"));

        assertArrayEquals(new Long[] {1L,2L,3L}, ((java.util.ArrayList<Long>)dataMap.get("items")).toArray());
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
        List<Struct> dataList = (List<Struct>) value.get("data");

        assertEquals(3, dataList.size());
        assertEquals(1L, dataList.get(0).get("id"));
        assertEquals(2L, dataList.get(1).get("id"));
        assertEquals(3L, dataList.get(2).get("id"));
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