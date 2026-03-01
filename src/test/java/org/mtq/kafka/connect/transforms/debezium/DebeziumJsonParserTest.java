package org.mtq.kafka.connect.transforms.debezium;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumJsonParserTest {

    private DebeziumJsonParser<SinkRecord> transformation;
    private static final String TEST_TOPIC = "test.topic";

    @BeforeEach
    void setUp() {
        transformation = new DebeziumJsonParser<>();
    }

    @Nested
    @DisplayName("Configuration Tests")
    class ConfigurationTests {

        @Test
        @DisplayName("Should configure with list of target fields")
        void shouldConfigureWithListOfFields() {
            // Given
            Map<String, Object> config = new HashMap<>();
            config.put("targetFields", Arrays.asList("field1", "field2"));
            config.put("fail.on.error", "true");

            // When
            transformation.configure(config);

            // Then - используем reflection для проверки приватных полей
            try {
                var targetFieldsField = DebeziumJsonParser.class.getDeclaredField("targetFields");
                targetFieldsField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Set<String> targetFields = (Set<String>) targetFieldsField.get(transformation);

                var failOnErrorField = DebeziumJsonParser.class.getDeclaredField("failOnError");
                failOnErrorField.setAccessible(true);
                boolean failOnError = failOnErrorField.getBoolean(transformation);

                assertEquals(Set.of("field1", "field2"), targetFields);
                assertTrue(failOnError);
            } catch (Exception e) {
                fail("Failed to access private fields: " + e.getMessage());
            }
        }
    }

    @Nested
    @DisplayName("Processing Tests")
    class ProcessingTests {

        private Schema debeziumEnvelopeSchema;
        private Schema tableValueSchema;

        @BeforeEach
        void setUp() {
            // Создаем схему таблицы
            tableValueSchema = SchemaBuilder.struct()
                    .name("test.TableValue")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("json_field", Schema.STRING_SCHEMA)
                    .field("json_array", Schema.STRING_SCHEMA)
                    .field("nested_json", Schema.STRING_SCHEMA)
                    .optional()
                    .build();

            // Создаем Debezium Envelope схему
            debeziumEnvelopeSchema = SchemaBuilder.struct()
                    .name("test.Envelope")
                    .field("before", tableValueSchema)
                    .field("after", tableValueSchema)
                    .field("source", SchemaBuilder.struct()
                            .name("test.Source")
                            .field("db", Schema.STRING_SCHEMA)
                            .field("table", Schema.STRING_SCHEMA)
                            .build())
                    .field("op", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.INT64_SCHEMA)
                    .build();
        }

        private SinkRecord createRecord(Struct value) {
            return new SinkRecord(
                    TEST_TOPIC,
                    0,
                    Schema.STRING_SCHEMA, "key",
                    debeziumEnvelopeSchema, value,
                    0L
            );
        }

        @Test
        @DisplayName("Should return record unchanged when value is null")
        void shouldReturnUnchangedWhenValueNull() {
            // Given
            SinkRecord record = new SinkRecord(TEST_TOPIC, 0, null, null, null, null, 0L);

            // When
            SinkRecord result = transformation.apply(record);

            // Then
            assertSame(record, result);
        }

        @Test
        @DisplayName("Should parse JSON fields in after section")
        void shouldParseJsonFieldsInAfter() {
            // Given
            Map<String, Object> config = new HashMap<>();
            config.put("targetFields", Arrays.asList("json_field", "json_array", "nested_json"));
            transformation.configure(config);

            Struct after = new Struct(tableValueSchema)
                    .put("id", 1)
                    .put("name", "test")
                    .put("json_field", "{\"key\": \"value\", \"number\": 42}")
                    .put("json_array", "[1, 2, 3, 4, 5]")
                    .put("nested_json", "{\"user\": {\"name\": \"John\", \"age\": 30}}");

            Struct value = new Struct(debeziumEnvelopeSchema)
                    .put("before", null)
                    .put("after", after)
                    .put("source", new Struct(debeziumEnvelopeSchema.field("source").schema())
                            .put("db", "test_db")
                            .put("table", "test_table"))
                    .put("op", "c")
                    .put("ts_ms", System.currentTimeMillis());

            SinkRecord record = createRecord(value);

            // When
            SinkRecord result = transformation.apply(record);

            // Then
            assertNotNull(result);
            assertNotNull(result.valueSchema());

            Struct resultValue = (Struct) result.value();
            Struct resultAfter = resultValue.getStruct("after");

            // Проверяем, что поля изменились
            assertNotNull(resultAfter);

            // Проверяем JSON field
            Object jsonField = resultAfter.get("json_field");
            assertTrue(jsonField instanceof Struct);
            Struct jsonFieldStruct = (Struct) jsonField;
            assertEquals("value", jsonFieldStruct.getString("key"));
            assertEquals(42, jsonFieldStruct.getInt32("number"));

            // Проверяем JSON array
            Object jsonArray = resultAfter.get("json_array");
            assertTrue(jsonArray instanceof List);
            @SuppressWarnings("unchecked")
            List<Integer> arrayList = (List<Integer>) jsonArray;
            assertEquals(5, arrayList.size());
            assertEquals(1, arrayList.get(0));
            assertEquals(5, arrayList.get(4));

            // Проверяем nested JSON
            Object nestedJson = resultAfter.get("nested_json");
            assertTrue(nestedJson instanceof Struct);
            Struct nestedStruct = (Struct) nestedJson;
            Struct user = nestedStruct.getStruct("user");
            assertEquals("John", user.getString("name"));
            assertEquals(30, user.getInt32("age"));
        }
    }
}