package sbp.school.kafka.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.InputStream;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * Класс для валидации JSON данных по заданной схеме.
 * Предоставляет метод для проверки соответствия JSON данных схеме,
 * загруженной из ресурсов класса.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
public class JsonSchemaValidator {

    /**
     * Валидирует JSON данные по указанной схеме.
     *
     * @param value JSON строка для валидации
     * @param path путь к файлу схемы относительно ресурсов класса
     * @throws JsonProcessingException если произошла ошибка при обработке JSON
     * @throws RuntimeException если схема недействительна или данные не соответствуют схеме
     */
    public void validate(String value, String path) throws JsonProcessingException {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        JsonSchema schema = null;

        try (InputStream schemaStream = getClass().getResourceAsStream(path)) {
            schema = factory.getSchema(schemaStream);
        } catch (Exception e) {
            log.error("Ошибка во время загрузки схемы json", e);
            throw new RuntimeException(e);
        }

        if (schema != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value);
            Set<ValidationMessage> validationResult = schema.validate(jsonNode);
            if (!validationResult.isEmpty()) {
                throw new RuntimeException(validationResult.toString());
            }
        }
    }
}