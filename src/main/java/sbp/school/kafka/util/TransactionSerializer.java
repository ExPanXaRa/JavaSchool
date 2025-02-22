package sbp.school.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.dto.TransactionDto;

/**
 * Реализация сериализатора для объектов TransactionDto в формат JSON.
 * Обеспечивает преобразование транзакционных данных в байтовый массив,
 * с валидацией по JSON-схеме перед сериализацией.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class TransactionSerializer implements Serializer<TransactionDto> {

    /**
     * ObjectMapper для преобразования объектов в JSON.
     * Настроен для корректной обработки дат Java Time API.
     */
    private final ObjectMapper objectMapper;

    /**
     * Валидатор JSON-схемы для проверки корректности данных.
     */
    private final JsonSchemaValidator jsonValidator;

    /**
     * Путь к JSON-схеме для валидации транзакций.
     */
    private final static String PATH_TO_JSON_SCHEMA = "/json/TransactionSchema.json";

    /**
     * Создает новый экземпляр сериализатора с настроенным ObjectMapper
     * и валидатором JSON-схемы.
     */
    public TransactionSerializer() {
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());
        this.jsonValidator = new JsonSchemaValidator();
    }

    /**
     * Сериализует объект TransactionDto в байтовый массив.
     * Перед сериализацией выполняет валидацию данных по JSON-схеме.
     *
     * @param topic имя топика (не используется в сериализации)
     * @param transactionDto объект для сериализации
     * @return сериализованные данные в формате UTF-8
     * @throws SerializationException если возникла ошибка при сериализации
     * @throws RuntimeException если transactionDto равен null
     */
    @Override
    public byte[] serialize(String topic, TransactionDto transactionDto) {
        if (transactionDto != null) {
            try {
                String jsonString = objectMapper.writeValueAsString(transactionDto);
                jsonValidator.validate(jsonString, PATH_TO_JSON_SCHEMA);
                return jsonString.getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                log.error("Во время сериализации возникла ошибка {}", e.getMessage());
                throw new SerializationException(e);
            }
        } else {
            log.error("Транакция равна null");
            throw new RuntimeException("Транакция равна null");
        }
    }
}