package sbp.school.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.dto.TransactionDto;

/**
 * Реализация десериализатора для преобразования байтовых данных в объекты TransactionDto.
 * Десериализатор выполняет валидацию JSON-схемы и обеспечивает корректную обработку дат и времени.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class TransactionDeserializer implements Deserializer<TransactionDto> {

    /**
     * Объект для работы с JSON преобразованиями. Настроен для корректной обработки дат и времени
     * через JavaTimeModule.
     */
    private final ObjectMapper objectMapper;

    /**
     * Валидатор JSON-схемы для проверки структуры десериализуемых данных.
     */
    private final JsonSchemaValidator validator;

    /**
     * Путь к JSON-схеме для валидации транзакций.
     */
    private final static String PATH_TO_JSON_SCHEMA = "/json/TransactionSchema.json";

    /**
     * Создает новый экземпляр десериализатора с настроенным ObjectMapper. Регистрирует модуль
     * JavaTimeModule для корректной обработки дат и времени.
     */
    public TransactionDeserializer() {
        this.objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());
        this.validator = new JsonSchemaValidator();
    }

    /**
     * Десериализует байтовый массив в объект TransactionDto. Выполняет следующие операции: 1.
     * Преобразование байтов в строку UTF-8 2. Валидация JSON-схемы 3. Преобразование JSON в объект
     * TransactionDto
     *
     * @param topic тема Kafka
     * @param data  байтовый массив для десериализации
     * @return десериализованный объект TransactionDto или null в случае ошибки
     * @throws RuntimeException если входные данные равны null
     */
    @Override
    public TransactionDto deserialize(String topic, byte[] data) {
        if (data != null) {
            try {
                String dataString = new String(data, StandardCharsets.UTF_8);
                validator.validate(dataString, PATH_TO_JSON_SCHEMA);
                return objectMapper.readValue(dataString, TransactionDto.class);
            } catch (Exception e) {
                log.error("Во время десериализации возникла ошибка", e);
                return null;
            }
        } else {
            log.error("data равна null");
            throw new RuntimeException("data равна null");
        }
    }
}