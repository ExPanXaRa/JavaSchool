package sbp.school.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.dto.ChecksumDto;

/**
 * Сериализатор для преобразования объектов ChecksumDto в байтовые массивы для Kafka. Реализует
 * интерфейс Serializer из библиотеки Kafka для обеспечения корректной сериализации объектов с
 * датами и временем в формате JSON.
 *
 * @since 1.0
 */
@Slf4j
public class ChecksumSerializer implements Serializer<ChecksumDto> {

    /**
     * Сериализует объект ChecksumDto в байтовый массив для отправки в Kafka-топик.
     *
     * @param topic Kafka-топик, в который будут отправлены данные
     * @param ack   объект ChecksumDto для сериализации
     * @return байтовый массив, содержащий сериализованные данные в формате UTF-8
     * @throws SerializationException если возникла ошибка при сериализации
     */
    @Override
    public byte[] serialize(String topic, ChecksumDto ack) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());

        if (ack != null) {
            try {
                String jsonString = objectMapper.writeValueAsString(ack);
                return jsonString.getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                log.error("Ошибка во время сериализации", e);
                throw new SerializationException(e);
            }
        }
        return null;
    }
}