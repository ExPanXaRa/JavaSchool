package sbp.school.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.dto.ChecksumDto;

@Slf4j
public class ChecksumDeserializer implements Deserializer<ChecksumDto> {

    @Override
    public ChecksumDto deserialize(String topic, byte[] data) {
        if (data == null) {
            log.debug("Получен пустой байтовый массив для десериализации");
            throw new RuntimeException("Получен пустой байтовый массив для десериализации");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonString = new String(data, StandardCharsets.UTF_8);
            return objectMapper.readValue(jsonString, ChecksumDto.class);
        } catch (Exception e) {
            log.error("Ошибка десериализации", e);
            throw new RuntimeException("Ошибка десериализации", e);
        }
    }
}

