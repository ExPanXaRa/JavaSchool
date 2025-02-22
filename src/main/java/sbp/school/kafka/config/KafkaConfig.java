package sbp.school.kafka.config;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Конфигурационный класс для настройки подключения к Kafka.
 * Загружает настройки из файла kafka-producer.properties.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
@Getter
public class KafkaConfig {

    /**
     * Хранилище свойств конфигурации Kafka.
     */
    private final Properties properties = new Properties();

    /**
     * Инициализирует конфигурацию Kafka, загружая настройки из файла kafka-producer.properties.
     *
     * @throws RuntimeException если произошла ошибка при загрузке конфигурации
     */
    public KafkaConfig() {
        try {
            properties.load(
                KafkaConfig.class.getClassLoader()
                    .getResourceAsStream("kafka-producer.properties"));
        } catch (Exception e) {
            log.error("Ошибка во время загрузки конфигурации", e);
            throw new RuntimeException(e);
        }
    }
}