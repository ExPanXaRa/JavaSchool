package sbp.school.kafka.config;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Конфигурационный класс для настройки подключения к Kafka. Загружает настройки из файла
 * kafka-producer.properties. Предоставляет методы для фильтрации свойств по префиксам и получения
 * отдельных значений.
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
     * Конструктор класса KafkaConfig. Инициализирует конфигурацию путем загрузки свойств из файла
     * kafka-producer.properties. При возникновении ошибок во время загрузки генерирует исключение
     * RuntimeException.
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

    /**
     * Фильтрует свойства по указанному префиксу и возвращает отфильтрованные свойства. Все ключи в
     * результирующем объекте Properties будут иметь префикс удалён.
     *
     * @param prefix префикс для фильтрации свойств
     * @return новый объект Properties содержащий только свойства с указанным префиксом
     */
    public Properties getFilteredProperties(String prefix) {
        Properties filteredProps = new Properties();

        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String newKey = key.substring(prefix.length());
                filteredProps.setProperty(newKey, properties.getProperty(key));
            }
        }
        log.debug("Свойства с префиксом '{}' успешно отфильтрованы.", prefix);
        return filteredProps;
    }

    /**
     * Получает значение свойства по его ключу. Если свойство не найдено, генерируется
     * предупреждение в логах.
     *
     * @param key ключ свойства
     * @return значение свойства или null если свойство не найдено
     */
    public String getPropertyValue(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            log.warn("Свойство с ключом '{}' не найдено в конфигурации.", key);
        }
        return value;
    }

    /**
     * Возвращает настройки для консьюмера транзакций из общей конфигурации. Использует префикс
     * "consumer." для фильтрации свойств.
     *
     * @return объект Properties содержащий настройки консьюмера транзакций
     */
    public Properties getTransactionConsumerProperties() {
        log.info("Свойства для консьюмера транзакций успешно загружены.");
        return getFilteredProperties("consumer.");
    }

    /**
     * Возвращает настройки для продюсера чексум из общей конфигурации. Использует префикс
     * "producer." для фильтрации свойств.
     *
     * @return объект Properties содержащий настройки продюсера чексум
     */
    public Properties getChecksumProducerProperties() {
        log.info("Свойства для продюсера чексум успешно загружены.");
        return getFilteredProperties("producer.");
    }
}