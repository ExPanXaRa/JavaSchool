package sbp.school.kafka.config;

import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Конфигурационный класс для управления настройками подключения к Kafka. Класс отвечает за загрузку
 * и управление свойствами из файла конфигурации kafka-producer.properties. Предоставляет методы для
 * фильтрации и получения свойств по различным критериям.
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
     * результирующем объекте Properties будут ез префикса.
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
     * Возвращает настройки продюсера транзакций из общей конфигурации. Использует префикс
     * "transaction.producer." для фильтрации свойств.
     *
     * @return объект Properties содержащий настройки продюсера транзакций
     */
    public Properties getTransactionProducerConfig() {
        Properties producerProps = getFilteredProperties("producer.");
        log.info("Свойства для продюсера транзакций успешно загружены.");
        return producerProps;
    }

    /**
     * Возвращает настройки потребителя подтверждений транзакций из общей конфигурации. Использует
     * префикс "consumer." для фильтрации свойств.
     *
     * @return объект Properties содержащий настройки потребителя подтверждений
     */
    public Properties getTransactionAckConsumerConfig() {
        Properties consumerProps = getFilteredProperties("consumer.");
        log.info("Свойства для потребителя подтверждений успешно загружены.");
        return consumerProps;
    }
}