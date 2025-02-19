package sbp.school.kafka.producer;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.dto.TransactionDto;

/**
 * Сервис для отправки транзакционных данных в Kafka.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class KafkaProducerService {

    /**
     * Название топика Kafka для отправки сообщений
     */
    private final String topic;

    /**
     * Производитель Kafka для отправки сообщений
     */
    private final KafkaProducer<String, TransactionDto> kafkaProducer;

    /**
     * Создает новый экземпляр сервиса для отправки сообщений в Kafka.
     *
     * @param kafkaProducerProperties Свойства конфигурации Kafka-производителя
     */
    public KafkaProducerService(Properties kafkaProducerProperties) {
        this.topic = kafkaProducerProperties.getProperty("topic.name");
        this.kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
    }

    /**
     * Отправляет транзакцию в Kafka-топик.
     *
     * @param transactionDto Транзакция для отправки
     */
    public void send(TransactionDto transactionDto) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topic, transactionDto);

        kafkaProducer.send(record, ((recordMetadata, e) -> {
            if (e == null) {
                log.error(
                    "Во время отправки сообщения возникла ошибка: partition - {}, offset - {}",
                    recordMetadata.partition(), recordMetadata.offset());
            } else {
                log.debug("Сообщение успешно отправлено: partition - {}, offset - {}",
                    recordMetadata.partition(), recordMetadata.offset());
            }
        }));
    }
}