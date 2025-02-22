package sbp.school.kafka.producer;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
        Runtime.getRuntime().addShutdownHook(new Thread(this::flushAndClose));
    }

    /**
     * Отправляет транзакцию в Kafka-топик.
     *
     * @param transactionDto Транзакция для отправки.
     */
    public void send(TransactionDto transactionDto) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topic, transactionDto);

        kafkaProducer.send(record, (recordMetadata, e) -> {
            handleSendResult(recordMetadata, e);
        });
    }

    /**
     * Обрабатывает результат отправки сообщения в Kafka.
     *
     * @param recordMetadata Метаданные отправленного сообщения (partition, offset и т.д.).
     * @param e              Исключение, если отправка не удалась.
     */
    private void handleSendResult(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            log.debug("Сообщение успешно отправлено: partition={}, offset={}",
                recordMetadata.partition(), recordMetadata.offset());
        } else {
            log.error("Ошибка при отправке сообщения: partition={}, offset={}",
                recordMetadata != null ? recordMetadata.partition() : "unknown",
                recordMetadata != null ? recordMetadata.offset() : "unknown", e);
        }
    }

    /**
     * Гарантированно отправляет все сообщения из буфера и завершает работу производителя.
     */
    public void flushAndClose() {
        try {
            log.info("Отправка всех сообщений из буфера...");
            kafkaProducer.flush();
            log.info("Все сообщения из буфера успешно отправлены.");
        } catch (Exception e) {
            log.error("Ошибка при отправке сообщений из буфера", e);
        } finally {
            log.info("Завершение работы производителя Kafka.");
            kafkaProducer.close();
        }
    }
}