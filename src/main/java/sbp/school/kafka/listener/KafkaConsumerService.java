package sbp.school.kafka.listener;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.InMemoryStorage;

/**
 * Сервис для чтения и обработки сообщений из Kafka-топика. Реализует автоматическое управление
 * смещениями и асинхронное подтверждение.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class KafkaConsumerService extends Thread implements AutoCloseable {

    /**
     * Имя топика Kafka для чтения сообщений.
     */
    private final String topic;

    /**
     * Потребитель Kafka для чтения записей из топика.
     */
    private final KafkaConsumer<String, TransactionDto> consumer;

    /**
     * Уникальный идентификатор продюсера для отслеживания.
     */
    public static final String PRODUCER_ID = "producer-id";

    /**
     * Текущие смещения для каждой партиции, ожидающие подтверждения.
     */
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Хранилище транзакций для управления состоянием обработки.
     */
    private final InMemoryStorage storage;

    /**
     * Создает новый экземпляр сервиса потребителя Kafka.
     *
     * @param config  конфигурация Kafka для настройки потребителя
     * @param storage хранилище транзакций для обработки сообщений
     */
    public KafkaConsumerService(KafkaConfig config, InMemoryStorage storage) {
        this.consumer = new KafkaConsumer<>(config.getTransactionConsumerProperties());
        this.topic = config.getTransactionConsumerProperties().getProperty("consumer.topic.name");
        this.storage = storage;
    }

    /**
     * Запускает процесс чтения и обработки сообщений из Kafka-топика. Реализует циклическое чтение
     * с обработкой ошибок и корректным завершением.
     */
    public void read() {
        consumer.subscribe(Collections.singletonList(topic));
        log.info("Запуск потребителя для топика: {}", topic);

        try {
            while (true) {
                ConsumerRecords<String, TransactionDto> records = consumer.poll(
                    Duration.ofMillis(100));
                processMessages(records);
                commitCurrentOffsets();
            }
        } catch (Exception e) {
            log.error("Критическая ошибка при чтении сообщений из топика: {}", topic, e);
            throw new RuntimeException("Ошибка при чтении сообщений", e);
        } finally {
            shutdown();
        }
    }

    /**
     * Обрабатывает полученную партию сообщений из Kafka.
     *
     * @param records набор записей из Kafka
     */
    private void processMessages(ConsumerRecords<String, TransactionDto> records) {
        for (ConsumerRecord<String, TransactionDto> record : records) {
            try {
                log.info("Получено сообщение: {}", record);
                handleMessage(record);
                trackOffset(record);
            } catch (Exception e) {
                log.error("Ошибка во время обработки сообщения: {}", record, e);
            }
        }
    }

    /**
     * Обрабатывает отдельную запись из Kafka, проверяя валидность данных.
     *
     * @param record запись из Kafka для обработки
     */
    private void processRecord(ConsumerRecord<String, TransactionDto> record) {
        TransactionDto transaction = record.value();
        String producerId = new String(record.headers().lastHeader(PRODUCER_ID).value());
        if (transaction == null || producerId.isBlank()) {
            log.warn("Пропущено невалидного сообщение: producerId={}, offset={}", producerId,
                record.offset());
            return;
        }

        storage.addTransactionForProducer(producerId, transaction);

        log.debug("Получена и обработана валидная транзакция: {}, producerId={}, offset={}",
            transaction, producerId, record.offset());
    }

    /**
     * Обрабатывает сообщение с проверкой на null и логированием.
     *
     * @param record запись из Kafka для обработки
     */
    private void handleMessage(ConsumerRecord<String, TransactionDto> record) {
        TransactionDto transactionDto = record.value();
        if (transactionDto != null) {
            log.info("Сообщение успешно обработано: {}, offset={}", transactionDto,
                record.offset());
            processRecord(record);
        } else {
            log.error("Получено сообщение с пустым значением: offset={}", record.offset());
        }
    }

    /**
     * Отслеживает смещение для записи в Kafka.
     *
     * @param record запись из Kafka
     */
    private void trackOffset(ConsumerRecord<String, TransactionDto> record) {
        currentOffsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        );
    }

    /**
     * Асинхронно подтверждает текущие смещения в Kafka.
     */
    private void commitCurrentOffsets() {
        if (!currentOffsets.isEmpty()) {
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    log.error("Ошибка при подтверждении смещений: {}", offsets, exception);
                } else {
                    log.debug("Смещения успешно подтверждены: {}", offsets);
                }
            });
        }
    }

    /**
     * Корректно завершает работу сервиса, выполняя финальное подтверждение смещений.
     */
    private void shutdown() {
        try {
            if (!currentOffsets.isEmpty()) {
                log.info("Выполнение финального коммита смещений перед завершением работы");
                consumer.commitSync(currentOffsets);
            }
        } finally {
            log.info("Завершение работы потребителя для топика: {}", topic);
            currentOffsets.clear();
            consumer.close();
        }
    }

    /**
     * Запрашивает корректное завершение работы сервиса. Реализует метод интерфейса AutoCloseable.
     */
    @Override
    public void close() {
        log.info("Запрос на завершение работы потребителя");
        consumer.wakeup();
    }
}