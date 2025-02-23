package sbp.school.kafka.listener;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.ChecksumDto;
import sbp.school.kafka.service.InMemoryStorage;

/**
 * Потребитель Kafka для обработки подтверждений контрольных сумм транзакций. Реализует паттерн
 * Consumer с поддержкой асинхронного подтверждения офсетов. Обеспечивает обработку ошибок и
 * корректное завершение работы.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class ChecksumConsumer extends Thread implements AutoCloseable {

    /**
     * Имя топика Kafka для получения подтверждений контрольных сумм.
     */
    private final String topicName;

    /**
     * Потребитель Kafka для чтения записей из топика.
     */
    private final Consumer<String, ChecksumDto> consumer;

    /**
     * Текущие офсеты для каждой партиции, ожидающие подтверждения.
     */
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Хранилище транзакций для проверки контрольных сумм.
     */
    private final InMemoryStorage storage;

    /**
     * Создает новый потребитель подтверждений контрольных сумм.
     *
     * @param config  конфигурация Kafka для настройки потребителя
     * @param storage хранилище транзакций для проверки контрольных сумм
     */
    public ChecksumConsumer(KafkaConfig config, InMemoryStorage storage, Consumer<String, ChecksumDto> consumer) {
        Properties consumerProperties = config.getTransactionAckConsumerConfig();
        this.consumer = consumer;
        this.topicName = config.getPropertyValue("consumer.topic.name");
        this.storage = storage;
        log.info("Потребитель подтверждений инициализирован для топика: {}", topicName);
    }

    /**
     * Основной метод потребления записей из Kafka. Обрабатывает подтверждения контрольных сумм и
     * управляет офсетами.
     */
    public void consumeChecksums() {
        consumer.subscribe(Collections.singletonList(topicName));
        log.info("Потребитель подтверждений подписан на топик: {}", topicName);

        try {
            while (true) {
                ConsumerRecords<String, ChecksumDto> records = consumer.poll(
                    Duration.ofMillis(100));
                for (ConsumerRecord<String, ChecksumDto> record : records) {
                    try {
                        processChecksumRecord(record);

                        currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                        );
                    } catch (Exception e) {
                        log.error("Ошибка обработки подтверждения: {}", record, e);
                    }
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitAsync(this::handleCommitCompletion);
                }
            }
        } catch (Exception e) {
            log.error("Неожиданная ошибка в работе потребителя подтверждений", e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                }
            } finally {
                log.info("Завершение работы потребителя подтверждений");
                currentOffsets.clear();
                consumer.close();
            }
        }
    }

    /**
     * Обрабатывает результат асинхронного подтверждения офсетов.
     *
     * @param offsets   подтверждаемые офсеты
     * @param exception возникшая ошибка (null если подтверждение успешно)
     */
    private void handleCommitCompletion(Map<TopicPartition, OffsetAndMetadata> offsets,
        Exception exception) {
        if (exception != null) {
            log.error("Ошибка при асинхронном коммите offset={}", offsets, exception);
        }
    }

    /**
     * Обрабатывает отдельную запись подтверждения контрольной суммы. Проверяет корректность ключа
     * интервала и сравнивает контрольные суммы.
     *
     * @param record запись Kafka с данными подтверждения
     */
    private void processChecksumRecord(ConsumerRecord<String, ChecksumDto> record) {
        ChecksumDto ack = record.value();
        long intervalKey;
        try {
            intervalKey = Long.parseLong(ack.getIntervalKey());
        } catch (NumberFormatException e) {
            log.warn(
                "Пропущено подтверждение с некорректным ключом интервала: intervalKey={}, offset={}",
                ack.getIntervalKey(), record.offset());
            return;
        }

        String ackChecksum = ack.getChecksum();
        String sentChecksum = storage.getSentCheckSum(intervalKey);

        if (sentChecksum == null) {
            log.warn(
                "Получено подтверждение для несуществующего интервала: ackChecksum={}, intervalKey={}, offset={}",
                ackChecksum, intervalKey, record.offset());
        } else if (ackChecksum.equals(sentChecksum)) {
            storage.cleanupInterval(intervalKey);
            log.debug("Подтверждение успешно обработано: intervalKey={}, offset={}", intervalKey,
                record.offset());
        } else {
            log.warn(
                "Контрольная сумма в подтверждении не совпадает: ackChecksum={}, sentChecksum={}, intervalKey={}, offset={}",
                ackChecksum, sentChecksum, intervalKey, record.offset());
        }
    }

    /**
     * Запускает потребление записей из Kafka. Реализует метод интерфейса Runnable.
     */
    @Override
    public void run() {
        consumeChecksums();
    }

    /**
     * Завершает работу потребителя и освобождает ресурсы. Вызывает метод wakeup() для корректного
     * завершения потребителя Kafka.
     */
    @Override
    public void close() {
        log.info("Завершение работы потребителя подтверждений");
        consumer.wakeup();
    }
}