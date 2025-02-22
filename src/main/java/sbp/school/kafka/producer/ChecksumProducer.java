package sbp.school.kafka.producer;

import static sbp.school.kafka.util.ChecksumHelper.calculateChecksum;
import static sbp.school.kafka.util.ChecksumHelper.getIntervalKey;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.ChecksumDto;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.InMemoryStorage;

/**
 * Потокобезопасный продюсер Kafka для отправки подтверждений транзакций. Реализует автоматическое
 * управление ресурсами через интерфейс AutoCloseable. Отправляет контрольные суммы для
 * подтвержденных транзакций в Kafka-топик.
 *
 * @since 1.0
 */
@Slf4j
public class ChecksumProducer extends Thread implements AutoCloseable {

    /**
     * Имя Kafka-топика для отправки подтверждений.
     */
    private final String topicName;

    /**
     * Продюсер Kafka для отправки сообщений.
     */
    private final KafkaProducer<String, ChecksumDto> producer;

    /**
     * Хранилище транзакций для доступа к данным.
     */
    private final InMemoryStorage storage;

    /**
     * Таймаут для получения транзакций.
     */
    private final Duration receiveTimeout;

    /**
     * Интервал времени для группировки транзакций при отправке подтверждений.
     */
    private final Duration checksumIntervalDuration;

    /**
     * Создает новый экземпляр продюсера с указанными конфигурацией и хранилищем.
     *
     * @param config  конфигурация Kafka
     * @param storage хранилище транзакций
     */
    public ChecksumProducer(KafkaConfig config, InMemoryStorage storage) {
        this.producer = new KafkaProducer<>(config.getChecksumProducerProperties());
        this.topicName = config.getPropertyValue("producer.topic.name");
        this.storage = storage;
        this.receiveTimeout = Duration.parse(config.getPropertyValue("consumer.timeout"));
        this.checksumIntervalDuration = Duration.parse(
            config.getPropertyValue("producer.interval"));
        log.info("Сервис отправки подтверждений инициализирован для топика: {}", topicName);
    }

    /**
     * Отправляет подтверждения для всех готовых транзакций. Группирует транзакции по интервалам
     * времени и вычисляет контрольные суммы. Удаляет подтвержденные транзакции из хранилища.
     */
    public void sendConfirmations() {
        if (storage.isEmpty()) {
            log.trace("Нет транзакций для отправки подтверждений");
            return;
        }

        log.trace("Начало отправки подтверждений");

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime timeoutThresholdTime = now.minus(receiveTimeout);
        long timeoutThresholdIntervalKey = getIntervalKey(timeoutThresholdTime,
            checksumIntervalDuration);
        Set<String> producerIds = storage.getAllProducerIds();
        int ackCounter = 0;

        for (String producerId : producerIds) {
            List<TransactionDto> processedTransactions = storage.getTransactionsByProducer(
                producerId);
            Map<Long, List<String>> transactionIdsToAck = new HashMap<>();

            for (TransactionDto transaction : processedTransactions) {
                long intervalKey = getIntervalKey(transaction.getDate(), checksumIntervalDuration);
                if (intervalKey < timeoutThresholdIntervalKey) {
                    transactionIdsToAck.computeIfAbsent(intervalKey, k -> new ArrayList<>())
                        .add(transaction.getId());
                }
            }

            for (Long intervalKey : transactionIdsToAck.keySet()) {
                ++ackCounter;
                sendConfirmationForInterval(producerId, intervalKey,
                    transactionIdsToAck.get(intervalKey));
            }
        }

        if (ackCounter == 0) {
            log.debug("Нет подтверждений, готовых для отправки");
        }
    }

    /**
     * Отправляет подтверждение для конкретного интервала времени. Вычисляет контрольную сумму для
     * списка транзакций и отправляет её в Kafka.
     *
     * @param producerId     идентификатор продюсера
     * @param intervalKey    ключ интервала времени
     * @param transactionIds список идентификаторов транзакций
     */
    private void sendConfirmationForInterval(String producerId, Long intervalKey,
        List<String> transactionIds) {
        String checkSum = calculateChecksum(transactionIds);
        ChecksumDto ack = new ChecksumDto(intervalKey.toString(), checkSum);
        ProducerRecord<String, ChecksumDto> record = new ProducerRecord<>(topicName, ack);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка при отправке подтверждения: partition={}, offset={}",
                    metadata.partition(), metadata.offset(), exception);
            } else {
                storage.removeTransactionsForProducer(producerId, transactionIds);
                log.debug(
                    "Подтверждение успешно отправлено: producerId={}, intervalKey={}, checkSum={}, partition={}, offset={}",
                    producerId, ack.getIntervalKey(), ack.getChecksum(), metadata.partition(),
                    metadata.offset());
            }
        });
    }

    /**
     * Запускает продюсер в отдельном потоке.
     */
    @Override
    public void run() {
        sendConfirmations();
    }

    /**
     * Закрывает продюсер и освобождает все ресурсы. Должен быть вызван при завершении работы для
     * корректного освобождения ресурсов Kafka.
     */
    @Override
    public void close() {
        log.info("Завершение работы сервиса отправки подтверждений");
        producer.close();
    }
}