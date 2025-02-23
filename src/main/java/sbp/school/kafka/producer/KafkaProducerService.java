package sbp.school.kafka.producer;

import static sbp.school.kafka.util.ChecksumHelper.getIntervalKey;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.InMemoryStorage;

/**
 * Сервис для отправки транзакций в Kafka с поддержкой повторных попыток отправки. Реализует
 * механизм обработки неудачных отправок и автоматического повторения.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
@Getter
public class KafkaProducerService extends Thread implements AutoCloseable {

    /**
     * Имя топика Kafka для отправки транзакций.
     */
    private final String topic;

    /**
     * Продюсер Kafka для отправки сообщений.
     */
    private final Producer<String, TransactionDto> kafkaProducer;

    /**
     * Таймаут ожидания подтверждения отправки транзакции.
     */
    private final Duration ackTimeout;

    /**
     * Продолжительность интервала для вычисления контрольных сумм.
     */
    private final Duration checksumIntervalDuration;

    /**
     * Максимальное количество попыток повторной отправки транзакции.
     */
    private final int retryMaxCount;

    /**
     * Уникальный идентификатор продюсера для отслеживания.
     */
    private final String producerId = UUID.randomUUID().toString();

    /**
     * Хранилище транзакций для управления состоянием отправки.
     */
    private final InMemoryStorage storage;

    /**
     * Создает новый экземпляр сервиса отправки транзакций.
     *
     * @param kafkaConfig конфигурация Kafka для настройки продюсера
     * @param storage     хранилище транзакций для отслеживания состояния
     */
    public KafkaProducerService(KafkaConfig kafkaConfig, InMemoryStorage storage,  Producer<String, TransactionDto> kafkaProducer) {
        this.topic = kafkaConfig.getTransactionProducerConfig().getProperty("topic.name");
        this.kafkaProducer = kafkaProducer;
        this.ackTimeout = Duration.parse(kafkaConfig.getPropertyValue("consumer.timeout"));
        this.checksumIntervalDuration = Duration.parse(
            kafkaConfig.getPropertyValue("consumer.interval"));
        this.retryMaxCount = Integer.parseInt(
            kafkaConfig.getPropertyValue("producer.max-retry"));
        this.storage = storage;
        log.info("Сервис отправки транзакций инициализирован для топика: {}", topic);
        Runtime.getRuntime().addShutdownHook(new Thread(this::flushAndClose));
    }

    /**
     * Отправляет транзакцию в Kafka с асинхронным подтверждением.
     *
     * @param transactionDto объект транзакции для отправки
     */
    public void send(TransactionDto transactionDto) {
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topic, transactionDto);
        storage.putTransactionSendInProgress(transactionDto);

        kafkaProducer.send(record, (recordMetadata, e) -> {
            handleSendResult(recordMetadata, e, transactionDto);
        });
    }

    /**
     * Обрабатывает результат отправки сообщения в Kafka.
     *
     * @param recordMetadata Метаданные отправленного сообщения (partition, offset и т.д.).
     * @param e              Исключение, если отправка не удалась.
     */
    private void handleSendResult(RecordMetadata recordMetadata, Exception e,
        TransactionDto transactionDto) {
        if (e != null) {
            log.error("Ошибка при отправке сообщения: partition={}, offset={}",
                recordMetadata.partition(), recordMetadata.offset(), e);
        } else {
            long intervalKey = getIntervalKey(transactionDto.getDate(), checksumIntervalDuration);
            storage.putSentTransaction(intervalKey, transactionDto);
            storage.updateCheckSum(intervalKey);
            log.debug("Сообщение успешно отправлено: partition={}, offset={}",
                recordMetadata.partition(), recordMetadata.offset());
        }
    }

    /**
     * Проверяет и повторно отправляет транзакции, которые не получили подтверждение в течение
     * заданного таймаута.
     *
     * @see #ackTimeout
     */
    public void retryFailedTransactions() {
        if (storage.isSentTransactionsEmpty()) {
            log.trace("Нет транзакций для повторной отправки");
            return;
        }

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime timeoutThresholdTime = now.minus(ackTimeout);
        Long timeoutThresholdIntervalKey = getIntervalKey(timeoutThresholdTime,
            checksumIntervalDuration);

        Set<Long> intervalKeysToRetry = storage.getSentTransactionIntervalKeys().stream()
            .filter(intervalKey -> intervalKey < timeoutThresholdIntervalKey)
            .collect(Collectors.toSet());

        for (Long intervalKey : intervalKeysToRetry) {
            if (retryTransactionsForInterval(intervalKey, now) > 0) {
                log.debug("Выполнена повторная отправка транзакций для интервала: intervalKey={}",
                    intervalKey);
            }
            storage.cleanupInterval(intervalKey);
        }
    }

    /**
     * Повторно отправляет транзакции для указанного интервала времени.
     *
     * @param intervalKey ключ интервала времени
     * @param time        текущее время для создания новых транзакций
     * @return количество отправленных транзакций
     */
    private int retryTransactionsForInterval(Long intervalKey, OffsetDateTime time) {
        List<TransactionDto> transactions = storage.getSentTransactions(intervalKey);
        int transactionsSentCount = 0;

        for (TransactionDto transaction : transactions) {
            String transactionId = transaction.getId();
            int retryCount = storage.getRetryCount(transactionId);
            if (retryCount < retryMaxCount) {
                storage.putRetryCount(transactionId, ++retryCount);
                TransactionDto retryTransaction = createRetryTransaction(transaction, time);
                send(retryTransaction);
                transactionsSentCount++;
            } else {
                log.warn(
                    "Превышено максимальное количество повторных отправок для транзакции: id={}, retryCount={}",
                    transactionId, retryCount);
            }
        }

        return transactionsSentCount;
    }

    /**
     * Создает новую транзакцию для повторной отправки на основе оригинальной.
     *
     * @param original оригинальная транзакция
     * @param time     текущее время для новой транзакции
     * @return новая транзакция с обновленным временем
     */
    private TransactionDto createRetryTransaction(TransactionDto original, OffsetDateTime time) {
        return new TransactionDto(
            original.getId(),
            original.getOperationType(),
            original.getAmount(),
            original.getAccount(),
            time
        );
    }

    /**
     * Запускает процесс повторной отправки неудачных транзакций. Реализует метод интерфейса
     * Runnable.
     */
    @Override
    public void run() {
        retryFailedTransactions();
    }

    /**
     * Завершает работу сервиса и освобождает ресурсы Kafka-продюсера.
     */
    @Override
    public void close() {
        log.info("Завершение работы сервиса отправки транзакций");
        kafkaProducer.close();
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
            close();
        }
    }
}