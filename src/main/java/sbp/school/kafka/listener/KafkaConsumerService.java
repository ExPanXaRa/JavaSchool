package sbp.school.kafka.listener;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.dto.TransactionDto;

/**
 * Сервис для чтения и обработки сообщений из Kafka-топика. Реализует автоматическое управление
 * смещениями и асинхронное подтверждение.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class KafkaConsumerService implements AutoCloseable {

    /**
     * Имя топика Kafka для подписки.
     */
    private final String topic;

    /**
     * Потребитель Kafka для чтения сообщений.
     */
    private final KafkaConsumer<String, TransactionDto> consumer;

    /**
     * Текущие смещения для каждой партиции.
     */
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Создает новый экземпляр сервиса с указанными свойствами потребителя.
     *
     * @param kafkaConsumerProperties свойства конфигурации потребителя Kafka
     */
    public KafkaConsumerService(Properties kafkaConsumerProperties) {
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.topic = kafkaConsumerProperties.getProperty("topic.name");
    }

    /**
     * Начинает непрерывное чтение сообщений из топика. Выполняет следующие операции: 1.
     * Подписывается на указанный топик 2. Периодически опрашивает брокеры на наличие новых
     * сообщений 3. Обрабатывает полученные записи 4. Сохраняет смещения в асинхронном режиме
     *
     * @throws RuntimeException если возникла критическая ошибка при чтении
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
     * Обрабатывает набор полученных сообщений из Kafka.
     *
     * @param records набор записей для обработки
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
     * Обрабатывает отдельную запись из Kafka.
     *
     * @param record запись для обработки
     */
    private void handleMessage(ConsumerRecord<String, TransactionDto> record) {
        TransactionDto transactionDto = record.value();
        if (transactionDto != null) {
            log.info("Сообщение успешно обработано: {}, offset={}", transactionDto,
                record.offset());
        } else {
            log.error("Получено сообщение с пустым значением: offset={}", record.offset());
        }
    }

    /**
     * Отслеживает смещение для записи.
     *
     * @param record запись для отслеживания
     */
    private void trackOffset(ConsumerRecord<String, TransactionDto> record) {
        currentOffsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        );
    }

    /**
     * Выполняет асинхронное подтверждение текущих смещений. Логирует успешное подтверждение или
     * ошибку.
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
     * Выполняет корректное завершение работы сервиса: 1. Выполняет финальное синхронное
     * подтверждение смещений 2. Очищает текущие смещения 3. Закрывает потребитель
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
     * Запрашивает корректное завершение работы потребителя. Вызывает метод wakeup() для прерывания
     * текущего poll.
     */
    @Override
    public void close() {
        log.info("Запрос на завершение работы потребителя");
        consumer.wakeup();
    }
}