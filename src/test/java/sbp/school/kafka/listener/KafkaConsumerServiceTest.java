package sbp.school.kafka.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.constant.TransactionConstant.createConsumerRecord;
import static sbp.school.kafka.constant.TransactionConstant.createTransactionDto;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.InMemoryStorage;

/**
 * Тесты для {@link KafkaConsumerService}.
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

    private static final String TOPIC_NAME = "test-topic";
    private static final String PRODUCER_ID = "producer-id";
    private static final String TEST_TOPIC = "sber-test";

    private MockConsumer<String, TransactionDto> mockConsumer;
    private KafkaConsumerService kafkaConsumerService;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private InMemoryStorage inMemoryStorage;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        Properties properties = new Properties();
        properties.setProperty("topic.name", TEST_TOPIC);
        when(kafkaConfig.getTransactionConsumerProperties()).thenReturn(properties);

        kafkaConsumerService = new KafkaConsumerService(kafkaConfig, inMemoryStorage, mockConsumer);

        mockConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        mockConsumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
        mockConsumer.updateBeginningOffsets(
            Collections.singletonMap(new TopicPartition(TOPIC_NAME, 0), 0L));
    }

    @AfterEach
    void tearDown() {
        mockConsumer.close();
    }

    /**
     * Проверяет, что сообщение с корректными данными успешно обрабатывается.
     */
    @Test
    void read_ShouldProcessValidMessage() throws InterruptedException {
        // Подготовка
        TransactionDto transaction = createTransactionDto();
        ConsumerRecord<String, TransactionDto> record = createConsumerRecord(TOPIC_NAME,
            transaction);
        record.headers().add(PRODUCER_ID, PRODUCER_ID.getBytes());
        mockConsumer.addRecord(record);

        // Вызов
        CompletableFuture.runAsync(() -> kafkaConsumerService.read());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Проверка
        verify(inMemoryStorage).addTransactionForProducer(PRODUCER_ID, transaction);
    }

    /**
     * Проверяет, что сообщение без заголовка PRODUCER_ID пропускается.
     */
    @Test
    void read_ShouldSkipMessageWithoutProducerId() throws InterruptedException {
        // Подготовка
        TransactionDto transaction = createTransactionDto();
        ConsumerRecord<String, TransactionDto> record = createConsumerRecord(TOPIC_NAME,
            transaction);
        mockConsumer.addRecord(record);

        // Вызов
        CompletableFuture.runAsync(() -> kafkaConsumerService.read());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Проверка
        verify(inMemoryStorage, never()).addTransactionForProducer(any(), any());
    }

    /**
     * Проверяет, что сообщение с null-транзакцией пропускается.
     */
    @Test
    void read_ShouldSkipMessageWithNullTransaction() throws InterruptedException {
        // Подготовка
        ConsumerRecord<String, TransactionDto> record = createConsumerRecord(TOPIC_NAME, null);
        record.headers().add(PRODUCER_ID, PRODUCER_ID.getBytes());
        mockConsumer.addRecord(record);

        // Вызов
        CompletableFuture.runAsync(() -> kafkaConsumerService.read());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Проверка
        verify(inMemoryStorage, never()).addTransactionForProducer(any(), any());
    }
}