package sbp.school.kafka.listener;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.constant.ChecksumConstant.createConsumerRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import sbp.school.kafka.dto.ChecksumDto;
import sbp.school.kafka.producer.KafkaProducerService;
import sbp.school.kafka.service.InMemoryStorage;

/**
 * Тесты для {@link ChecksumConsumer}.
 */
@ExtendWith(MockitoExtension.class)
class ChecksumConsumerTest {

    private static final String TOPIC_NAME = "checksum";
    private static final String GROUP_ID = "sber-test";
    private static final Long INTERVAL_KEY = 1234L;
    private static final String TEST_CHECKSUM = "checksum";
    private static final String CORRECT_CHECKSUM = "sadft8w4ir7twiaeyfhawlesr";
    private static final String WRONG_CHECKSUM = "error";

    private MockConsumer<String, ChecksumDto> mockConsumer;
    private ChecksumConsumer checksumConsumer;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private KafkaProducerService kafkaProducerService;

    @Mock
    private InMemoryStorage inMemoryStorage;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        lenient().when(kafkaConfig.getPropertyValue("producer.topic.name")).thenReturn(TOPIC_NAME);
        lenient().when(kafkaConfig.getPropertyValue("consumer.topic.name")).thenReturn(TOPIC_NAME);
        lenient().when(kafkaConfig.getTransactionAckConsumerConfig())
            .thenReturn(consumerProperties);

        checksumConsumer = new ChecksumConsumer(kafkaConfig, inMemoryStorage, mockConsumer);

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
     * Проверяет, что корректное сообщение с контрольной суммой обрабатывается успешно.
     */
    @Test
    void consumeChecksums_expectSuccess() throws InterruptedException {
        // Подготовка
        ChecksumDto checksumDto = new ChecksumDto(INTERVAL_KEY.toString(), TEST_CHECKSUM);
        ConsumerRecord<String, ChecksumDto> record = createConsumerRecord(TOPIC_NAME, checksumDto);

        when(inMemoryStorage.getSentCheckSum(INTERVAL_KEY)).thenReturn(TEST_CHECKSUM);
        mockConsumer.addRecord(record);

        // Вызов
        CompletableFuture.runAsync(() -> checksumConsumer.consumeChecksums());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Подтверждение
        verify(inMemoryStorage).getSentCheckSum(INTERVAL_KEY);
        verify(inMemoryStorage).cleanupInterval(INTERVAL_KEY);
    }

    /**
     * Проверяет, что сообщение с неверной контрольной суммой не обрабатывается.
     */
    @Test
    void consumeChecksums_expectMismatchNotConsume() throws InterruptedException {
        // Подготовка
        ChecksumDto checksumDto = new ChecksumDto(INTERVAL_KEY.toString(), WRONG_CHECKSUM);
        ConsumerRecord<String, ChecksumDto> record = createConsumerRecord(TOPIC_NAME, checksumDto);

        when(inMemoryStorage.getSentCheckSum(INTERVAL_KEY)).thenReturn(CORRECT_CHECKSUM);
        mockConsumer.addRecord(record);

        // Вызов
        CompletableFuture.runAsync(() -> checksumConsumer.consumeChecksums());
        Thread.sleep(100);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        // Подтверждение
        verify(inMemoryStorage).getSentCheckSum(INTERVAL_KEY);
        verify(inMemoryStorage, never()).cleanupInterval(anyLong());
    }
}