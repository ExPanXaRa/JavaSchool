package sbp.school.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.constant.TransactionConstant.createTransactionDto;
import static sbp.school.kafka.util.ChecksumHelper.getIntervalKey;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.InMemoryStorage;
import sbp.school.kafka.util.TransactionSerializer;

/**
 * Тесты для {@link KafkaProducerService}.
 */
@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    private static final String TEST_TOPIC = "sber-test";
    private static final String TRANSACTION_ID = "test";
    private static final String ERROR_TRANSACTION_ID = "error";
    private static final Duration CONSUMER_TIMEOUT = Duration.parse("PT10M");
    private static final Duration CONSUMER_INTERVAL = Duration.parse("PT5M");
    private static final int MAX_RETRY_COUNT = 3;

    private MockProducer<String, TransactionDto> mockProducer;
    private KafkaProducerService kafkaProducerService;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private InMemoryStorage inMemoryStorage;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(),
            new TransactionSerializer());

        Properties producerProperties = new Properties();
        producerProperties.setProperty("topic.name", TEST_TOPIC);

        when(kafkaConfig.getPropertyValue("consumer.timeout")).thenReturn(
            CONSUMER_TIMEOUT.toString());
        when(kafkaConfig.getPropertyValue("consumer.interval")).thenReturn(
            CONSUMER_INTERVAL.toString());
        when(kafkaConfig.getPropertyValue("producer.max-retry")).thenReturn(
            String.valueOf(MAX_RETRY_COUNT));
        when(kafkaConfig.getTransactionProducerConfig()).thenReturn(producerProperties);

        kafkaProducerService = new KafkaProducerService(kafkaConfig, inMemoryStorage, mockProducer);
    }

    /**
     * Проверяет, что транзакция успешно отправляется в Kafka.
     */
    @Test
    void send_expectSuccess() {
        // Подготовка
        TransactionDto transactionDto = createTransactionDto(TRANSACTION_ID);

        // Вызов
        kafkaProducerService.send(transactionDto);

        // Проверка
        assertEquals(1, mockProducer.history().size(), "Должно быть отправлено одно сообщение");
        ProducerRecord<String, TransactionDto> transactionDtoProducerRecord = mockProducer.history()
            .get(0);
        assertEquals(TEST_TOPIC, transactionDtoProducerRecord.topic(),
            "Топик должен соответствовать ожидаемому");
        assertEquals(transactionDto, transactionDtoProducerRecord.value(),
            "Отправленная транзакция должна соответствовать ожидаемой");
        verify(inMemoryStorage).putTransactionSendInProgress(transactionDto);
    }

    /**
     * Проверяет, что истекшие транзакции повторно отправляются.
     */
    @Test
    void retryFailedTransactions_expectSuccess() {
        // Подготовка
        OffsetDateTime testTime = OffsetDateTime.now().minus(Duration.parse("PT15M"));
        TransactionDto expiredTransaction = createTransactionDto(testTime);

        long intervalKey = getIntervalKey(testTime, CONSUMER_INTERVAL);
        when(inMemoryStorage.isSentTransactionsEmpty()).thenReturn(false);
        when(inMemoryStorage.getSentTransactionIntervalKeys()).thenReturn(Set.of(intervalKey));
        when(inMemoryStorage.getSentTransactions(intervalKey)).thenReturn(
            List.of(expiredTransaction));
        when(inMemoryStorage.getRetryCount(ERROR_TRANSACTION_ID)).thenReturn(1);

        // Вызов
        kafkaProducerService.retryFailedTransactions();

        // Проверка
        verify(inMemoryStorage).putRetryCount(ERROR_TRANSACTION_ID, 2);
        assertEquals(1, mockProducer.history().size(), "Должно быть отправлено одно сообщение");
    }

    /**
     * Проверяет, что транзакции не отправляются повторно, если превышено максимальное количество
     * попыток.
     */
    @Test
    void retryFailedTransactions_expectNotSentTransactions() {
        // Подготовка
        OffsetDateTime testTime = OffsetDateTime.now().minus(Duration.parse("PT15M"));
        TransactionDto maxRetryTransaction = createTransactionDto(testTime);

        long intervalKey = getIntervalKey(testTime, CONSUMER_INTERVAL);
        when(inMemoryStorage.isSentTransactionsEmpty()).thenReturn(false);
        when(inMemoryStorage.getSentTransactionIntervalKeys()).thenReturn(Set.of(intervalKey));
        when(inMemoryStorage.getSentTransactions(intervalKey)).thenReturn(
            List.of(maxRetryTransaction));
        when(inMemoryStorage.getRetryCount(ERROR_TRANSACTION_ID)).thenReturn(MAX_RETRY_COUNT);

        // Вызов
        kafkaProducerService.retryFailedTransactions();

        // Проверка
        assertTrue(mockProducer.history().isEmpty(),
            "Не должно быть отправлено ни одного сообщения");
    }
}