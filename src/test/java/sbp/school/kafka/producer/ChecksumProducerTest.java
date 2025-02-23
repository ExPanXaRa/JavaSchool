package sbp.school.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static sbp.school.kafka.constant.TransactionConstant.createTransactionDto;

import java.time.OffsetDateTime;
import java.util.List;
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
import sbp.school.kafka.dto.ChecksumDto;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.InMemoryStorage;
import sbp.school.kafka.util.ChecksumSerializer;

/**
 * Тесты для {@link ChecksumProducer}.
 */
@ExtendWith(MockitoExtension.class)
class ChecksumProducerTest {

    private static final String TOPIC_NAME = "sber-test";
    private static final String PRODUCER_ID = "producer-id";
    private static final String CONSUMER_TIMEOUT = "PT10M";
    private static final String PRODUCER_INTERVAL = "PT5M";

    private MockProducer<String, ChecksumDto> mockProducer;
    private ChecksumProducer checksumProducer;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private InMemoryStorage inMemoryStorage;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new ChecksumSerializer());

        when(kafkaConfig.getPropertyValue("producer.topic.name")).thenReturn(TOPIC_NAME);
        when(kafkaConfig.getPropertyValue("consumer.timeout")).thenReturn(CONSUMER_TIMEOUT);
        when(kafkaConfig.getPropertyValue("producer.interval")).thenReturn(PRODUCER_INTERVAL);

        checksumProducer = new ChecksumProducer(kafkaConfig, inMemoryStorage, mockProducer);
    }

    /**
     * Проверяет, что подтверждения успешно отправляются, если есть транзакции для отправки.
     */
    @Test
    void sendConfirmations_ShouldSendSuccessfully() {
        // Подготовка
        OffsetDateTime testTime = OffsetDateTime.now();
        TransactionDto transaction = createTransactionDto(testTime);

        when(inMemoryStorage.getAllProducerIds()).thenReturn(Set.of(PRODUCER_ID));
        when(inMemoryStorage.isEmpty()).thenReturn(false);
        when(inMemoryStorage.getTransactionsByProducer(PRODUCER_ID)).thenReturn(
            List.of(transaction));

        // Вызов
        checksumProducer.sendConfirmations();

        // Проверка
        List<ProducerRecord<String, ChecksumDto>> history = mockProducer.history();
        assertEquals(1, history.size(), "Должно быть отправлено одно сообщение");
        ProducerRecord<String, ChecksumDto> record = history.get(0);
        assertEquals(TOPIC_NAME, record.topic(), "Топик должен соответствовать ожидаемому");
        assertNotNull(record.value().getChecksum(), "Контрольная сумма не должна быть null");
    }

    /**
     * Проверяет, что подтверждения не отправляются, если нет транзакций для отправки.
     */
    @Test
    void sendConfirmations_ShouldNotSendWhenNoTransactions() {
        // Подготовка
        when(inMemoryStorage.isEmpty()).thenReturn(true);

        // Вызов
        checksumProducer.sendConfirmations();

        // Проверка
        assertTrue(mockProducer.history().isEmpty(),
            "Не должно быть отправлено ни одного сообщения");
    }
}