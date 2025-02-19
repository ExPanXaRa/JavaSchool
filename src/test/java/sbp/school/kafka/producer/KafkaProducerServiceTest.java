package sbp.school.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.constant.TransactionConstant;
import sbp.school.kafka.dto.TransactionDto;

@Slf4j
public class KafkaProducerServiceTest {

    @Test
    void testSendMultipleTransactions() {
        // Подготовка
        List<TransactionDto> transactions = TransactionConstant.createTestTransactions();
        KafkaConfig kafkaConfig = new KafkaConfig();
        KafkaProducerService producer = new KafkaProducerService(kafkaConfig.getProperties());

        // Вызов метода и проверка результата
        assertDoesNotThrow(() -> transactions.forEach(transaction -> {
            producer.send(transaction);
            log.info("Тестовая транзакция отправлена: {}", transaction);
        }));
    }
}