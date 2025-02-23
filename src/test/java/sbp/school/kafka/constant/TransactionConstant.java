package sbp.school.kafka.constant;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.util.enums.TransactionOperationType;

public class TransactionConstant {


    public static TransactionDto createTransactionDto(OffsetDateTime time) {
        return new TransactionDto(
            "testtesttest",
            TransactionOperationType.DEPOSIT,
            new BigDecimal("43214235"),
            "qwtertgsdf",
            time.minus(Duration.parse("PT15M")));
    }

    public static TransactionDto createTransactionDto() {
        return new TransactionDto(
            "testtesttesttest",
            TransactionOperationType.DEPOSIT,
            new BigDecimal("234.23"),
            "wqer34tgdfgsdfg",
            OffsetDateTime.now()
        );
    }

    public static ConsumerRecord<String, TransactionDto> createConsumerRecord(String topicName,
        TransactionDto transactionDto) {
        return new ConsumerRecord<>(
            topicName,
            0,
            0,
            null,
            transactionDto
        );
    }
}
