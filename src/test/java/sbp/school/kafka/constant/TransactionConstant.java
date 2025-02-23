package sbp.school.kafka.constant;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.util.enums.TransactionOperationType;

public class TransactionConstant {


    public static TransactionDto createTransactionDto(String id) {
        return new TransactionDto(
            id,
            TransactionOperationType.DEPOSIT,
            new BigDecimal("123.43"),
            "12345tyfhgfdsetruyt",
            OffsetDateTime.now()
        );
    }

    public static TransactionDto createTransactionDto(OffsetDateTime date) {
        return new TransactionDto(
            "error",
            TransactionOperationType.DEPOSIT,
            new BigDecimal("123.43"),
            "12345tyfhgfdsetruyt",
            date
        );
    }


}
