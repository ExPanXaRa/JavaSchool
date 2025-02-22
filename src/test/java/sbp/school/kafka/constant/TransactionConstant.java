package sbp.school.kafka.constant;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.util.enums.TransactionOperationType;

public class TransactionConstant {

    public static List<TransactionDto> createTestTransactions() {
        return Arrays.asList(
            new TransactionDto(
                TransactionOperationType.DEPOSIT,
                new BigDecimal("321.23"),
                "dsfhghdggDzdGSfhdfxg",
                OffsetDateTime.now()
            ),
            new TransactionDto(
                TransactionOperationType.TRANSFER,
                new BigDecimal("52412.43"),
                "dsfhghdggDzdGSfhdfxg",
                OffsetDateTime.now()
            ),
            new TransactionDto(
                TransactionOperationType.COMMISSIONS,
                new BigDecimal("76523.21"),
                "dsfhghdggDzdGSfhdfxg",
                OffsetDateTime.now()
            )
        );
    }
}
