package sbp.school.kafka.entity;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Random;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import sbp.school.kafka.util.enums.TransactionOperationType;

/**
 * Entity для представления транзакции в системе.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class Transaction {

    /**
     * Уникальный идентификатор транзакции.
     */
    private Long id = new Random().nextLong();

    /**
     * Тип операции транзакции (TRANSFER, DEPOSIT, COMMISSIONS)
     */
    private TransactionOperationType operationType;

    /**
     * Сумма транзакции
     */
    private BigDecimal amount;

    /**
     * Номер счета, с которым производится операция
     */
    private String account;

    /**
     * Временная метка транзакции с учетом временной зоны
     */
    private OffsetDateTime date;
}