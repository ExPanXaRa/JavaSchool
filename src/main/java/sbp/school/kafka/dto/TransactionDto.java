package sbp.school.kafka.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import sbp.school.kafka.util.enums.TransactionOperationType;

/**
 * DTO для представления транзакции в системе.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Getter
@Builder
@ToString
@AllArgsConstructor
public final class TransactionDto {

    /**
     * Уникальный идентификатор транзакции.
     */
    private final String id;

    /**
     * Тип операции транзакции (TRANSFER, DEPOSIT, COMMISSIONS)
     */
    private final TransactionOperationType operationType;

    /**
     * Сумма транзакции
     */
    private final BigDecimal amount;

    /**
     * Номер счета, с которым производится операция
     */
    private final String account;

    /**
     * Временная метка транзакции с учетом временной зоны
     */
    private final OffsetDateTime date;
}