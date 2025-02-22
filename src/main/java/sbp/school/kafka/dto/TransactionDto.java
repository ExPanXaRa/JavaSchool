package sbp.school.kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
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
public final class TransactionDto {

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

    /**
     * Создает новый экземпляр TransactionDto из указанных параметров.
     *
     * @param operationType тип операции над транзакцией
     * @param amount        сумма транзакции
     * @param account       номер счета
     * @param date          время выполнения операции
     */
    @JsonCreator
    public TransactionDto(
        @JsonProperty("operationType") TransactionOperationType operationType,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("account") String account,
        @JsonProperty("date") OffsetDateTime date) {
        this.operationType = operationType;
        this.amount = amount;
        this.account = account;
        this.date = date;
    }
}