package sbp.school.kafka.util.enums;

/**
 * Типы операций с транзакциями в системе.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
public enum TransactionOperationType {
  /**
   * Перевод средств между счетами
   */
  TRANSFER,

  /**
   * Пополнение счета
   */
  DEPOSIT,

  /**
   * Комиссионные операции
   */
  COMMISSIONS
}