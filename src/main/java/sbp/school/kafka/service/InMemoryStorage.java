package sbp.school.kafka.service;

import static sbp.school.kafka.util.ChecksumHelper.calculateChecksum;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import sbp.school.kafka.dto.TransactionDto;

/**
 * Хранилище в памяти для управления состоянием транзакций. Реализует потокобезопасное хранение
 * информации о отправленных транзакциях, их повторных попытках и контрольных суммах.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Getter
public class InMemoryStorage {

    /**
     * Хранилище отправленных транзакций, сгруппированных по ключу интервала времени. Использует
     * ConcurrentHashMap для потокобезопасного доступа.
     */
    private final Map<Long, List<TransactionDto>> sentTransactions = new ConcurrentHashMap<>();

    /**
     * Хранилище количества попыток повторной отправки для каждой транзакции. Использует
     * ConcurrentHashMap для потокобезопасного доступа.
     */
    private final Map<String, Integer> retryCountMap = new ConcurrentHashMap<>();

    /**
     * Хранилище контрольных сумм для каждого интервала времени. Использует ConcurrentHashMap для
     * потокобезопасного доступа.
     */
    private final Map<Long, String> sentChecksumMap = new ConcurrentHashMap<>();

    /**
     * Хранилище транзакций, находящихся в процессе отправки. Использует ConcurrentHashMap для
     * потокобезопасного доступа.
     */
    private final Map<String, TransactionDto> transactionsSendInProgress = new ConcurrentHashMap<>();

    /**
     * Устанавливает количество попыток повторной отправки для транзакции.
     *
     * @param transactionId идентификатор транзакции
     * @param retryCount    количество попыток
     */
    public void putRetryCount(String transactionId, int retryCount) {
        retryCountMap.put(transactionId, retryCount);
    }

    /**
     * Получает список отправленных транзакций для указанного интервала времени.
     *
     * @param intervalKey ключ интервала времени
     * @return список транзакций или null если интервал не существует
     */
    public List<TransactionDto> getSentTransactions(long intervalKey) {
        return sentTransactions.get(intervalKey);
    }

    /**
     * Получает контрольную сумму для указанного интервала времени.
     *
     * @param intervalKey ключ интервала времени
     * @return контрольная сумма или null если интервал не существует
     */
    public String getSentCheckSum(long intervalKey) {
        return sentChecksumMap.get(intervalKey);
    }

    /**
     * Проверяет, есть ли отправленные транзакции в хранилище.
     *
     * @return true если нет отправленных транзакций, false если есть
     */
    public boolean isSentTransactionsEmpty() {
        return sentTransactions.isEmpty();
    }

    /**
     * Проверяет, есть ли транзакции в процессе отправки.
     *
     * @return true если нет транзакций в процессе отправки, false если есть
     */
    public boolean isTransactionsSendInProgressEmpty() {
        return transactionsSendInProgress.isEmpty();
    }

    /**
     * Обновляет checksum.
     */
    public void updateCheckSum(long intervalKey) {
        sentChecksumMap.put(intervalKey, calculateChecksum(
            sentTransactions.get(intervalKey).stream().map(TransactionDto::getId).collect(
                Collectors.toList())));
    }

    public void putTransactionSendInProgress(TransactionDto transaction) {
        transactionsSendInProgress.put(transaction.getId(), transaction);
    }

    public void putSentTransaction(long intervalKey, TransactionDto transaction) {
        sentTransactions.computeIfAbsent(intervalKey, k -> new ArrayList<>()).add(transaction);

    }

    /**
     * Получает набор всех ключей интервалов времени с отправленными транзакциями.
     *
     * @return множество ключей интервалов времени
     */
    public Set<Long> getSentTransactionIntervalKeys() {
        return sentTransactions.keySet();
    }

    /**
     * Получает количество попыток повторной отправки для транзакции. Если транзакция не найдена,
     * возвращает 0.
     *
     * @param transactionId идентификатор транзакции
     * @return количество попыток повторной отправки
     */
    public int getRetryCount(String transactionId) {
        Integer retryCount = retryCountMap.get(transactionId);
        if (retryCount == null) {
            retryCount = 0;
        }
        return retryCount;
    }

    /**
     * Очищает данные для указанного интервала времени. Удаляет как сами транзакции, так и их
     * контрольную сумму.
     *
     * @param intervalKey ключ интервала времени для очистки
     */
    public void cleanupInterval(Long intervalKey) {
        sentTransactions.remove(intervalKey);
        sentChecksumMap.remove(intervalKey);
    }
}
