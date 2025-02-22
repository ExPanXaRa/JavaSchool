package sbp.school.kafka.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.dto.TransactionDto;

/**
 * Хранилище для временного хранения транзакций в памяти с поддержкой потокобезопасного доступа.
 * Предоставляет операции для управления транзакциями, сгруппированными по идентификаторам
 * продюсеров.
 *
 * @since 1.0
 */
@Slf4j
public class InMemoryStorage {

    /**
     * Потокобезопасная карта для хранения транзакций, сгруппированных по идентификаторам
     * продюсеров. Каждый продюсер имеет свою список транзакций.
     */
    private final Map<String, List<TransactionDto>> transactionsByProducer = new ConcurrentHashMap<>();

    /**
     * Получает список всех транзакций для указанного продюсера.
     *
     * @param producerId идентификатор продюсера
     * @return список транзакций для продюсера, или null если продюсер не найден
     */
    public List<TransactionDto> getTransactionsByProducer(String producerId) {
        log.debug("Получение транзакций для продюсера: {}", producerId);
        return transactionsByProducer.get(producerId);
    }

    /**
     * Добавляет новую транзакцию для указанного продюсера. Если продюсер еще не существует в
     * хранилище, создается новый список для него.
     *
     * @param producerId  идентификатор продюсера
     * @param transaction транзакция для добавления
     */
    public void addTransactionForProducer(String producerId, TransactionDto transaction) {
        log.debug("Добавление транзакции для продюсера: {}, транзакция: {}", producerId,
            transaction);
        transactionsByProducer.computeIfAbsent(producerId, k -> new ArrayList<>()).add(transaction);
    }

    /**
     * Возвращает набор всех идентификаторов продюсеров, имеющих транзакции в хранилище.
     *
     * @return множество идентификаторов продюсеров
     */
    public Set<String> getAllProducerIds() {
        log.debug("Получение всех идентификаторов продюсеров");
        return transactionsByProducer.keySet();
    }

    /**
     * Удаляет указанные транзакции для продюсера. Если все транзакции продюсера удалены, запись
     * продюсера удаляется из хранилища.
     *
     * @param producerId     идентификатор продюсера
     * @param transactionIds список идентификаторов транзакций для удаления
     * @return количество удаленных транзакций
     */
    public int removeTransactionsForProducer(String producerId, List<String> transactionIds) {
        log.debug("Удаление транзакций для продюсера: {}, количество транзакций для удаления: {}",
            producerId, transactionIds.size());

        List<TransactionDto> producerTransactions = transactionsByProducer.get(producerId);
        if (producerTransactions == null) {
            log.warn("Продюсер не найден: {}", producerId);
            return 0;
        }

        if (transactionIds.isEmpty()) {
            log.warn("Список транзакций для удаления пуст");
            return 0;
        }

        Set<String> transactionIdSet = new HashSet<>(transactionIds);

        synchronized (producerTransactions) {
            int initialSize = producerTransactions.size();

            boolean removed = producerTransactions.removeIf(transaction ->
                transactionIdSet.contains(transaction.getId())
            );

            if (removed && producerTransactions.isEmpty()) {
                transactionsByProducer.remove(producerId);
                log.debug("Все транзакции для продюсера удалены: {}", producerId);
            }

            int removedCount = initialSize - producerTransactions.size();
            log.debug("Удалено транзакций: {}", removedCount);
            return removedCount;
        }
    }

    /**
     * Проверяет, пусто ли хранилище транзакций.
     *
     * @return true если хранилище пусто, false если содержит транзакции
     */
    public boolean isEmpty() {
        boolean empty = transactionsByProducer.isEmpty();
        log.debug("Проверка, пусто ли хранилище: {}", empty);
        return empty;
    }
}
