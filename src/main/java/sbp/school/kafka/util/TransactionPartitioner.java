package sbp.school.kafka.util;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.util.enums.TransactionOperationType;

/**
 * Реализация Partitioner для распределения транзакционных данных по партициям Kafka.
 * Каждая партиция соответствует определенному типу операции (TransactionOperationType).
 * Это обеспечивает группировку однотипных операций в одной партиции.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class TransactionPartitioner implements Partitioner {

    /**
     * Определяет номер партиции для записи сообщения в топик.
     * Партиция выбирается на основе типа операции в TransactionDto.
     * Количество партиций должно точно соответствовать количеству типов операций.
     *
     * @param topic        имя топика
     * @param key         ключ сообщения (не используется)
     * @param keyBytes    сериализованный ключ (не используется)
     * @param value       значение сообщения (TransactionDto)
     * @param valueBytes  сериализованное значение (не используется)
     * @param cluster     метаданные кластера
     * @return номер партиции (ordinal типа операции)
     * @throws RuntimeException если количество партиций не соответствует количеству типов операций
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
        Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionSize = partitionInfos.size();

        if (TransactionOperationType.values().length != partitionSize) {
            log.error("Количество типов операций и количество партиций не совпадают");
            throw new RuntimeException("Количество типов операций и количество партиций не совпадают");
        }

        return ((TransactionDto) value).getOperationType().ordinal();
    }

    /**
     * Метод конфигурации партиционера.
     * В данной реализации не требует дополнительной конфигурации.
     *
     * @param configs конфигурационные параметры (не используются)
     */
    @Override
    public void configure(Map<String, ?> configs) {
    }

    /**
     * Метод закрытия партиционера.
     * Выполняется при завершении работы продюсера.
     * В данной реализации не требует освобождения ресурсов.
     */
    @Override
    public void close() {
    }
}