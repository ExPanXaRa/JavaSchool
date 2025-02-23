package sbp.school.kafka.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import sbp.school.kafka.entity.Transaction;
import sbp.school.kafka.util.enums.TransactionOperationType;

/**
 * Задача источника данных, которая читает транзакции из базы данных и передает их в Kafka. Эта
 * задача отвечает за: - Подключение к базе данных - Чтение новых транзакций - Преобразование данных
 * в формат Kafka - Отслеживание прочитанных записей
 *
 * @author Alexander Dylevskiy
 * @since 1.0.0
 */
@Slf4j
public class CustomDBStreamSourceTask extends SourceTask {

    /**
     * Константа для поля имени базы данных в метаданных записи Kafka.
     */
    private static final String DATABASE_NAME_FIELD = "db_name";

    /**
     * Константа для поля позиции в метаданных записи Kafka.
     */
    private static final String POSITION_FIELD = "position";

    /**
     * URL для подключения к базе данных.
     */
    private String dbUrl;

    /**
     * Имя пользователя для подключения к базе данных.
     */
    private String dbUsername;

    /**
     * Пароль для подключения к базе данных.
     */
    private String dbPassword;

    /**
     * Имя таблицы в базе данных, из которой производится чтение данных.
     */
    private String tableName;

    /**
     * Имя Kafka-топика, куда будут публиковаться записи.
     */
    private String topic;

    /**
     * Размер пакета записей при чтении из базы данных.
     */
    private int batchSize;

    /**
     * Подключение к базе данных.
     */
    private Connection dbConnection;

    /**
     * Метка времени последней прочитанной записи. Используется для отслеживания прогресса чтения
     * данных.
     */
    private long lastModifiedOffset = 0;

    /**
     * Возвращает версию реализации задачи.
     *
     * @return Строка с номером версии
     */
    @Override
    public String version() {
        return new CustomDBStreamSourceConnector().version();
    }

    /**
     * Инициализирует задачу с предоставленными свойствами конфигурации. Создает подключение к базе
     * данных и проверяет его корректность.
     *
     * @param props Карта свойств конфигурации задачи
     */
    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CustomDBStreamSourceConnector.CONFIG_DEF, props);
        dbUrl = config.getString(CustomDBStreamSourceConnector.DATABASE_URL);
        dbUsername = config.getString(CustomDBStreamSourceConnector.DATABASE_USERNAME);
        dbPassword = config.getString(CustomDBStreamSourceConnector.DATABASE_PASSWORD);
        tableName = config.getString(CustomDBStreamSourceConnector.DATABASE_TABLE);
        topic = config.getString(CustomDBStreamSourceConnector.TOPIC_CONFIG);
        batchSize = config.getInt(CustomDBStreamSourceConnector.TASK_BATCH_SIZE_CONFIG);

        initializeDatabaseConnection();
    }

    /**
     * Устанавливает соединение с базой данных. Создает подключение с использованием заданных
     * учетных данных.
     */
    private void initializeDatabaseConnection() {
        try {
            dbConnection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
            log.info("Успешное подключение к базе данных.");
        } catch (SQLException ex) {
            log.error("Ошибка при подключении к базе данных: {}", ex.getMessage());
            throw new RuntimeException("Ошибка подключения к базе данных", ex);
        }
    }

    /**
     * Читает новые транзакции из базы данных. Использует последнюю прочитанную позицию для
     * получения только новых записей.
     *
     * @return Список записей для отправки в Kafka
     * @throws InterruptedException если задача прервана
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        Map<String, Object> offset = context.offsetStorageReader()
            .offset(Collections.singletonMap(DATABASE_NAME_FIELD, tableName));

        String query = "SELECT * FROM ? WHERE dateOfTransaction > ?";
        try (PreparedStatement statement = dbConnection.prepareStatement(query)) {
            statement.setString(1, tableName);
            statement.setTimestamp(2, new Timestamp(lastModifiedOffset));

            if (statement.execute()) {
                ResultSet resultSet = statement.getResultSet();
                while (resultSet.next()) {
                    Transaction transaction = mapResultSetToTransaction(resultSet);
                    updateLastModifiedOffset(transaction);

                    SourceRecord record = createSourceRecord(transaction, offset);
                    records.add(record);
                }
            }
        } catch (SQLException ex) {
            log.error("Ошибка выполнения SQL-запроса: {}", ex.getMessage());
            throw new RuntimeException("Ошибка выполнения SQL-запроса", ex);
        }

        return records;
    }

    /**
     * Преобразует результаты SQL-запроса в объект транзакции.
     *
     * @param resultSet Результаты SQL-запроса
     * @return Объект транзакции
     * @throws SQLException если возникла ошибка при чтении данных
     */
    private Transaction mapResultSetToTransaction(ResultSet resultSet) throws SQLException {
        Transaction transaction = new Transaction();
        transaction.setId(resultSet.getLong("id"));
        transaction.setAmount(resultSet.getBigDecimal("sum"));
        transaction.setOperationType(
            TransactionOperationType.valueOf(resultSet.getString("operationType")));
        transaction.setDate(
            OffsetDateTime.parse(resultSet.getTimestamp("dateOfTransaction").toString()));
        return transaction;
    }

    /**
     * Обновляет метку времени последней прочитанной транзакции.
     *
     * @param transaction Текущая транзакция
     */
    private void updateLastModifiedOffset(Transaction transaction) {
        long lastModified = transaction.getDate().toInstant().toEpochMilli();
        if (lastModified > lastModifiedOffset) {
            lastModifiedOffset = lastModified;
        }
    }

    /**
     * Создает запись Kafka из объекта транзакции.
     *
     * @param transaction Транзакция для преобразования
     * @param offset      Текущая позиция чтения
     * @return Запись Kafka
     */
    private SourceRecord createSourceRecord(Transaction transaction, Map<String, Object> offset) {
        return new SourceRecord(
            Collections.singletonMap(DATABASE_NAME_FIELD, tableName),
            Collections.singletonMap(POSITION_FIELD, lastModifiedOffset),
            topic,
            null,
            getTransactionSchema(),
            getTransactionStruct(transaction)
        );
    }

    /**
     * Создает схему данных для транзакции.
     *
     * @return Схема данных в формате Kafka Connect
     */
    private Schema getTransactionSchema() {
        return SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("operationType", Schema.STRING_SCHEMA)
            .field("sum", Schema.STRING_SCHEMA)
            .field("dateOfTransaction", Schema.STRING_SCHEMA)
            .build();
    }

    /**
     * Преобразует объект транзакции в структуру данных Kafka.
     *
     * @param transaction Транзакция для преобразования
     * @return Структура данных в формате Kafka Connect
     */
    private Struct getTransactionStruct(Transaction transaction) {
        return new Struct(getTransactionSchema())
            .put("id", transaction.getId())
            .put("operationType", transaction.getOperationType().toString())
            .put("sum", transaction.getAmount().toString())
            .put("dateOfTransaction", transaction.getDate().toString());
    }

    /**
     * Останавливает задачу и закрывает все ресурсы. Закрывает подключение к базе данных.
     */
    @Override
    public void stop() {
        if (dbConnection != null) {
            try {
                dbConnection.close();
                log.info("Подключение к базе данных закрыто.");
            } catch (SQLException ex) {
                log.error("Ошибка при закрытии подключения к базе данных: {}", ex.getMessage());
            }
        }
    }
}