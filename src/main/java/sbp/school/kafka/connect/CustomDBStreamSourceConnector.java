package sbp.school.kafka.connect;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import sbp.school.kafka.config.ApplicationConfig;

/**
 * Kafka Connect источник данных, который считывает данные из базы данных и передает их в Kafka.
 * Этот коннектор позволяет непрерывно передавать данные из базы данных в Kafka-топики, преобразуя
 * записи базы данных в сообщения Kafka.
 *
 * @author Alexander Dylevskiy
 * @since 1.0.0
 */
@Slf4j
public class CustomDBStreamSourceConnector extends SourceConnector {

    /**
     * Конфигурационная переменная для имени Kafka-топика, куда будут публиковаться данные.
     */
    public static final String TOPIC_CONFIG = ApplicationConfig.getAppProperties()
        .getProperty("kafka.topic.name");

    /**
     * Конфигурационная переменная для URL подключения к базе данных.
     */
    public static final String DATABASE_URL = ApplicationConfig.getAppProperties()
        .getProperty("h2.db.url");

    /**
     * Конфигурационная переменная для имени пользователя базы данных.
     */
    public static final String DATABASE_USERNAME = ApplicationConfig.getAppProperties()
        .getProperty("h2.db.username");

    /**
     * Конфигурационная переменная для пароля базы данных.
     */
    public static final String DATABASE_PASSWORD = ApplicationConfig.getAppProperties()
        .getProperty("h2.db.password");

    /**
     * Конфигурационная переменная для имени таблицы базы данных.
     */
    public static final String DATABASE_TABLE = ApplicationConfig.getAppProperties()
        .getProperty("h2.db.table.name");

    /**
     * Конфигурационная переменная для контроля размера пакета при чтении данных.
     */
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    /**
     * Значение по умолчанию для размера пакета записей.
     */
    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(DATABASE_URL, Type.STRING, null, Importance.HIGH, "URL подключения к базе данных")
        .define(DATABASE_USERNAME, Type.STRING, null, Importance.HIGH,
            "Имя пользователя базы данных")
        .define(DATABASE_PASSWORD, Type.STRING, null, Importance.HIGH, "Пароль базы данных")
        .define(DATABASE_TABLE, Type.STRING, null, Importance.HIGH, "Имя таблицы базы данных")
        .define(TOPIC_CONFIG, Type.STRING, NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
            Importance.HIGH, "Топик Kafka, куда будут публиковаться данные")
        .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
            "Максимальное количество записей, которые источник-задача может прочитать за один запрос");

    private Map<String, String> configProps;

    /**
     * Инициализирует коннектор с предоставленными свойствами конфигурации.
     *
     * @param props Карта, содержащая все необходимые свойства конфигурации
     */
    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configProps);
        String dbUrl = config.getString(DATABASE_URL);
        log.info("Запуск коннектора базы данных для чтения из {}", dbUrl);
    }

    /**
     * Возвращает класс реализации задачи, которая будет выполнять фактическое чтение данных.
     *
     * @return Класс, реализующий интерфейс Task
     */
    @Override
    public Class<? extends Task> taskClass() {
        return CustomDBStreamSourceTask.class;
    }

    /**
     * Создает конфигурационные карты для каждой экземпляра задачи на основе конфигурации
     * коннектора.
     *
     * @param maxTasks Максимальное количество задач, для которых нужно создать конфигурации
     * @return Список конфигурационных карт, по одной для каждой задачи
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }
        return taskConfigs;
    }

    /**
     * Останавливает работу коннектора и выполняет необходимые операции по очистке.
     */
    @Override
    public void stop() {
        log.info("Остановка коннектора базы данных");
    }

    /**
     * Возвращает определение конфигурации для этого коннектора.
     *
     * @return Объект ConfigDef, содержащий все свойства конфигурации
     */
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    /**
     * Возвращает версию реализации коннектора.
     *
     * @return Строка с номером версии
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}