package sbp.school.kafka.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Класс для управления конфигурацией приложения через файл свойств. Предоставляет статический метод
 * для загрузки настроек из файла конфигурации.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since [Дата создания]
 */
@Slf4j
public class ApplicationConfig {

    /**
     * Путь к файлу конфигурации относительно корня проекта.
     */
    private static final String PATH = "src/main/resources/app.properties";

    /**
     * Загружает свойства приложения из файла конфигурации.
     *
     * @return Объект Properties, содержащий все загруженные параметры конфигурации
     * @throws RuntimeException если возникает ошибка при чтении файла конфигурации
     */
    public static Properties getAppProperties() {
        Properties props = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(PATH))) {
            props.load(input);
            log.info("Конфигурация успешно загружена");
            return props;
        } catch (IOException e) {
            log.error("Ошибка во время загрузки конфигурации", e);
            throw new RuntimeException(e.getMessage());
        }
    }
}