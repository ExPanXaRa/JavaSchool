package sbp.school.kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

/**
 * DTO для представления контрольной суммы и ключа интервала времени. Используется для передачи
 * данных о контрольных суммах между компонентами системы.
 *
 * @author [Ваше имя]
 * @version 1.0
 * @since 1.0
 */
@Getter
@ToString
public final class ChecksumDto {

    /**
     * Ключ интервала времени, для которого вычислена контрольная сумма.
     */
    private final String intervalKey;

    /**
     * Значение контрольной суммы.
     */
    private final String checksum;

    /**
     * Создает новый экземпляр ChecksumDto с указанным ключом интервала и контрольной суммой.
     *
     * @param intervalKey ключ интервала времени
     * @param checksum    значение контрольной суммы
     */
    @JsonCreator
    public ChecksumDto(@JsonProperty("intervalKey") String intervalKey,
        @JsonProperty("checksum") String checksum) {
        this.intervalKey = intervalKey;
        this.checksum = checksum;
    }
}