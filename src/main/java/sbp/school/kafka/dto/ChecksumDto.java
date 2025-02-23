package sbp.school.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * DTO для передачи информации о контрольной сумме. Используется для передачи данных о контрольной
 * сумме и ключа интервала времени.
 *
 * @author Alexander Dylevskiy
 * @version 1.0
 * @since 1.0
 */
@AllArgsConstructor
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
}