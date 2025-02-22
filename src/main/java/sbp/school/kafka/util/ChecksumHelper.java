package sbp.school.kafka.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.experimental.UtilityClass;

/**
 * Класс утилит для работы с контрольными суммами и временными интервалами. Предоставляет методы для
 * расчёта MD5-хешей и преобразования временных меток в ключи интервалов.
 *
 * @since 1.0
 */
@UtilityClass
public final class ChecksumHelper {

    /**
     * Вычисляет MD5-контрольную сумму списка идентификаторов транзакций.
     *
     * @param transactionIds список идентификаторов транзакций для хеширования
     * @return строковое представление MD5-хеша в шестнадцатеричном формате
     * @throws RuntimeException если возникает ошибка при создании экземпляра MessageDigest
     */
    public static String calculateChecksum(List<String> transactionIds) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (String id : transactionIds) {
                md.update(id.getBytes());
            }
            byte[] digest = md.digest();
            return new BigInteger(1, digest).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Ошибка вычисления контрольной суммы", e);
        }
    }

    /**
     * Преобразует временную метку в числовой ключ на основе заданного интервала времени.
     *
     * @param time     момент времени для преобразования
     * @param interval размер интервала времени для группировки
     * @return целочисленный ключ, представляющий номер интервала времени
     */
    public static long getIntervalKey(OffsetDateTime time, Duration interval) {
        long timeMilliseconds = time.toInstant().toEpochMilli();
        long intervalMilliseconds = interval.toMillis();
        return timeMilliseconds / intervalMilliseconds;
    }
}