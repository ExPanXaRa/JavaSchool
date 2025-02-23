package sbp.school.kafka.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class ChecksumHelper {

    public static long getIntervalKey(OffsetDateTime time, Duration interval) {
        long timeMilliseconds = time.toInstant().toEpochMilli();
        long intervalMilliseconds = interval.toMillis();
        return timeMilliseconds / intervalMilliseconds;
    }

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
}
