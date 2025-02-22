package sbp.school.kafka.util;

import java.time.Duration;
import java.time.OffsetDateTime;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class ChecksumHelper {

    public static long getIntervalKey(OffsetDateTime time, Duration interval) {
        long timeMilliseconds = time.toInstant().toEpochMilli();
        long intervalMilliseconds = interval.toMillis();
        return timeMilliseconds / intervalMilliseconds;
    }
}
