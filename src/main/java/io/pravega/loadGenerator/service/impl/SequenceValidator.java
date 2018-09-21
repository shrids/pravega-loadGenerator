package io.pravega.loadGenerator.service.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * Pravega guarantees that events written to a particular key will be read in the same order.
 *
 * To validate this we add a SenderId and SequenceNumber (always increasing integer) to each payload.  When the payloads
 * are read this class is used to check the payload sequence number for the SenderId against the last received sequence number
 * to validate it's always increasing.
 *
 * The SenderId isn't just the key because we COULD have multiple Hulk tasks all sending to a particular key but with different
 * sequence numbers (since they aren't coordinated across the cluster).  To handle this, the SenderId is the key with the UUID
 * of the particular Hulk task that wrote the event.
 */
public class SequenceValidator {
    private final Map<String, Long> sequenceCounts = new HashMap<>();
    private final boolean strictIncremental;
    private boolean hasNotReset = true;
    public SequenceValidator() {
        this(false);
    }

    public SequenceValidator(boolean incremental) {
        this.strictIncremental = incremental;
    }

    public static String createSenderId(String key, String taskId) {
        return String.format("%s-%s", key, taskId);
    }

    public synchronized void validate(String senderId, Long sequenceNumber) {
        Long defaultPreviousNumber = hasNotReset ? 0 : sequenceNumber - 1;
        Long lastSequenceNumber = sequenceCounts.getOrDefault(senderId,  defaultPreviousNumber);

        if (sequenceNumber <= lastSequenceNumber || (strictIncremental && lastSequenceNumber + 1 != sequenceNumber)) {
            throw new IllegalStateException(String.format("Event out of sequence %s: lastSeq: %d, currentSeq: %d", senderId, lastSequenceNumber, sequenceNumber));
        }
        else {
            sequenceCounts.put(senderId, sequenceNumber);
        }
    }

    public synchronized void reset() {
        this.sequenceCounts.clear();
        this.hasNotReset = false;
    }
}
