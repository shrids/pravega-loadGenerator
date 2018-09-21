package io.pravega.loadGenerator.service.impl;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Payload which attaches a eventTime to the payload data
 */
@RequiredArgsConstructor
@Data
public class PayLoad implements Serializable {
    private final String senderId;
    private final long sequenceNumber;
    private final String routingKey; //TODO: meta
    private final Instant eventTime;
    private final byte[] constantdata = new byte[0]; //TODO: to change based on size of payload.
}
