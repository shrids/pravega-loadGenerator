package io.pravega.loadGenerator.service.impl.readers;

import io.pravega.client.stream.EventStreamReader;
import io.pravega.loadGenerator.config.LoadGeneratorConfig.TestConfig;
import io.pravega.loadGenerator.service.LoadGeneratorService.TestState;
import io.pravega.loadGenerator.service.impl.PayLoad;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Builder
public class Reader implements AutoCloseable {
    private final EventStreamReader<PayLoad> pravegaReader;
    private final TestState testState;
    private final ExecutorService executorService;
    private final String readerId;
    private final TestConfig config;

    public CompletableFuture<Void> start() {
        return startReading();
    }

    private CompletableFuture<Void> startReading() {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status: {}, Read count: {}, Write count: {}", testState.stopReadFlag.get(),
                     testState.getEventReadCount(), testState.getEventWrittenCount());
            final Map<String, Long> writerIDWithKeyToSeqNumber = new HashMap<>();
            while (!(testState.stopReadFlag.get() && testState.getEventReadCount() == testState.getEventWrittenCount())) {
                log.info("Entering read loop");
                // Exit only if exitFlag is true  and read Count equals write count.
                try {
                    final PayLoad event = pravegaReader.readNextEvent(SECONDS.toMillis(5)).getEvent();
                    log.debug("Reading event {}", event);
                    if (event != null) {
                        if (config.getAssertions().isInSequence()) {  // Assert based on test configuration.
                            verifyEventOrder(writerIDWithKeyToSeqNumber, event);
                        }
                        testState.incrementTotalReadEvents();
                        log.debug("Event read count {}", testState.getEventReadCount());
                    } else {
                        log.debug("Read timeout");
                    }
                } catch (Throwable e) {
                    log.error("Test exception in reading events: ", e);
                    testState.getReadException.set(e);
                }
            }
            log.info("Completed reading");
            closeReader(pravegaReader);
        }, executorService);
    }

    // verify event order and duplicates.
    private void verifyEventOrder(final Map<String, Long> writerIDWithKeyToSeqNumber, final PayLoad eventRead) {
        long seqNumber = eventRead.getSequenceNumber();
        writerIDWithKeyToSeqNumber.compute(eventRead.getSenderId() + eventRead.getRoutingKey(), (writerIDWithRk, currentSeqNum) -> {
            if (currentSeqNum != null && currentSeqNum >= seqNumber) {
                log.error("Events not in sequence. LastSequenceNumber: {}, current event: {} ", currentSeqNum, eventRead);
                throw new AssertionError("Event order violated at " + currentSeqNum + " by " + seqNumber);
            }
            return seqNumber;
        });
    }

    private void closeReader(EventStreamReader<PayLoad> reader) {
        try {
            log.info("Closing pravegaReader");
            reader.close();
        } catch (Throwable e) {
            log.error("Error while closing pravegaReader", e);
            testState.getReadException.compareAndSet(null, e);
        }
    }

    @Override
    public void close() {
        closeReader(pravegaReader);
    }
}
