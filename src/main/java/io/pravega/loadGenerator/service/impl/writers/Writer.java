package io.pravega.loadGenerator.service.impl.writers;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.common.Exceptions;
import io.pravega.loadGenerator.service.impl.PayLoad;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Builder
public class Writer implements AutoCloseable {

    private final EventStreamWriter<PayLoad> writer;
    private final WriterManager.TestState testState;
    private final ExecutorService executorService;
    private final List<String> routingKeys;
    private final String writerId;

    public CompletableFuture<Void> start() {
        return startWriting();
    }

    private CompletableFuture<Void> startWriting() {
        return CompletableFuture.runAsync(() -> {
           ;
            long seqNumber = 0;
            while (!testState.stopWriteFlag.get()) {
                try {
                    String uniqueRoutingKey =  routingKeys.get(ThreadLocalRandom.current().nextInt(routingKeys.size()));
                    Exceptions.handleInterrupted(() -> Thread.sleep(100));

                    // The content of events is generated following the pattern routingKey:seq_number, where
                    // seq_number is monotonically increasing for every routing key, being the expected delta between
                    // consecutive seq_number values always 1.
                    PayLoad payload = new PayLoad(writerId, seqNumber, uniqueRoutingKey, Instant.now());
                    log.debug("Event write count before write call {}", testState.getEventWrittenCount());
                    writer.writeEvent(uniqueRoutingKey, payload);
                    log.debug("Event write count before flush {}", testState.getEventWrittenCount());
                    writer.flush();
                    testState.incrementTotalWrittenEvents();
                    log.debug("Writing event {}", payload);
                    log.debug("Event write count {}", testState.getEventWrittenCount());
                    seqNumber++;
                } catch (Throwable e) {
                    log.error("Test exception in writing events: ", e);
                    testState.getWriteException.set(e);
                }
            }
            log.info("Completed writing");
            closeWriter(writer);
        }, executorService);
    }

    private void closeWriter(EventStreamWriter<PayLoad> writer) {
        try {
            log.info("Closing writer");
            writer.close();
        } catch (Throwable e) {
            log.error("Error while closing writer", e);
            testState.getWriteException.compareAndSet(null, e);
        }
    }

    @Override
    public void close() {
        writer.close();
    }
}
