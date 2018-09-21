package io.pravega.loadGenerator.service.impl.writers;

import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.loadGenerator.config.LoadGeneratorConfig;
import io.pravega.loadGenerator.config.LoadGeneratorConfig.TestConfig;
import io.pravega.loadGenerator.service.impl.PayLoad;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.synchronizedList;

@Data
@Slf4j
public class WriterManager implements AutoCloseable {

    private static final int WRITER_MAX_BACKOFF_MILLIS = 5 * 1000;
    private static final int WRITER_MAX_RETRY_ATTEMPTS = 20;
    private static final int NUM_EVENTS_PER_TRANSACTION = 50;
    private static final int TRANSACTION_TIMEOUT = 59 * 1000;

    private final List<String> routingKeys;
    private final ClientFactory clientFactory;
    private final List<Writer> writers;
    private final ScheduledExecutorService executor;
    private final TestState state;

    private final JavaSerializer<PayLoad> SERIALIZER = new JavaSerializer<>();
    private final EventWriterConfig writerConfig = EventWriterConfig.builder()
                                                      .maxBackoffMillis(WRITER_MAX_BACKOFF_MILLIS)
                                                      .retryAttempts(WRITER_MAX_RETRY_ATTEMPTS)
                                                      .transactionTimeoutTime(TRANSACTION_TIMEOUT)
                                                      .build();
    private final Function<Integer, List<String>> getKeyList = count -> IntStream.of(count).boxed()
                                                                                 .map(i -> UUID.randomUUID().toString())
                                                                                 .collect(Collectors.toList());


    public WriterManager(final TestConfig config, final URI controllerURI) {
       this.routingKeys =  getKeyList.apply(config.getWriterKeycount());
       this.state = new TestState(false);
       this.executor = ExecutorServiceHelpers.newScheduledThreadPool(config.getWriterCount() + 1, "WriterPool");
       this.clientFactory = ClientFactory.withScope(config.getStream().getScope(), controllerURI);

        this.writers = IntStream.of(config.getWriterCount()).boxed().map(i -> {
            return Writer.builder()
                         .writer(clientFactory.createEventWriter(config.getStream().getStreamName(), SERIALIZER, writerConfig))
                         .routingKeys(routingKeys)
                         .testState(this.state)
                         .writerId("Writer-" + i)
                         .executorService(executor)
                         .build();

        }).collect(Collectors.toList());

    }

    //TODO: split test state to writer state and reader state.
    static class TestState {

        //read and write count variables
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicReference<Throwable> getWriteException = new AtomicReference<>();
        final AtomicReference<Throwable> getTxnWriteException = new AtomicReference<>();
        final AtomicReference<Throwable> getReadException = new AtomicReference<>();
        //list of all writer's futures
        final List<CompletableFuture<Void>> writers = synchronizedList(new ArrayList<>());
        //list of all reader's futures
        final List<CompletableFuture<Void>> readers = synchronizedList(new ArrayList<>());
        final List<CompletableFuture<Void>> writersListComplete = synchronizedList(new ArrayList<>());
        final CompletableFuture<Void> writersComplete = new CompletableFuture<>();
        final CompletableFuture<Void> newWritersComplete = new CompletableFuture<>();
        final CompletableFuture<Void> readersComplete = new CompletableFuture<>();
        final List<CompletableFuture<Void>> txnStatusFutureList = synchronizedList(new ArrayList<>());
        final ConcurrentSet<UUID> committingTxn = new ConcurrentSet<>();
        final ConcurrentSet<UUID> abortedTxn = new ConcurrentSet<>();
        final boolean txnWrite;

        final AtomicLong writtenEvents = new AtomicLong();
        final AtomicLong readEvents = new AtomicLong();

        TestState(boolean txnWrite) {
            this.txnWrite = txnWrite;
        }

        long incrementTotalWrittenEvents() {
            return writtenEvents.incrementAndGet();
        }

        long incrementTotalWrittenEvents(int increment) {
            return writtenEvents.addAndGet(increment);
        }

        long incrementTotalReadEvents() {
            return readEvents.incrementAndGet();
        }

        long getEventWrittenCount() {
            return writtenEvents.get();
        }

        long getEventReadCount() {
            return readEvents.get();
        }

        void checkForAnomalies() {
            boolean failed = false;
            long eventReadCount = getEventReadCount();
            long eventWrittenCount = getEventWrittenCount();
            if (eventReadCount != eventWrittenCount) {
                failed = true;
                log.error("Read write count mismatch => readCount = {}, writeCount = {}", eventReadCount, eventWrittenCount);
            }

            if (committingTxn.size() > 0) {
                failed = true;
                log.error("Txn left committing: {}", committingTxn);
            }

            if (abortedTxn.size() > 0) {
                failed = true;
                log.error("Txn aborted: {}", abortedTxn);
            }
            throw new AssertionError("Test Failed");
        }

        public void cancelAllPendingWork() {
            synchronized (readers) {
                readers.forEach(future -> {
                    try {
                        future.cancel(true);
                    } catch (Exception e) {
                        log.error("exception thrown while cancelling reader thread", e);
                    }
                });
            }

            synchronized (writers) {
                writers.forEach(future -> {
                    try {
                        future.cancel(true);
                    } catch (Exception e) {
                        log.error("exception thrown while cancelling writer thread", e);
                    }
                });
            }
        }
    }

    @Override
    public void close() {
        writers.forEach(Writer::close);
        clientFactory.close();
        executor.shutdownNow();
    }

    public List<CompletableFuture> startWriters() {
        log.info("Starting all writers");
        return writers.stream().map(writer -> writer.start()).collect(Collectors.toList());
    }

    public void stopWriters() {
        this.state.stopWriteFlag.set(true);
        Exceptions.handleInterrupted(() ->TimeUnit.SECONDS.sleep(15));
        close();
    }


}
