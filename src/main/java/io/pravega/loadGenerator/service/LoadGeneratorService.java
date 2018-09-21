package io.pravega.loadGenerator.service;

import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.loadGenerator.config.LoadGeneratorConfig;
import io.pravega.loadGenerator.config.LoadGeneratorConfig.TestConfig;
import io.pravega.loadGenerator.service.impl.readers.ReaderManager;
import io.pravega.loadGenerator.service.impl.writers.WriterManager;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.synchronizedList;

@Slf4j
@RequiredArgsConstructor
public class LoadGeneratorService {
    private final LoadGeneratorConfig config;
    private final URI controllerURI;
    private final TestState state = new TestState(false);
    private WriterManager writerManager;
    private ReaderManager readerManager;

    public void prepare() {
        //1. create scope, stream
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        TestConfig testConfig = config.getTestConfig();
        final boolean scopeIsNew = streamManager.createScope(testConfig.getStream().getScope());

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                                              .build();
        final boolean streamIsNew = streamManager.createStream(testConfig.getStream().getScope(), testConfig.getStream().getStreamName(), testConfig.getStream());
        //2. create writerManager
        this.writerManager = new WriterManager(testConfig, controllerURI, state);

        // 3. Create ReaderGroup.
        //TODO: cleanup
        final ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(testConfig.getStream().getScope(),
                                                                                   ClientConfig.builder().controllerURI(controllerURI).build());
        final ReaderGroupConfig rgConfig = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                            .stream(Stream.of(testConfig.getStream().getScope(), testConfig.getStream().getStreamName()))
                                                            .build();

        String groupName = config.getTestConfig().getName() + "RG";
        readerGroupManager.createReaderGroup(groupName, rgConfig);

        //4. Create ReaderManager
        this.readerManager = new ReaderManager(testConfig, controllerURI, groupName, state);


    }

    public void start() {
        List<CompletableFuture<Void>> writerFutures = writerManager.startWriters();
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(5));
        writerManager.stopWriters(); // ... This can be invoked any time to stop the writers.

        List<CompletableFuture<Void>> readerFutures = readerManager.startReaders();
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(10));
        readerManager.stopReaders(); // ... This can be invoked any time to stop the readers.

        // Logic to check for failures can be added here.
        Futures.allOf(writerFutures); // wait until all the writers have stopped.
        Futures.allOf(readerFutures); // wait until all the readers have stopped.
    }

    public static class TestState {

        //read and write count variables
        public final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        public final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        public final AtomicReference<Throwable> getWriteException = new AtomicReference<>();
        public final AtomicReference<Throwable> getTxnWriteException = new AtomicReference<>();
        public final AtomicReference<Throwable> getReadException = new AtomicReference<>();
        //list of all writer's futures
        public final List<CompletableFuture<Void>> writers = synchronizedList(new ArrayList<>());
        //list of all reader's futures
        public final List<CompletableFuture<Void>> readers = synchronizedList(new ArrayList<>());
        public final List<CompletableFuture<Void>> writersListComplete = synchronizedList(new ArrayList<>());
        public final CompletableFuture<Void> writersComplete = new CompletableFuture<>();
        public final CompletableFuture<Void> newWritersComplete = new CompletableFuture<>();
        public final CompletableFuture<Void> readersComplete = new CompletableFuture<>();
        public final List<CompletableFuture<Void>> txnStatusFutureList = synchronizedList(new ArrayList<>());
        public final ConcurrentSet<UUID> committingTxn = new ConcurrentSet<>();
        public final ConcurrentSet<UUID> abortedTxn = new ConcurrentSet<>();
        public final boolean txnWrite;

        final AtomicLong writtenEvents = new AtomicLong();
        final AtomicLong readEvents = new AtomicLong();

        TestState(boolean txnWrite) {
            this.txnWrite = txnWrite;
        }

        public long incrementTotalWrittenEvents() {
            return writtenEvents.incrementAndGet();
        }

        long incrementTotalWrittenEvents(int increment) {
            return writtenEvents.addAndGet(increment);
        }

        public long incrementTotalReadEvents() {
            return readEvents.incrementAndGet();
        }

        public long getEventWrittenCount() {
            return writtenEvents.get();
        }

        public long getEventReadCount() {
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
}
