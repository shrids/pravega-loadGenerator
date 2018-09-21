package io.pravega.loadGenerator.service.impl.writers;

import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.loadGenerator.config.LoadGeneratorConfig.TestConfig;
import io.pravega.loadGenerator.service.LoadGeneratorService;
import io.pravega.loadGenerator.service.LoadGeneratorService.TestState;
import io.pravega.loadGenerator.service.impl.PayLoad;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

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


    public WriterManager(final TestConfig config, final URI controllerURI, final TestState state) {
        this.routingKeys = getKeyList.apply(config.getWriterKeycount());
        this.state = state;
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

    public List<CompletableFuture<Void>> startWriters() {
        log.info("Starting all writers");
        return writers.stream().map(writer -> writer.start()).collect(Collectors.toList());
    }

    public void stopWriters() {
        this.state.stopWriteFlag.set(true);
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(15));
        close();
    }


    @Override
    public void close() {
        writers.forEach(Writer::close);
        clientFactory.close();
        ExecutorServiceHelpers.shutdown(executor);
    }



}
