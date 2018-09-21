package io.pravega.loadGenerator.service.impl.readers;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.loadGenerator.config.LoadGeneratorConfig;
import io.pravega.loadGenerator.service.LoadGeneratorService.TestState;
import io.pravega.loadGenerator.service.impl.PayLoad;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@Slf4j
public class ReaderManager implements AutoCloseable {
    private final ClientFactory clientFactory;
    private final List<Reader> readers;
    private final ScheduledExecutorService executor;
    private final TestState state;

    private final JavaSerializer<PayLoad> SERIALIZER = new JavaSerializer<>();
    private final ReaderConfig readerConfig = ReaderConfig.builder().build();

    public ReaderManager(final LoadGeneratorConfig.TestConfig config, final URI controllerURI, final String readerGroupName, final TestState state) {
        this.state = state;
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(config.getReaderCount() + 1, "ReaderPool");
        this.clientFactory = ClientFactory.withScope(config.getStream().getScope(), controllerURI);

        this.readers = IntStream.of(config.getReaderCount()).boxed().map(i -> {
            return Reader.builder()
                         .config(config)
                         .executorService(executor)
                         .readerId("Reader1-" + i)
                         .testState(this.state)
                         .pravegaReader(clientFactory.createReader("reader1-" + i, readerGroupName, SERIALIZER, readerConfig))
                         .build();
        }).collect(Collectors.toList());

    }

    public List<CompletableFuture<Void>> startReaders() {
        log.info("Starting all readers");
        return readers.stream().map(reader -> reader.start()).collect(Collectors.toList());
    }

    public void stopReaders() {
        this.state.stopReadFlag.set(true);
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(15));
        close();
    }

    @Override
    public void close() {
        readers.forEach(reader -> reader.close());
        clientFactory.close();
        ExecutorServiceHelpers.shutdown(executor);
    }
}
