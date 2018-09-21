package io.pravega.loadGenerator.service;

import io.pravega.common.Exceptions;
import io.pravega.loadGenerator.config.LoadGeneratorConfig;
import io.pravega.loadGenerator.service.impl.writers.WriterManager;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class LoadGeneratorService {
    private final WriterManager writerManager;
    public LoadGeneratorService(LoadGeneratorConfig config, URI controllerURI)  {
        this.writerManager = new WriterManager(config.getTestConfig(), controllerURI);
    }

    public void start() {
        List<CompletableFuture> result = writerManager.startWriters();
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(30));
        writerManager.stopWriters(); //TODO: testing
    }
}
