package io.pravega.loadGenerator.service;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.loadGenerator.config.LoadGeneratorConfig;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class LoadGeneratorServiceTest {

        @Test
    public void start() throws URISyntaxException {

        LoadGeneratorService service = new LoadGeneratorService(getConfig(), new URI("tcp://127.0.0.1:9090"));
        service.prepare();
        service.start();
    }

    private LoadGeneratorConfig getConfig() {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .streamName("basic")
                                                              .scope("basicScope")
                                                              .scalingPolicy(ScalingPolicy.fixed(2))
                                                              .build();
        LoadGeneratorConfig.TestConfig testConfig = LoadGeneratorConfig.TestConfig.builder()
                                                                                  .name("basicLongevityTest")
                                                                                  .stream(streamConfig)
                                                                                  .readerCount(1)
                                                                                  .writerCount(1)
                                                                                  .writerMaxwritespersecond(2)
                                                                                  .writerPayloadSizeinbytes(100)
                                                                                  .writerKeycount(2)
                                                                                  .assertions(LoadGeneratorConfig.Assertions.builder().isInSequence(true).isRunning(true).build())
                                                                                  .build();
        LoadGeneratorConfig config = LoadGeneratorConfig.builder()
                                                        .defaultStreamCreate(true)
                                                        .testConfig(testConfig)
                                                        .build();
        return config;
    }
}