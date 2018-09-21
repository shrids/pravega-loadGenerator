package io.pravega.loadGenerator.config;


import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.loadGenerator.config.LoadGeneratorConfig.Assertions;
import io.pravega.loadGenerator.config.LoadGeneratorConfig.TestConfig;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@Slf4j
public class LoadGeneratorConfigTest {
    @Test
    public void testSerialize() {

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .streamName("basic")
                                                              .scope("basicScope")
                                                              .scalingPolicy(ScalingPolicy.fixed(2))
                                                              .build();
        TestConfig testConfig = TestConfig.builder()
                                          .name("basicLongevityTest")
                                          .stream(streamConfig)
                                          .readerCount(1)
                                          .writerCount(1)
                                          .writerMaxwritespersecond(2)
                                          .writerPayloadSizeinbytes(100)
                                          .writerKeycount(2)
                                          .assertions(Assertions.builder().isInSequence(true).isRunning(true).build())
                                          .build();
        LoadGeneratorConfig config = LoadGeneratorConfig.builder()
                                                        .defaultStreamCreate(true)
                                                        .testConfig(testConfig)
                                                        .build();
        String jsonString = config.toJson();
        log.info(jsonString);
        assertEquals(config, LoadGeneratorConfig.fromJson(jsonString));
    }

    @Test
    public void testSerializeFromFile() throws IOException, URISyntaxException {
        @Cleanup
        BufferedReader reader = Files.newBufferedReader(Paths.get(ClassLoader.getSystemResource("config.json").toURI()));
        LoadGeneratorConfig config = LoadGeneratorConfig.fromJson(reader);
        assertEquals("basicLongevityTest", config.getTestConfig().getName());
    }

}