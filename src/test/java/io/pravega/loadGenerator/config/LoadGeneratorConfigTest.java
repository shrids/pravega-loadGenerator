package io.pravega.loadGenerator.config;


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

        TestConfig testConfig = TestConfig.builder()
                                          .name("basicLongevityTest")
                                          .readerCount(1)
                                          .writerCount(1)
                                          .writerMaxwritespersecond(2)
                                          .writerPayloadSizeinbytes(100)
                                          .writerKeycount(2)
                                          .assertions(Assertions.builder().isInSequence(true).isRunning(true).build())
                                          .build();
        LoadGeneratorConfig config = LoadGeneratorConfig.builder()
                                                        .defaultScope("s")
                                                        .defaultStreamCreate(true)
                                                        .testConfigs(Arrays.asList(testConfig))
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
        assertEquals("basicLongevityTest", config.getTestConfigs().get(0).getName());
    }

}