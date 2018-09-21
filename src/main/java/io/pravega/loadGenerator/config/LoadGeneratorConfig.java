package io.pravega.loadGenerator.config;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.pravega.client.stream.StreamConfiguration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Reader;
import java.util.List;

@AllArgsConstructor
@Data
@Builder
public class LoadGeneratorConfig {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DOTS).create();

    private final boolean defaultStreamCreate;
    private final TestConfig testConfig;

    @Builder
    @Data
    public static final class TestConfig {
        private final String name;
        private final StreamConfiguration stream;
        private final int writerCount; // writer count is per stream.
        private final int readerCount;
        // private final int durationInMins;

        //Writer configuration
        private final int writerMaxwritespersecond;
        private final int writerPayloadSizeinbytes;
        private final int writerKeycount;

        // Test assertions.
        private final Assertions assertions;
    }

    @Builder
    @Data
    public static final class Assertions {
        // types of validation
        private final boolean isRunning;
        private final boolean isInSequence;
    }

    public String toJson() {
        return gson.toJson(this);
    }

    public static LoadGeneratorConfig fromJson(final String jsonString) {
        return gson.fromJson(jsonString, new TypeToken<LoadGeneratorConfig>() {
        }.getType());
    }

    public static LoadGeneratorConfig fromJson(Reader reader) {
        return gson.fromJson(reader, new TypeToken<LoadGeneratorConfig>() {
        }.getType());
    }
}
