package io.pravega.loadGenerator;

import io.pravega.loadGenerator.config.LoadGeneratorConfig;
import io.pravega.loadGenerator.service.LoadGeneratorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Main class
 */
@Slf4j
public class Main {
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";

    public static void main(String[] args) throws Exception {
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("pravega-loadGenerator", options);
            System.exit(1);
        }

        String jsonConfigFile = cmd.getOptionValue("jsonConfigFile");
        String controllerUri = cmd.getOptionValue("uri", DEFAULT_CONTROLLER_URI);
        log.debug("Option: {}, {}", jsonConfigFile, controllerUri);

        @Cleanup
        BufferedReader reader = Files.newBufferedReader(Paths.get(jsonConfigFile));
        LoadGeneratorConfig config = LoadGeneratorConfig.fromJson(reader);
        log.info("Start LoadGenerator for config :{}", config.toJson());

        LoadGeneratorService service = new LoadGeneratorService(config, new URI(controllerUri));
        service.start();
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addRequiredOption("f", "jsonConfigFile", true, "Test Configuration file");
        options.addOption("u", "uri", true, "The URI to the controller in the form tcp://host:port");
        return options;
    }
}
