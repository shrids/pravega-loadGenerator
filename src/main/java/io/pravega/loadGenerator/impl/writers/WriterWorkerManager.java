//package io.pravega.loadGenerator.impl.writers;
//
//import com.emc.nautilus.hulk.messagebus.ExecutorMessageClient;
//import com.emc.nautilus.hulk.messagebus.PravegaTaskParametersPayload;
//import com.emc.nautilus.hulk.model.PravegaTaskConfiguration;
//import com.emc.nautilus.hulk.taskexecutor.workers.TaskPerformanceCollector;
//import com.emc.nautilus.hulk.taskexecutor.workers.events.EventGenerator;
//import com.emc.nautilus.util.JacksonUtil;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import io.pravega.client.ClientConfig;
//import io.pravega.client.ClientFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.function.Consumer;
//
//import static com.emc.nautilus.hulk.utils.PravegaStreamHelper.adminCredentials;
//
///**
// * Manages a pool of Writer threads performing Write operations against a Pravega instance.
// */
//public class WriterWorkerManager {
//    private static final Logger LOG = LoggerFactory.getLogger(WriterWorkerManager.class);
//    private final ScheduledExecutorService scheduledExector;
//
//    private ClientFactory clientFactory;
//    private final ExecutorService executor;
//
//    private final ExecutorMessageClient messageClient;
//    private final Consumer finishedCallback;
//
//    private TaskPerformanceCollector performanceCollector;
//    private CountDownLatch finishedCountdown;
//
//    private List<PravegaWriter> writers = new ArrayList<>();
//    private boolean prepared = false;
//    private boolean killed;
//
//    private PravegaTaskParametersPayload taskParameters;
//    private PravegaTaskConfiguration taskConfiguration;
//
//    public WriterWorkerManager(PravegaTaskParametersPayload taskParameters,
//                               PravegaTaskConfiguration taskConfiguration,
//                               TaskPerformanceCollector performanceCollector,
//                               ExecutorMessageClient messageClient,
//                               Consumer<Boolean> finishedCallback) {
//        this.messageClient = messageClient;
//
//        this.taskParameters = taskParameters;
//        try {
//            LOG.info("Task Parameters: {}", JacksonUtil.createObjectMapper().writeValueAsString(taskParameters));
//        }
//        catch (JsonProcessingException e) {
//            LOG.info("Error'd: {}", taskParameters);
//        }
//        this.taskConfiguration = taskConfiguration;
//
//        this.performanceCollector = performanceCollector;
//        this.executor = Executors.newCachedThreadPool();
//        this.scheduledExector = Executors.newScheduledThreadPool(1);
//        this.finishedCallback = finishedCallback;
//    }
//
//    public void prepare() {
//        int numWriters = taskConfiguration.getNumWriters();
//        String scope = taskConfiguration.getScope();
//        String stream = taskConfiguration.getStream();
//
//        LOG.info("Preparing {} writers on stream {}/{}",numWriters, scope, stream);
//
//        clientFactory = ClientFactory.withScope(scope,
//                                                ClientConfig.builder()
//                                                            .credentials(adminCredentials())
//                                                            .controllerURI(URI.create(taskParameters.getControllerUri()))
//                                                            .build());
//        finishedCountdown = new CountDownLatch(numWriters);
//
//        EventGenerator eventGenerator = taskConfiguration.getPayload();
//        LOG.info("Setting up Writers");
//        for (int i = 0; i < numWriters; i++) {
//
//            PravegaWriterState stateHelper = new PravegaWriterState(this.scheduledExector, performanceCollector);
//
//            PravegaWriter newWriter = new PravegaWriter(stateHelper)
//                    .withClientFactory(clientFactory)
//                    .withStreamName(stream)
//                    .withWriterFinishedHandler(this::onWriterFinished)
//                    .withThrottleConfig(taskConfiguration.getThrottle())
//                    .withPerformanceCollector(performanceCollector)
//                    .withEventGenerator(eventGenerator);
//
//            if (taskConfiguration.getForever()) {
//                newWriter.withRunForever();
//            }
//            else {
//                newWriter.withSecondsToRun(taskConfiguration.getMinutes() * 60);
//            }
//
//            if (taskConfiguration.getTransactional()) {
//                stateHelper.withTransaction(taskConfiguration.getTransactionSize());
//            }
//
//            newWriter.prepare();
//            writers.add(newWriter);
//
//            LOG.info("Writer {} prepared",i);
//            performanceCollector.writerStarted();
//        }
//
//        prepared = true;
//    }
//
//    /**
//     * Called when the test moves from PREPARED -> RUNNING
//     */
//    public void start() {
//        if (!prepared) {
//            throw new IllegalStateException("has not been prepared");
//        }
//
//        LOG.info("Executing Writers");
//        int index = 0;
//        for (PravegaWriter writer : writers) {
//            executor.execute(() -> {
//                try {
//                    writer.start();
//                }
//                catch(Throwable shouldNotHappen) {
//                    LOG.error("Calling start on writer",shouldNotHappen);
//                    onWriterFinished(shouldNotHappen);
//                }
//            });
//            LOG.info("Writer {} started",index++);
//        }
//
//        executor.execute(() -> {
//            try {
//                LOG.info("Waiting for writers to finish");
//                finishedCountdown.await();
//            } catch (Throwable e) {
//                if (!killed) { // If we killed this then we assume there will be Interrupted Errors etc, so ignore
//                    LOG.error("Error waiting for writers to finish",e);
//                }
//            } finally {
//                finishedCallback.accept(false);
//            }
//        });
//    }
//
//    public void kill() {
//        killed = true;
//        executor.shutdownNow();
//        scheduledExector.shutdownNow();
//        LOG.info("Killed all writer tasks");
//    }
//
//    /**
//     * Called when the test enters the ABORT
//     */
//
//    public void abort() {
//        executor.shutdownNow();
//        scheduledExector.shutdownNow();
//    }
//
//    public void onWriterFinished(Throwable e) {
//        if (e != null) {
//            LOG.error("Writer finished with Error",e);
//            if (messageClient != null) {
//                messageClient.sendError(e);
//            }
//        }
//
//        performanceCollector.writerEnded();
//        finishedCountdown.countDown();
//
//        LOG.info("Writer finished, {} left running", finishedCountdown.getCount());
//    }
//}