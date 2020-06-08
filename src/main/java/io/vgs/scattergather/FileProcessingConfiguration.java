package io.vgs.scattergather;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.aggregator.AggregatingMessageHandler;
import org.springframework.integration.aggregator.DefaultAggregatingMessageGroupProcessor;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.splitter.FileSplitter;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageChannel;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Configuration
@EnableIntegration
public class FileProcessingConfiguration {

  public final String INPUT_DIR = "/Users/vgs/Desktop/lol";
  public final String FILE_PATTERN = "*.csv";

  @Bean
  QueueChannel aggregateFlowChannel() {
    return new QueueChannel();
  }

  @Bean
  QueueChannel processFlowChannel() {
    return new QueueChannel();
  }

  @Bean
  QueueChannel logChan() {
    return new QueueChannel();
  }

  @Bean
  public MessageChannel fileChannel() {
    return new DirectChannel();
  }

  @Bean
  public FileSplitter getFileSplitter() {
    return new FileSplitter();
  }

  @Bean
  public AggregatingMessageHandler aggregator() {
    AggregatingMessageHandler aggregator =
        new AggregatingMessageHandler(new DefaultAggregatingMessageGroupProcessor());
    aggregator.setGroupTimeoutExpression(new ValueExpression<>(500L));
    return aggregator;
  }

  @Bean
  public FileToStringTransformer fileToStringTransformer() {
    return new FileToStringTransformer();
  }

  @Bean(name = PollerMetadata.DEFAULT_POLLER)
  public PollerMetadata poller() {
    return Pollers.fixedRate(500).get();
  }

  @Bean
  public PollerMetadata asyncPoller(TaskExecutor taskExecutor) {

    return Pollers.fixedRate(500)
        .taskExecutor(taskExecutor)
        .get();
  }

  @Bean
  @InboundChannelAdapter(value = "fileChannel", poller = @Poller(fixedDelay = "10000"))
  public MessageSource<File> fileReadingMessageSource() {
    FileReadingMessageSource sourceReader = new FileReadingMessageSource();
    sourceReader.setDirectory(new File(INPUT_DIR));
    sourceReader.setFilter(new SimplePatternFileListFilter(FILE_PATTERN));
    return sourceReader;
  }

  @Bean
  public ExecutorChannel executorChannel() {
    return new ExecutorChannel(Executors.newCachedThreadPool());
  }

  @Bean
  public IntegrationFlow splitFileFlow() {
    return IntegrationFlows.from("fileChannel")
        .log()
        .split(getFileSplitter())
        .channel("processFlowChannel")
        .get();
  }

  @Bean
  public IntegrationFlow processFileFlow(PollerMetadata asyncPoller) {
    return IntegrationFlows.from("processFlowChannel")
        .<String>handle((p, h) -> p.toUpperCase(), e -> e.poller(asyncPoller))
        .log()
        .channel("aggregateFlowChannel")
        .get();
  }

  @Bean
  public IntegrationFlow aggregateFileFlow(AggregatingMessageHandler aggregator) {
    return IntegrationFlows.from("aggregateFlowChannel")
        .aggregate(a -> a.outputProcessor(g -> g.getMessages()
            .stream()
            .map(m -> (String) m.getPayload())
            .collect(Collectors.joining("\n")))
            .groupTimeout(1000L)
            .sendPartialResultOnExpiry(true)
        )
        .channel("logChan")
        .get();
  }

  @Bean
  public IntegrationFlow logChanFlow() {
    return IntegrationFlows.from("logChan")
        .log()
        .get();
  }
}
