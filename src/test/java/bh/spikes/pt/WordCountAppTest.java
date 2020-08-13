package bh.spikes.pt;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class WordCountAppTest {
    TopologyTestDriver testDriver;

    TestInputTopic<String, String> inputTopic ;

    TestOutputTopic<String, Long> outputTopic ;

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic("word-count-input", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("word-count-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void makeSureCountsAreCorrect() {
        String firstExample = "testing Kafka Streams";
        inputTopic.pipeInput(firstExample);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("testing", 1L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 1L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("streams", 1L)));
        String secondExample = "testing Kafka again";
        inputTopic.pipeInput(secondExample);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("testing", 2L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 2L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("again", 1L)));
    }

    @Test
    public void makeSureWordsBecomeLowercase() {
        String upperCaseString = "KAFKA kafka Kafka";
        inputTopic.pipeInput(upperCaseString);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 1L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 2L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 3L)));

    }

}
