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

    TestInputTopic<String, String> inputTopic;

    TestOutputTopic<String, Long> outputTopic;




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
    public void postalCodeSplit() {
        int postalCode = 2410245;
        int first = postalCode / 1000;
        int second = postalCode % 1000;
        String postalStr = String.format("%d-%d", first, second);
        System.out.println(postalStr);

    }

}
