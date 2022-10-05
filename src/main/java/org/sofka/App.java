package org.sofka;


import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class App
{
    public static void main( String[] args ) {

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setProject("low-code-dataflow");
        options.setRegion("us-central1");
        options.setStagingLocation("gs://low-code-dataflow-bucket/binaries/");
        options.setGcpTempLocation("gs://low-code-dataflow-bucket/temp/");
        options.setNetwork("default");
        options.setSubnetwork("regions/us-central1/subnetworks/default");
        options.setRunner(DataflowRunner.class);

        Pipeline p = Pipeline.create(options);

        p.apply("Read PubSub Messages", PubsubIO.readMessagesWithAttributesAndMessageId()
                        .fromTopic("projects/low-code-dataflow/topics/TOPIC_DATAFLOW_FILES"))
                .apply(new ReadPubSubMessages())
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Write Bucket Names", TextIO.write().to("gs://pubsub-dataflow-output/")
                        .withWindowedWrites().withNumShards(1));

        p.run().waitUntilFinish();
    }
}
