package gcp.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class Dataflow1 {

  interface TestOptions extends PipelineOptions{

    @Description("The Cloud Pub/Sub topic to read from.")
    @Validation.Required
    ValueProvider<String> getInputTopic();
    void setInputTopic(ValueProvider<String> value);

    @Description("Output file's window size in number of minutes.")
    @Default.Integer(1)
    Integer getWindowSize();
    void setWindowSize(Integer value);

    @Description("Path of the output file including its filename prefix.")
    @Validation.Required
    ValueProvider<String> getOutputLocation();
    void setOutput(ValueProvider<String> value);

  }

  public static void main(String[] args) {

    int numShards = 1;

    TestOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(TestOptions.class);


//    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
           .apply("to gcs", TextIO.write().to(options.getOutputLocation()).withNumShards(options.getWindowSize()));

    pipeline.run();

  }


}
