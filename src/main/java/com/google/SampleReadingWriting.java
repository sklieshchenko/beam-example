package com.google;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class SampleReadingWriting {

    public static void main(String[] args) {
        SampleReadingWriting sp = new SampleReadingWriting();
        sp.run();
    }

    public void run() {
        PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        processData(pipeline);
        pipeline.run();
    }

    public void processData(Pipeline pipeline) {
        pipeline.apply("ReadMyFile", TextIO.read().from("src/main/resources/avocado.csv"))
                .apply(TextIO.write().to("src/main/resources/output.txt"));
    }

}
