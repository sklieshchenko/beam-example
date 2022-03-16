package com.google;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

public class SampleReadingWriting {

    public interface Options extends DataflowPipelineOptions {
        @Description("The GCS location of the text you'd like to process")
        ValueProvider<String> getInputFilePattern();
        void setInputFilePattern(ValueProvider<String> value);

        @Description("The directory to output files to. Must end with a slash.")
        @Required
        ValueProvider<String> getOutputDirectory();
        void setOutputDirectory(ValueProvider<String> value);
    }

    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
                .apply("Write files to destination", TextIO.write().to(options.getOutputDirectory()));

        pipeline.run();
    }
}