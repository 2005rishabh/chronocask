package io.chronocask.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Runner main class for executing ChronoCask JMH benchmarks from CLI.
 */
public class BenchmarkRunner {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include("io.chronocask.benchmark.*")
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
