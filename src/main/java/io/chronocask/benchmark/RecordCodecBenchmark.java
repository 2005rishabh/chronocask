package io.chronocask.benchmark;

import io.chronocask.record.Record;
import io.chronocask.record.RecordCodec;
import io.chronocask.record.RecordHeader;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark for binary serialization, deserialization, and CRC32C computation.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordCodecBenchmark {

    @Param({"32", "256", "4096"})
    private int valueSize;

    private Record record;
    private ByteBuffer encodedBuffer;

    @Setup(Level.Trial)
    public void setup() {
        byte[] key = "benchmark:entity:key:1001".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = (byte) (i & 0xFF);
        }

        record = Record.put(1001L, key, value);
        encodedBuffer = RecordCodec.encode(record);
    }

    @Benchmark
    public ByteBuffer benchmarkEncode() {
        return RecordCodec.encode(record);
    }

    @Benchmark
    public Record benchmarkDecode() {
        encodedBuffer.rewind();
        return RecordCodec.decode(encodedBuffer);
    }

    @Benchmark
    public RecordHeader benchmarkDecodeHeaderOnly() {
        encodedBuffer.rewind();
        return RecordCodec.decodeHeader(encodedBuffer);
    }
}
