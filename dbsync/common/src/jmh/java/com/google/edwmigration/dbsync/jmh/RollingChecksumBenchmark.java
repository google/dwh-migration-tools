package com.google.edwmigration.dbsync.jmh;

import com.google.edwmigration.dbsync.common.RollingChecksumImpl;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(value=2)
@Warmup(iterations=2, time=RollingChecksumBenchmark.MS, timeUnit = TimeUnit.MILLISECONDS )
@Measurement(iterations=4, time=RollingChecksumBenchmark.MS, timeUnit = TimeUnit.MILLISECONDS)
public class RollingChecksumBenchmark {
  public static final int MS = 1000;
  private static final int N = 1024;

  private byte[] bytes = new byte[65536];
  private InputStream stream;
  private RollingChecksumImpl impl = new RollingChecksumImpl(4096);

  @Setup(Level.Iteration)
  public void setUp() throws Exception {
    ThreadLocalRandom.current().nextBytes(bytes);
    stream = new ByteArrayInputStream(bytes);
    stream.mark(bytes.length);
  }

  @Benchmark
  public void testResetBytes(Blackhole bh) throws Exception{
    for (int i = 0; i < N; i++)
      impl.reset(bytes, 0);
    bh.consume(impl);
  }

  @Benchmark
  public void testResetStream(Blackhole bh) throws Exception{
    for (int i = 0; i < N; i++) {
      stream.reset();
      impl.reset(stream);
    }
    bh.consume(impl);
  }
}
