package io.jhdf.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(time = 3, batchSize = 2, iterations = 2)
@Fork(2)
public class MemoryMapBenchmark {

	private Path tempFile;

	@Param({
//		"32",
//		"64",
//		"128",
//		"1024", // 1k
//		"10240", // 10k
//		"1048576", // 1m
//		"5242880", // 5m
		"10485760" // 10m
	})
	public int readSize;

	@Setup(Level.Trial)
	public void setUp() throws IOException {
		Random random = new Random();
		tempFile = Files.createTempFile("randomData", ".bin");
		System.out.println("Temp file: " + tempFile.toAbsolutePath());
		byte[] bytes = new byte[102400];
		random.nextBytes(bytes);
		Files.write(tempFile, bytes);
	}

	@TearDown(Level.Trial)
	public  void tearDown() throws IOException {
		Files.delete(tempFile);
	}

	@Benchmark
//	@BenchmarkMode(Mode.SampleTime)
//	@Warmup(time = 5, iterations = 2)
//	@Measurement(time = 5, batchSize = 5, timeUnit = TimeUnit.MICROSECONDS)
	public void plainRead(Blackhole blackhole) throws IOException {
		try (FileInputStream fileInputStream = new FileInputStream(tempFile.toFile())) {
			byte[] bytes = new byte[readSize];
			fileInputStream.read(bytes);
			blackhole.consume(bytes);
		}
	}

	@Benchmark
//	@BenchmarkMode(Mode.SampleTime)
//	@Warmup(time = 5, iterations = 2)
//	@Measurement(time = 5, batchSize = 5, timeUnit = TimeUnit.MICROSECONDS)
	public void memoryMappedRead(Blackhole blackhole) throws IOException {
		try (FileChannel fileInputStream = FileChannel.open(tempFile, StandardOpenOption.READ)) {
			MappedByteBuffer map = fileInputStream.map(FileChannel.MapMode.READ_ONLY, 0, readSize);
			byte[] bytes = new byte[readSize];
			map.get(bytes);
			blackhole.consume(bytes);
		}
	}


}
