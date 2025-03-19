/*
 * This file is part of jHDF. A pure Java library for accessing HDF5 files.
 *
 * https://jhdf.io
 *
 * Copyright (c) 2025 James Mudd
 *
 * MIT License see 'LICENSE' file
 */
package io.jhdf.writing;

import io.jhdf.HdfFile;
import io.jhdf.StreamableDatasetImpl;
import io.jhdf.WritableHdfFile;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

class WritableSegmentedDatasetImplTest {

  private static final Logger log = LoggerFactory.getLogger(WritableSegmentedDatasetImplTest.class);

  @Test
  void testLargeDataset() {

    System.out.println("memory: " + Runtime.getRuntime().maxMemory());
    Path hdf5Out;
    try {
      hdf5Out = Files.createTempFile(
          Path.of("."),
          this.getClass().getSimpleName() + "_BiggerThan2GbB_dataset",
          ".hdf5"
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<Integer> chunkIdx = List.of(0, 1, 2, 3);

    int chunkRows = (1024 * 1024) / Long.BYTES;
    int rowsize = 1024;
    Supplier sf =
        new SupplierFunc<Integer, long[][]>(chunkIdx, i -> getArrayData(i, rowsize, chunkRows));

    try (WritableHdfFile out = HdfFile.write(hdf5Out)) {
      StreamableDatasetImpl sd =
          new StreamableDatasetImpl(sf, chunkRows * chunkIdx.size(), "", out);
      out.putWritableDataset("testname", sd);
    }
  }

  private long[][] getArrayData(long offset, int rowsize, int rows) {
    //    int rowbytes = Long.BYTES * rowsize;
    //    int rows = (int) (minBytes / rowbytes);
    long[][] data = new long[rows][rowsize];
    for (int i = 0; i < data.length; i++) {
      long[] row = new long[rowsize];
      Arrays.fill(row, offset + i);
      data[i] = row;
    }
    return data;
  }

  public static final class SupplierFunc<I, O> implements Supplier<O> {

    private final List<I> list;
    private final Iterator<I> iter;
    private final Function<I, O> f;

    SupplierFunc(List<I> in, Function<I, O> f) {
      this.list = in;
      this.iter = list.iterator();
      this.f = f;
    }

    @Override
    public O get() {
      if (iter.hasNext()) {
        I next = iter.next();
        System.out.println("returning projected test data for " + next);
        return f.apply(next);
      } else {
        return null;
      }
    }
  }
}
