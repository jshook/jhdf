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
import io.jhdf.SegmentedDatasetBuilder;
import io.jhdf.WritableHdfFile;
import io.jhdf.WritableSegmentedDataset;
import io.jhdf.api.WritableDataset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WritableSegmentedDatasetImplTest {

  @Test
  void testLargeDataset() {

    Path hdf5Out;
    try {
      hdf5Out = Files.createTempFile(Path.of("."),this.getClass().getSimpleName()+
                                     "_BiggerThan2GbB_dataset", ".hdf5");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try (WritableHdfFile out = HdfFile.write(hdf5Out)) {
      SegmentedDatasetBuilder b= new SegmentedDatasetBuilder("testname", out);
      long offset=0;
      for (int i = 0; i < 3; i++) {
        long[][] arryData = getArrayData(offset, 1024, (long) ((double) (Integer.MAX_VALUE)));
        offset+=arryData.length;
        System.out.println("arrydata dims:" + arryData.length + ":" + arryData[0].length);

        b.putChunk(arryData);
      }
      out.putSegmentedDataset("testname",b.build());
//      WritableSegmentedDataset ds = new WritableSegmentedDataset("testname", out);

//      for (int i = 0; i < 3; i++) {
//        out.putSegmentedDataset("2.1GB-MaxSize", ds);
//      }
//      WritableDataset wds = ds.flush();
    }

  }

  //    MatcherAssert.assertThat(dataset.getMaxSize().length, is(equalTo(1)));
  //    MatcherAssert.assertThat(dataset.getMaxSize()[0], is(equalTo(100000000000L)));

  //		assertThat(writableDataset.getSize()).isEqualTo(3);
  //		assertThat(writableDataset.getSizeInBytes()).isEqualTo(3 * 4);
  //		assertThat(writableDataset.getStorageInBytes()).isEqualTo(3 * 4);


  private long[][] getArrayData(long offset, int rowsize, long minBytes) {
    int rowbytes = Long.BYTES * rowsize;
    int rows = (int) (minBytes / rowbytes);
    long[][] data = new long[rows][rowsize];
    for (int i = 0; i < data.length; i++) {
      long[] row = new long[rowsize];
      Arrays.fill(row,offset+i);
      data[i]=row;
    }
    return data;
  }
  //
  //	@Test
  //	void testGettingData() {
  //		int[][] data = new int[][] {{1,2,3}, {4,5,6}};
  //		WritableDatasetImpl writableDataset = new WritableDatasetImpl(data, "ints", null);
  //		assertThat(writableDataset.getData()).isEqualTo(new int[][] {{1,2,3}, {4,5,6}});
  //		assertThat(writableDataset.getDataFlat()).isEqualTo(ArrayUtils.toObject(new int[] {1,2,3, 4,5,6}));
  //		assertThat(writableDataset.getDimensions()).isEqualTo(new int[]{2,3});
  //		assertThat(writableDataset.getMaxSize()).isEqualTo(new long[]{2,3});
  //		assertThat(writableDataset.getJavaType()).isEqualTo(int.class);
  //		assertThat(writableDataset.getDataLayout()).isEqualTo(DataLayout.CONTIGUOUS);
  //		assertThrows(HdfException.class, () ->
  //			writableDataset.getData(new long[] {1, 2}, new int[] {1, 2}));
  //		assertThat(writableDataset.getFillValue()).isNull();
  //		assertThat(writableDataset.getFilters()).isEmpty();
  //		assertThat(writableDataset.getDataType()).isNotNull();
  //	}
  //
  //	@Test
  //	void testGettingFlags() {
  //		int[][] data = new int[][] {{1,2,3}, {4,5,6}};
  //		WritableDatasetImpl writableDataset = new WritableDatasetImpl(data, "ints", null);
  //		assertThat(writableDataset.isScalar()).isFalse();
  //		assertThat(writableDataset.isEmpty()).isFalse();
  //		assertThat(writableDataset.isLink()).isFalse();
  //		assertThat(writableDataset.isGroup()).isFalse();
  //		assertThat(writableDataset.isVariableLength()).isFalse();
  //		assertThat(writableDataset.isCompound()).isFalse();
  //		assertThat(writableDataset.isAttributeCreationOrderTracked()).isFalse();
  //		assertThat(writableDataset.getType()).isEqualTo(NodeType.DATASET);
  //	}
  //
}
