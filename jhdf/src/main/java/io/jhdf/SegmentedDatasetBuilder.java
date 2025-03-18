/*
 * This file is part of jHDF. A pure Java library for accessing HDF5 files.
 *
 * https://jhdf.io
 *
 * Copyright (c) 2025 James Mudd
 *
 * MIT License see 'LICENSE' file
 */

package io.jhdf;

import io.jhdf.api.*;
import io.jhdf.exceptions.HdfWritingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 <P>This class represents a writable dataset. It is used to build up a dataset on disk in chunks
 before writing it to the target dataset. This is required for datasets that are too large to fit
 in memory.</P>

 <P>This version continues to use the dataset property inferencing that is in the original
 WritableDatasetImpl class. This means that the type and properties of the dataset are not known
 until the last chunk is written. Chunks that are written are stored in an on-disk temporary file
 which is adjacent to the target dataset.
 </P> */
public class SegmentedDatasetBuilder {

  private static final Logger logger = LoggerFactory.getLogger(SegmentedDatasetBuilder.class);
  private final String name;
  private final Group group
      ;
  private WritableHdfFile tmpFile;
  private final Path dspath;
  //  private DataType dataType;
  //  private DataSpace dataSpace;
  private int enumerator = 0;

  public SegmentedDatasetBuilder(String name, Group parent) {
    WritableHdfFile rootFile = findRootGroup(parent);
    Path fileAsPath = rootFile.getFileAsPath();
    this.name = name;
    this.group = parent;
    this.dspath = fileAsPath.resolveSibling("DS_" + name + ".tmpbuffer");
    this.tmpFile = HdfFile.write(dspath);
  }

  private WritableHdfFile findRootGroup(Group parent) {
    Node node = parent;
    while (node.getParent() != null && node.getParent() != node) {
      node = node.getParent();
    }
    if (node instanceof WritableHdfFile) {
      return (WritableHdfFile) node;
    } else {
      throw new HdfWritingException("Failed to find root group");
    }
  }

  public void putChunk(Object data) {
    int dsidx = enumerator++;
    String dsname = String.format("%05d", dsidx);
    WritableDataset extent = tmpFile.putDataset(dsname, data);
    //    DataType chunkDataType = DataType.fromObject(data);
    //    if (this.dataType == null) {
    //      this.dataType = chunkDataType;
    //    } else {
    //      this.dataType = this.dataType.reduce(chunkDataType);
    //    }
    //
    //    DataSpace chunkDataSpace = DataSpace.fromObject(data);
    //    if (this.dataSpace == null) {
    //      this.dataSpace = chunkDataSpace;
    //    } else {
    //      this.dataSpace = this.dataSpace.combine(chunkDataSpace);
    //    }
  }

  public synchronized WritableSegmentedDataset build() {
    if (tmpFile != null) {
      tmpFile.close();
      this.tmpFile = null;
    }
    return new WritableSegmentedDataset(this.name, this.group, new HdfFile(this.dspath));
  }

  //  public WritableDataset flush() {
  //    this.tmpFile.close();
  //    HdfFile rootGroup = new HdfFile(dspath);
  //    List<Dataset> datasets = new ArrayList<>();
  //    for (Node node : rootGroup) {
  //      Dataset ds = (Dataset) node;
  //      datasets.add(ds);
  //    }
  //    Optional<DataType> dt = datasets.stream().map(Dataset::getDataType).reduce(DataType::reduce);
  //
  //    return ((WritableGroup) getParent()).putSegmentedDataset(getName(), this);
  //  }

}
