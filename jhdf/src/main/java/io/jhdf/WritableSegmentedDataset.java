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
import io.jhdf.filter.PipelineFilterWithData;
import io.jhdf.object.datatype.DataType;
import io.jhdf.object.message.*;
import io.jhdf.object.message.DataLayoutMessage.ContiguousDataLayoutMessage;
import io.jhdf.storage.HdfFileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.jhdf.Utils.stripLeadingIndex;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

/**
 <P>This class represents a writable dataset. It is used to build up a dataset on disk in chunks
 before writing it to the target dataset. This is required for datasets that are too large to fit
 in memory.</P>

 <P>This version continues to use the dataset property inferencing that is in the original
 WritableDatasetImpl class. This means that the type and properties of the dataset are not known
 until the last chunk is written. Chunks that are written are stored in an on-disk temporary file
 which is adjacent to the target dataset.
 </P> */
public class WritableSegmentedDataset extends AbstractWritableNode implements WritableDataset {

  private static final Logger logger = LoggerFactory.getLogger(WritableSegmentedDataset.class);
  private final HdfFile hdfFile;
  private int enumerator = 0;

  public WritableSegmentedDataset(String name, Group parent, HdfFile hdffile) {
    super(parent, name);
    this.hdfFile = hdffile;

  }

  private HdfFile findRootGroup(Group parent) {
    Node node = parent;
    while (node.getParent() != null && node.getParent() != node) {
      node = node.getParent();
    }
    if (node instanceof HdfFile) {
      return (HdfFile) node;
    } else {
      throw new HdfWritingException("Failed to find root group");
    }
  }

  private Stream<Dataset> datasets() {
    Map<String, Node> children = hdfFile.getChildren();
    List<String> list = children.keySet().stream().sorted().toList();
    Stream<Dataset> sds = list.stream().map(children::get).map(Dataset.class::cast);
    return sds;
  }

  private void forEach(Consumer<Dataset> effector) {
    datasets().forEach(effector);
  }


  private <T> T reduce(Function<Dataset, T> property, BinaryOperator<T> reducer) {
    Optional<T> value =
        datasets().map(property).reduce(reducer);
    return value.orElseThrow(() -> new RuntimeException(
        "Unable to computed required property '" + property + "' of segmented dataset"));
  }

  private <T> T reduceStable(Function<Dataset, T> property, String name) {
    return reduce(
        property, (a, b) -> {
          if (!a.equals(b)) {
            throw new RuntimeException(name + " must be stable across all dataset extents");
          }
          return a;
        }
    );
  }

  @Override
  public long getSize() {
    return reduce(Dataset::getSize, Math::addExact);
  }

  @Override
  public long getSizeInBytes() {
    return reduce(Dataset::getSizeInBytes, Math::addExact);
  }

  @Override
  public long getStorageInBytes() {
    return reduce(Dataset::getStorageInBytes, Math::addExact);
  }

  @Override
  public int[] getDimensions() {
    return reduce(
        Dataset::getDimensions, (a, b) -> {
          if (a.length != b.length) {
            throw new RuntimeException(
                "Dimensional structure should be congruent to combine dimensions");
          }
          int[] dims = new int[a.length];
          for (int i = 0; i < a.length; i++) {
            if (i == 0) {
              dims[i] = a[i] + b[i];
            } else {
              if (a[i] != b[i]) {
                throw new RuntimeException("All but the first dimension should be congruent");
              }
              dims[i] = a[i];
            }
          }
          return dims;
        }
    );
  }

  @Override
  public boolean isScalar() {
    if (isEmpty()) {
      return false;
    }
    return getDimensions().length == 0;
  }

  @Override
  public boolean isEmpty() {
    return (hdfFile.getChildren().isEmpty());
  }

  @Override
  public boolean isCompound() {
    return false;
  }

  @Override
  public boolean isVariableLength() {
    return false;
  }

  @Override
  public long[] getMaxSize() {
    return reduce(
        Dataset::getMaxSize, (a, b) -> {
          if (a.length != b.length) {
            throw new RuntimeException("dimensions must have congruent structure");
          }
          long[] sizes = new long[a.length];
          for (int i = 0; i < sizes.length; i++) {
            sizes[i] = Math.max(a[i], b[i]);
          }
          return sizes;
        }
    );
  }

  @Override
  public DataLayout getDataLayout() {
    // ATM we only support contiguous
    return DataLayout.CONTIGUOUS;
  }

  @Override
  public Object getData() {
    throw new RuntimeException(
        "Unable to get data for writable segmented dataset during construction.");
  }

  @Override
  public Object getDataFlat() {
    throw new RuntimeException(
        "Unable to get data for writable segmented dataset during construction.");
  }

  @Override
  public Object getData(long[] sliceOffset, int[] sliceDimensions) {
    throw new RuntimeException(
        "Unable to get data for writable segmented dataset during construction.");
  }

  @Override
  public Class<?> getJavaType() {
    return reduceStable(Dataset::getJavaType, "JavaType");
  }

  @Override
  public DataType getDataType() {
    return reduceStable(Dataset::getDataType, "DataType");
  }

  @Override
  public Object getFillValue() {
    return reduceStable(Dataset::getFillValue, "FillValue");
  }

  @Override
  public List<PipelineFilterWithData> getFilters() {
    return reduceStable(Dataset::getFilters, "Filters");
  }

  @Override
  public NodeType getType() {
    return NodeType.DATASET;
  }

  @Override
  public boolean isGroup() {
    return false;
  }

  @Override
  public File getFile() {
    return getParent().getFile();
  }

  @Override
  public Path getFileAsPath() {
    return getParent().getFileAsPath();
  }

  @Override
  public HdfFile getHdfFile() {
    return getParent().getHdfFile();
  }

  @Override
  public long getAddress() {
    throw new HdfWritingException("Address not known until written");
  }

  @Override
  public boolean isLink() {
    return false;
  }

  @Override
  public boolean isAttributeCreationOrderTracked() {
    return false;
  }

  private DataType computeDataType() {
    return reduce(Dataset::getDataType, DataType::reduce);
  }

  private DataSpace computeDataSpace() {
    return reduce(ds -> DataSpace.fromObject(ds.getData()), DataSpace::combine);
  }

  @Override
  public long write(HdfFileChannel hdfFileChannel, long position) {
    DataType dataType = this.computeDataType();
    DataSpace dataSpace = this.computeDataSpace();

    logger.info("Writing dataset [{}] at position [{}]", getPath(), position);
    List<Message> messages = new ArrayList<>();
    messages.add(DataTypeMessage.create(dataType));
    messages.add(DataSpaceMessage.create(dataSpace));
    messages.add(FillValueMessage.NO_FILL);
    // TODO will have know fixed size so don't really need these objects but for now...
    ContiguousDataLayoutMessage placeholder =
        ContiguousDataLayoutMessage.create(
            Constants.UNDEFINED_ADDRESS,
            Constants.UNDEFINED_ADDRESS
        );
    messages.add(placeholder);

    if (!getAttributes().isEmpty()) {
      AttributeInfoMessage attributeInfoMessage = AttributeInfoMessage.create();
      messages.add(attributeInfoMessage);
      for (Map.Entry<String, Attribute> attribute : getAttributes().entrySet()) {
        logger.info("Writing attribute [{}]", attribute.getKey());
        AttributeMessage attributeMessage =
            AttributeMessage.create(attribute.getKey(), attribute.getValue());
        messages.add(attributeMessage);
      }
    }

    ObjectHeader.ObjectHeaderV2 objectHeader = new ObjectHeader.ObjectHeaderV2(position, messages);
    int ohSize = objectHeader.toBuffer().limit();

    // Now know where we will write the data
    long dataAddress = position + ohSize;
    long dataSize = writeData(hdfFileChannel, dataAddress, dataType, dataSpace);

    // Now switch placeholder for real data layout message
    messages.add(ContiguousDataLayoutMessage.create(dataAddress, dataSize));
    messages.remove(placeholder);

    objectHeader = new ObjectHeader.ObjectHeaderV2(position, messages);

    hdfFileChannel.write(objectHeader.toBuffer(), position);

    try {
      logger.info("unlinking temp buffer file at " + this.hdfFile.getFileAsPath());
      Files.deleteIfExists(this.hdfFile.getFileAsPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataAddress + dataSize;
  }

  private long writeData(
      HdfFileChannel hdfFileChannel,
      long dataAddress,
      DataType dataType,
      DataSpace dataSpace
  )
  {
    logger.info("Writing data for dataset [{}] at position [{}]", getPath(), dataAddress);
    hdfFileChannel.position(dataAddress);

    forEach(d -> {
      dataType.writeData(d.getData(), d.getDimensions(), hdfFileChannel);
    });

    return dataSpace.getTotalLength() * dataType.getSize();
  }

  private static void writeDoubleData(
      Object data,
      int[] dims,
      ByteBuffer buffer,
      HdfFileChannel hdfFileChannel
  )
  {
    if (dims.length > 1) {
      for (int i = 0; i < dims[0]; i++) {
        Object newArray = Array.get(data, i);
        writeDoubleData(newArray, stripLeadingIndex(dims), buffer, hdfFileChannel);
      }
    } else {
      buffer.asDoubleBuffer().put((double[]) data);
      hdfFileChannel.write(buffer);
      buffer.clear();
    }
  }
}
