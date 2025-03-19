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

import io.jhdf.api.Attribute;
import io.jhdf.api.Group;
import io.jhdf.api.NodeType;
import io.jhdf.api.WritableDataset;
import io.jhdf.exceptions.HdfWritingException;
import io.jhdf.filter.PipelineFilterWithData;
import io.jhdf.object.datatype.DataType;
import io.jhdf.object.message.*;
import io.jhdf.object.message.DataLayoutMessage.ContiguousDataLayoutMessage;
import io.jhdf.storage.HdfFileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static io.jhdf.Utils.flatten;
import static io.jhdf.Utils.stripLeadingIndex;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

/**
 TODO: This won't work well for variable sized and padded types like Strings */
public class StreamableDatasetImpl extends AbstractWritableNode implements WritableDataset {

  private static final Logger logger = LoggerFactory.getLogger(StreamableDatasetImpl.class);

  //	private final Object data;
  private final DataType dataType;
  //
  private final DataSpace dataSpace;
  private final Supplier<Object>  supplier;
  // will double-buffer, this is only the hand-off, not the pre-read
  private final LinkedBlockingQueue<Object> chunks = new LinkedBlockingQueue<>(1);
  private final int size;

  public StreamableDatasetImpl(Supplier<Object> chunkSupplier, int size, String name, Group parent)
  {
    super(parent, name);
    this.supplier = chunkSupplier;
    this.size = size;
    Object example = chunkSupplier.get();
    if (example == null) {
      throw new HdfWritingException("Example (first chunk) of array for '" + name + "' was null");
    }
    chunks.add(example);
    this.dataType = DataType.fromObject(example);
    if (dataType.getJavaType().equals(String.class)) {
      throw new HdfWritingException("String types do not have stable length, and are not "
                                    + "supported yet for streamable dataset writing");
    }
    this.dataSpace = DataSpace.resized(DataSpace.fromObject(example), size);
  }

  @Override
  public long getSize() {
    return this.size;
  }

  @Override
  public long getSizeInBytes() {
    return getSize() * dataType.getSize();
  }

  @Override
  public long getStorageInBytes() {
    // As there is no compression this is correct ATM
    return getSizeInBytes();
  }

  @Override
  public int[] getDimensions() {
    return dataSpace.getDimensions();
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
    return size == 0;
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
    return dataSpace.getMaxSizes();
  }

  @Override
  public DataLayout getDataLayout() {
    // ATM we only support contiguous
    return DataLayout.CONTIGUOUS;
  }

  @Override
  public Object getData() {
    throw new HdfWritingException("Slicing a streamable dataset source not supported");
  }

  @Override
  public Object getDataFlat() {
    throw new HdfWritingException("Slicing a streamable dataset source not supported");
  }

  @Override
  public Object getData(long[] sliceOffset, int[] sliceDimensions) {
    throw new HdfWritingException("Slicing a writable dataset not supported");
  }

  @Override
  public Class<?> getJavaType() {
    final Class<?> type = dataType.getJavaType();
    // For scalar datasets the returned type will be the wrapper class because
    // getData returns Object
    if (isScalar() && type.isPrimitive()) {
      return primitiveToWrapper(type);
    }
    return type;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public Object getFillValue() {
    return null;
  }

  @Override
  public List<PipelineFilterWithData> getFilters() {
    // ATM no filters support
    return Collections.emptyList();
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

  @Override
  public long write(HdfFileChannel hdfFileChannel, long position) {
    logger.info("Writing dataset via chunked reads [{}] at position [{}]", getPath(), position);
    List<Message> messages = new ArrayList<>();
    messages.add(DataTypeMessage.create(this.dataType));
    messages.add(DataSpaceMessage.create(this.dataSpace));
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

    long dataSize = writeData(hdfFileChannel, dataAddress);

    // Now switch placeholder for real data layout message
    messages.add(ContiguousDataLayoutMessage.create(dataAddress, dataSize));
    messages.remove(placeholder);

    objectHeader = new ObjectHeader.ObjectHeaderV2(position, messages);

    hdfFileChannel.write(objectHeader.toBuffer(), position);

    return dataAddress + dataSize;
  }

  private long writeData(HdfFileChannel hdfFileChannel, long dataAddress) {
    logger.info("Writing data for dataset [{}] at position [{}]", getPath(), dataAddress);

    hdfFileChannel.position(dataAddress);

    DoubleBufferer doubleBuffer = DoubleBufferer.start(chunks, supplier, "ds-streamer-" + getName());
    int totalSize = 0;

    while (doubleBuffer.running || !chunks.isEmpty()) {
      Object chunk = null;
      try {
        chunk = chunks.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (chunk==null) {
        System.out.println("end of chunks during writing");
        break;
      }
      int[] chunkDimensions = DataSpace.fromObject(chunk).getDimensions();
      System.out.println("major dimension of chunk is " + chunkDimensions[0]);
      dataType.writeData(chunk, chunkDimensions, hdfFileChannel);
      totalSize += chunkDimensions[0];

    }

    if (totalSize != size) {
      throw new RuntimeException("total size written from data source (" + totalSize + ") does "
                                 + "not equal declared size (" + size + ")");
    }

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

  private final static class DoubleBufferer implements Runnable {

    private final LinkedBlockingQueue<Object> queue;
    private final Supplier<Object> supplier;
    public volatile boolean running = true;

    private DoubleBufferer(LinkedBlockingQueue<Object> queue, Supplier<Object> supplier) {
      this.queue = queue;
      this.supplier = supplier;
    }

    public static DoubleBufferer start(
        LinkedBlockingQueue<Object> queue,
        Supplier<Object> supplier,
        String name
    )
    {
      DoubleBufferer db = new DoubleBufferer(queue, supplier);
      Thread thread = new Thread(db);
      thread.setDaemon(true);
      thread.setName("bufferer-" + name);
      thread.start();
      return db;
    }

    @Override
    public void run() {
      Object object = supplier.get();
      while (object != null) {
        try {
          System.out.println("got object, supplying to queue");
          queue.put(object);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        object = supplier.get();
      }
      running = false;
    }
  }
}
