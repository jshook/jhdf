package io.jhdf.dataset.chunked.indexing;

import io.jhdf.HdfFileChannel;
import io.jhdf.Utils;
import io.jhdf.dataset.chunked.Chunk;
import io.jhdf.exceptions.HdfException;
import io.jhdf.exceptions.UnsupportedHdfException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public class FixedArrayIndex implements ChunkIndex {

    private static final byte[] FIXED_ARRAY_HEADER_SIGNATURE = "FAHD".getBytes();
    private static final byte[] FIXED_ARRAY_DATA_BLOCK_SIGNATURE = "FADB".getBytes();

    private final long headerAddress;
    private final int chunkSizeInBytes;
    private final int elementSize;
    private final int[] datasetDimensions;

    private final int clientId;
    private final int entrySize;
    private final int pageBits;
    private final int maxNumberOfEntries;
    private final long dataBlockAddress;

    private final List<Chunk> chunks;

    public FixedArrayIndex(HdfFileChannel hdfFc, long address, int chunkSizeInBytes, int elementSize, int[] datasetDimensions) {
        this.headerAddress = address;
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.elementSize = elementSize;
        this.datasetDimensions =datasetDimensions;

        final int headerSize = 12 + hdfFc.getSizeOfOffsets() + hdfFc.getSizeOfLengths();
        final ByteBuffer bb = hdfFc.readBufferFromAddress(address, headerSize);

        byte[] formatSignatureBytes = new byte[4];
        bb.get(formatSignatureBytes, 0, formatSignatureBytes.length);

        // Verify signature
        if (!Arrays.equals(FIXED_ARRAY_HEADER_SIGNATURE, formatSignatureBytes)) {
            throw new HdfException("Fixed array header signature 'FAHD' not matched, at address " + address);
        }

        // Version Number
        final byte version = bb.get();
        if (version != 0) {
            throw new HdfException("Unsupported fixed array index version detected. Version: " + version);
        }

        clientId = bb.get();
        entrySize = bb.get();
        pageBits = bb.get();

        maxNumberOfEntries = Utils.readBytesAsUnsignedInt(bb, hdfFc.getSizeOfLengths());
        dataBlockAddress = Utils.readBytesAsUnsignedLong(bb, hdfFc.getSizeOfOffsets());

        chunks = new ArrayList<>(maxNumberOfEntries);

        // TODO checksum

        FixedArrayDataBlock dataBlock = new FixedArrayDataBlock(hdfFc, dataBlockAddress);
    }

    private class FixedArrayDataBlock {

        private final int clientId;

        public FixedArrayDataBlock(HdfFileChannel hdfFc, long address) {

            // TODO header size ignoring paging
            final int headerSize = 6 + hdfFc.getSizeOfOffsets() * (maxNumberOfEntries +1);
            final ByteBuffer bb = hdfFc.readBufferFromAddress(address, headerSize);

            byte[] formatSignatureBytes = new byte[4];
            bb.get(formatSignatureBytes, 0, formatSignatureBytes.length);

            // Verify signature
            if (!Arrays.equals(FIXED_ARRAY_DATA_BLOCK_SIGNATURE, formatSignatureBytes)) {
                throw new HdfException("Fixed array data block signature 'FADB' not matched, at address " + address);
            }

            // Version Number
            final byte version = bb.get();
            if (version != 0) {
                throw new HdfException("Unsupported fixed array data block version detected. Version: " + version);
            }

            clientId = bb.get();

            final long headerAddress = Utils.readBytesAsUnsignedLong(bb, hdfFc.getSizeOfOffsets());
            if (headerAddress != FixedArrayIndex.this.headerAddress) {
                throw new HdfException("Fixed array data block header address missmatch");
            }

            // TODO ignoring paging here might need to revisit

            if (clientId == 0) {
                // Not filtered
                for (int i = 0; i < maxNumberOfEntries; i++) {
                    final long chunkAddress = Utils.readBytesAsUnsignedLong(bb, hdfFc.getSizeOfOffsets());
                    final int[] chunkOffset = Utils.linearIndexToDimensionIndex((i*chunkSizeInBytes)/elementSize, datasetDimensions);

                    chunks.add(new FixedArrayChunk(chunkAddress, chunkSizeInBytes, chunkOffset));
                }

            } else  if (clientId == 1) {
                // Filtered


            } else {
                throw new HdfException("Unreconized client ID  = " + clientId);
            }

        }
    }

    private class FixedArrayChunk implements Chunk {
        private final long address;
        private final int size;
        private final int[] chunkOffset;

        public FixedArrayChunk(long address, int size, int[] chunkOffset) {
            this.address = address;
            this.size = size;
            this.chunkOffset = chunkOffset;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public BitSet getFilterMask() {
            return null;
        }

        @Override
        public int[] getChunkOffset() {
            return chunkOffset;
        }

        @Override
        public long getAddress() {
            return address;
        }
    }

    @Override
    public Collection<Chunk> getAllChunks() {
        return chunks;
    }
}
