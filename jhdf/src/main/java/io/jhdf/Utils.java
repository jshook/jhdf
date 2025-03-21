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

import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public final class Utils {
	private static final CharsetEncoder ASCII = StandardCharsets.US_ASCII.newEncoder();

	private Utils() {
		throw new AssertionError("No instances of Utils");
	}

	/**
	 * Converts an address to a hex string for display
	 *
	 * @param address to convert to Hex
	 * @return the address as a hex string
	 */
	public static String toHex(long address) {
		if (address == Constants.UNDEFINED_ADDRESS) {
			return "UNDEFINED";
		}
		return "0x" + Long.toHexString(address);
	}

	/**
	 * Reads UTF8 string from the buffer until a null character is reached. This
	 * will read from the buffers current position. After the method the buffer
	 * position will be after the null character.
	 *
	 * @param buffer to read from
	 * @return the string read from the buffer
	 * @throws IllegalArgumentException if the end of the buffer if reached before
	 *                                  and null terminator
	 */
	public static String readUntilNull(ByteBuffer buffer) {
		buffer.mark();
		int length = 0;
		// Step through the buffer while NULL is not found
		while (buffer.hasRemaining()) {
			byte b = buffer.get();
			if (b == Constants.NULL) {
				buffer.reset();
				byte[] bytes = new byte[length];
				buffer.get(bytes);
				buffer.get(); // Step over the NULL again to position the buffer
				return new String(bytes, StandardCharsets.UTF_8);
			}
			length++;
		}
		throw new IllegalArgumentException("End of buffer reached before NULL");
	}

	/**
	 * Check the provided name to see if it is valid for a HDF5 identifier. Checks
	 * name only contains ASCII characters and does not contain '/' or '.' which are
	 * reserved characters.
	 *
	 * @param name To check if valid
	 * @return <code>true</code> if this is a valid HDF5 name, <code>false</code>
	 * otherwise
	 */
	public static boolean validateName(String name) {
		return ASCII.canEncode(name) && !name.contains("/") && !name.contains(".");
	}

	/**
	 * Moves the position of the {@link ByteBuffer} to the next position aligned on
	 * 8 bytes. If the buffer position is already a multiple of 8 the position will
	 * not be changed.
	 *
	 * @param bb the buffer to be aligned
	 */
	public static void seekBufferToNextMultipleOfEight(ByteBuffer bb) {
		int pos = bb.position();
		if (pos % 8 == 0) {
			return; // Already on a 8 byte multiple
		}
		bb.position(pos + (8 - (pos % 8)));
	}

	public static long nextMultipleOfEight(long value) {
		if (value % 8 == 0) {
			return value; // Already on a 8 byte multiple
		}
		return value + (8 - (value % 8));
	}

	/**
	 * This reads the requested number of bytes from the buffer and returns the data
	 * as an unsigned <code>int</code>. After this call the buffer position will be
	 * advanced by the specified length.
	 * <p>
	 * This is used in HDF5 to read "size of lengths" and "size of offsets"
	 *
	 * @param buffer to read from
	 * @param length the number of bytes to read
	 * @return the <code>int</code> value read from the buffer
	 * @throws ArithmeticException      if the data cannot be safely converted to an
	 *                                  unsigned <code>int</code>
	 * @throws IllegalArgumentException if the length requested is not supported i.e
	 *                                  &gt; 8
	 */
	public static int readBytesAsUnsignedInt(ByteBuffer buffer, int length) {
		switch (length) {
			case 1:
				return Byte.toUnsignedInt(buffer.get());
			case 2:
				return Short.toUnsignedInt(buffer.getShort());
			case 3:
				return readArbitraryLengthBytesAsUnsignedInt(buffer, length);
			case 4:
				int value = buffer.getInt();
				if (value < 0) {
					throw new ArithmeticException("Could not convert to unsigned");
				}
				return value;
			case 5:
			case 6:
			case 7:
				return readArbitraryLengthBytesAsUnsignedInt(buffer, length);
			case 8:
				// Throws if the long can't be converted safely
				return Math.toIntExact(buffer.getLong());
			default:
				throw new IllegalArgumentException("Couldn't read " + length + " bytes as int");
		}
	}

	/**
	 * This method is used when the length required is awkward i.e. no support
	 * directly from {@link ByteBuffer}
	 *
	 * @param buffer to read from
	 * @param length the number of bytes to read
	 * @return the long value read from the buffer
	 * @throws ArithmeticException if the data cannot be safely converted to an
	 *                             unsigned long
	 */
	private static int readArbitraryLengthBytesAsUnsignedInt(ByteBuffer buffer, int length) {
		// Here we will use BigInteger to convert a byte array
		byte[] bytes = new byte[length];
		buffer.get(bytes);
		// BigInteger needs big endian so flip the order if needed
		if (buffer.order() == LITTLE_ENDIAN) {
			ArrayUtils.reverse(bytes);
		}
		// Convert to a unsigned long throws if it overflows
		return new BigInteger(1, bytes).intValueExact();
	}

	/**
	 * This reads the requested number of bytes from the buffer and returns the data
	 * as an unsigned long. After this call the buffer position will be advanced by
	 * the specified length.
	 * <p>
	 * This is used in HDF5 to read "size of lengths" and "size of offsets"
	 *
	 * @param buffer to read from
	 * @param length the number of bytes to read
	 * @return the long value read from the buffer
	 * @throws ArithmeticException      if the data cannot be safely converted to an
	 *                                  unsigned long
	 * @throws IllegalArgumentException if the length requested is not supported;
	 */
	public static long readBytesAsUnsignedLong(ByteBuffer buffer, int length) {
		switch (length) {
			case 1:
				return Byte.toUnsignedLong(buffer.get());
			case 2:
				return Short.toUnsignedLong(buffer.getShort());
			case 3:
				return readArbitraryLengthBytesAsUnsignedLong(buffer, length);
			case 4:
				return Integer.toUnsignedLong(buffer.getInt());
			case 5:
			case 6:
			case 7:
				return readArbitraryLengthBytesAsUnsignedLong(buffer, length);
			case 8:
				long value = buffer.getLong();
				if (value < 0 && value != Constants.UNDEFINED_ADDRESS) {
					throw new ArithmeticException("Could not convert to unsigned value: " + value);
				}
				return value;
			default:
				throw new IllegalArgumentException("Couldn't read " + length + " bytes as int");
		}
	}

	/**
	 * This method is used when the length required is awkward i.e. no support
	 * directly from {@link ByteBuffer}
	 *
	 * @param buffer to read from
	 * @param length the number of bytes to read
	 * @return the long value read from the buffer
	 * @throws ArithmeticException if the data cannot be safely converted to an
	 *                             unsigned long
	 */
	private static long readArbitraryLengthBytesAsUnsignedLong(ByteBuffer buffer, int length) {
		// Here we will use BigInteger to convert a byte array
		byte[] bytes = new byte[length];
		buffer.get(bytes);
		// BigInteger needs big endian so flip the order if needed
		if (buffer.order() == LITTLE_ENDIAN) {
			ArrayUtils.reverse(bytes);
		}
		// Convert to a unsigned long throws if it overflows
		return new BigInteger(1, bytes).longValueExact();
	}

	/**
	 * Creates a new {@link ByteBuffer} of the specified length. The new buffer will
	 * start at the current position of the source buffer and will be of the
	 * specified length. The {@link ByteOrder} of the new buffer will be the same as
	 * the source buffer. After the call the source buffer position will be
	 * incremented by the length of the sub-buffer. The new buffer will share the
	 * backing data with the source buffer.
	 *
	 * @param source the buffer to take the sub buffer from
	 * @param length the size of the new sub-buffer
	 * @return the new sub buffer
	 */
	public static ByteBuffer createSubBuffer(ByteBuffer source, int length) {
		ByteBuffer headerData = source.slice();
		headerData.limit(length);
		headerData.order(source.order());

		// Move the buffer past this header
		source.position(source.position() + length);
		return headerData;
	}

	private static final BigInteger TWO = BigInteger.valueOf(2);

	/**
	 * Takes a {@link BitSet} and a range of bits to inspect and converts the bits
	 * to a integer.
	 *
	 * @param bits   to inspect
	 * @param start  the first bit
	 * @param length the number of bits to inspect
	 * @return the integer represented by the provided bits
	 */
	public static int bitsToInt(BitSet bits, int start, int length) {
		if (length <= 0) {
			throw new IllegalArgumentException("length must be >0");
		}
		BigInteger result = BigInteger.ZERO;
		for (int i = 0; i < length; i++) {
			if (bits.get(start + i)) {
				result = result.add(TWO.pow(i));
			}
		}
		return result.intValue();
	}

	/**
	 * Calculates how many bytes are needed to store the given unsigned number.
	 *
	 * @param number to store
	 * @return the number of bytes needed to hold this number
	 * @throws IllegalArgumentException if a negative number is given
	 */
	public static int bytesNeededToHoldNumber(long number) {
		if (number < 0) {
			throw new IllegalArgumentException("Only for unsigned numbers");
		}
		if (number == 0) {
			return 1;
		}
		return (int) Math.ceil(BigInteger.valueOf(number).bitLength() / 8.0);
	}

	public static int[] linearIndexToDimensionIndex(int index, int[] dimensions) {
		int[] dimIndex = new int[dimensions.length];

		for (int i = dimIndex.length - 1; i >= 0; i--) {
			dimIndex[i] = index % dimensions[i];
			index = index / dimensions[i];
		}
		return dimIndex;
	}

	public static int dimensionIndexToLinearIndex(int[] index, int[] dimensions) {
		long[] indexAsLong = Arrays.stream(index).asLongStream().toArray();
		long linearLong = dimensionIndexToLinearIndex(indexAsLong, dimensions);
		return Math.toIntExact(linearLong);
	}

	public static long dimensionIndexToLinearIndex(long[] index, int[] dimensions) {
		if(index.length != dimensions.length) {
			throw new IllegalArgumentException("Mismatched index and dimension lengths");
		}
		for (int i = 0; i < dimensions.length; i++) {
			if(index[i] < 0 || dimensions[i] < 0) {
				throw new IllegalArgumentException("Negative index or dimension values");
			}
			if(index[i] > dimensions[i]) {
				throw new IllegalArgumentException("index is greater than dimension size");
			}
		}

		long linear = 0;
		for (int i = 0; i < dimensions.length; i++) {
			long temp = index[i];
			for (int j = i + 1; j < dimensions.length; j++) {
				temp *= dimensions[j];
			}
			linear += temp;
		}
		return linear;
	}

	/**
	 * Calculates the chunk offset from a given chunk index
	 *
	 * @param chunkIndex        The index to calculate for
	 * @param chunkDimensions   The chunk dimensions
	 * @param datasetDimensions The dataset dimensions
	 * @return The chunk offset for the chunk of this index
	 */
	public static int[] chunkIndexToChunkOffset(int chunkIndex, int[] chunkDimensions, int[] datasetDimensions) {
		final int[] chunkOffset = new int[chunkDimensions.length];

		// Start from the slowest dim
		for (int i = 0; i < chunkOffset.length; i++) {
			// Find out how many chunks make one chunk in this dim
			int chunksBelowThisDim = 1;
			// Start one dim faster
			for (int j = i + 1; j < chunkOffset.length; j++) {
				chunksBelowThisDim *= (int) Math.ceil((double) datasetDimensions[j] / chunkDimensions[j]);
			}

			chunkOffset[i] = (chunkIndex / chunksBelowThisDim) * chunkDimensions[i];
			chunkIndex -= chunkOffset[i] / chunkDimensions[i] * chunksBelowThisDim;
		}

		return chunkOffset;
	}

	/**
	 * Removes the zeroth (leading) index. e.g [1,2,3] → [2,3]
	 *
	 * @param dims the array to strip
	 * @return dims with the zeroth element removed
	 */
	public static int[] stripLeadingIndex(int[] dims) {
		return Arrays.copyOfRange(dims, 1, dims.length);
	}

	// See https://stackoverflow.com/a/4674035
	public static void setBit(byte[] bytes, int bit, boolean value) {
		if(bit < 0 || bit >= bytes.length * 8){
			throw new IllegalArgumentException("bit index out of range. index=" + bit);
		}
		final int byteIndex = bit / 8;
		final int bitInByte = bit % 8;
		if(value) {
			bytes[byteIndex] |= (byte) (1 << bitInByte);
		} else {
			bytes[byteIndex] &= (byte) ~(1 << bitInByte);
		}
	}

	public static boolean getBit(byte[] bytes, int bit) {
		final int byteIndex = bit / 8;
		final int bitInByte = bit % 8;
		return ((bytes[byteIndex]  >> bitInByte) & 1) == 1;
	}

	public static int[] getDimensions(Object data) {
		List<Integer> dims = new ArrayList<>();
		int dimLength = Array.getLength(data);
		dims.add(dimLength);

		while (dimLength > 0 && Array.get(data, 0).getClass().isArray()) {
			data = Array.get(data, 0);
			dims.add(Array.getLength(data));
		}
		return ArrayUtils.toPrimitive(dims.toArray(new Integer[0]));
	}

	public static Class<?> getType(Object obj) {
		final Class<?> type;
		if(obj.getClass().isArray()) {
			type = getArrayType(obj);
		} else {
			type = obj.getClass();
		}
		return type;
	}

	public static Class<?> getArrayType(Object array) {
		Object element = Array.get(array, 0);
		if (element.getClass().isArray()) {
			return getArrayType(element);
		} else {
			return array.getClass().getComponentType();
		}
	}

	public static void writeIntToBits(int value, BitSet bits, int start, int length) {
		if(value < 0) {
			throw new IllegalArgumentException("Value cannot be negative");
		}
		BigInteger bi = BigInteger.valueOf(value);
		if(bi.bitLength() > length) {
			throw new IllegalArgumentException("Value [" + value + "] to high to convert to bits");
		}
		for (int i = 0; i < length; i++) {
			bits.set(start + i, bi.testBit(i));
		}
	}

    public static Object[] flatten(Object data) {
        List<Object> flat = new ArrayList<>();
        flattenInternal(data, flat);
        return flat.toArray();
    }

	private static void flattenInternal(Object data, List<Object> flat) {
		if (data.getClass().isArray()) {
			int length = Array.getLength(data);
			for (int i = 0; i < length; i++) {
				Object element = Array.get(data, i);
				if (element.getClass().isArray()) {
					flattenInternal(element, flat);
				} else {
					flat.add(element);
				}
			}
		} else {
			flat.add(data);
		}
	}

	public static int totalChunks(int[] datasetDimensions, int[] chunkDimensions) {
		int chunks = 1;
		for (int i = 0; i < datasetDimensions.length; i++) {
			int chunksInDim = datasetDimensions[i] / chunkDimensions[i];
			// If there is a partial chunk then we need to add one chunk in this dim
			if(datasetDimensions[i] % chunkDimensions[i] != 0 ) chunksInDim++;
			chunks *=  chunksInDim;
		}
		return chunks;
	}
}
