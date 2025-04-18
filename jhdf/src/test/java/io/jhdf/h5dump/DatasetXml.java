/*
 * This file is part of jHDF. A pure Java library for accessing HDF5 files.
 *
 * https://jhdf.io
 *
 * Copyright (c) 2025 James Mudd
 *
 * MIT License see 'LICENSE' file
 */
package io.jhdf.h5dump;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class DatasetXml extends NodeXml {

	@JacksonXmlProperty(localName = "StorageLayout")
	String storageLayout;
	@JacksonXmlProperty(localName = "FillValueInfo")
	String fillValue;

	@JacksonXmlProperty(localName = "Data")
	DataXml data;

	@JacksonXmlProperty(localName = "Dataspace")
	DataspaceXml dataspace;

	@JacksonXmlProperty(localName = "DataType")
	DataTypeXml dataType;

	public String[] getData() {
		return data.getData();
	}

	public int[] getDimensions() {
		return dataspace.getDimensions();
	}

}
