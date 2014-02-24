package com.jaromin.mapreduce.avro.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class AvroMultiDeserializer<T> extends Configured implements
		Deserializer<AvroMultiWrapper<T>> {

	private Map<Integer, DatumReader<T>> readers;
	
	private BinaryDecoder decoder;
	
	public AvroMultiDeserializer(Configuration conf) {
		super(conf);
		readers = new HashMap<Integer, DatumReader<T>>();
	}

	@Override
	public AvroMultiWrapper<T> deserialize(AvroMultiWrapper<T> wrapper)
			throws IOException {
		if (wrapper == null) {
			wrapper = new AvroMultiWrapper<T>();
		}

		// Read in the first byte - the schema index
		int schemaIndex = decoder.inputStream().read();
		
		// Now hand off the rest to the datum reader for normal deser.
		DatumReader<T> reader = datumReaderFor(schemaIndex);
		wrapper.datum(reader.read(wrapper.datum(), decoder));
		return wrapper;
	}

	private DatumReader<T> datumReaderFor(int schemaIndex) {
		DatumReader<T> reader = readers.get(schemaIndex);
		if (reader == null) {
			Schema schema = MultiSchemaAvroSerialization.getSchemaAt(getConf(), schemaIndex);
			reader = new ReflectDatumReader<T>(schema);
			readers.put(schemaIndex, reader);
		}
		return reader;
	}

	@Override
	public void open(InputStream inputStream) throws IOException {
		this.decoder = DecoderFactory.get().directBinaryDecoder(inputStream, decoder);
	}

	@Override
	public void close() throws IOException {
		decoder.inputStream().close();
	}

}
