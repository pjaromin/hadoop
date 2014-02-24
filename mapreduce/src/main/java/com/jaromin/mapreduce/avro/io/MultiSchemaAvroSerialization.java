package com.jaromin.mapreduce.avro.io;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class MultiSchemaAvroSerialization<T> extends Configured implements Serialization<AvroMultiWrapper<T>> {

	public static final String CONF_KEY_MULTI_SCHEMAS = "com.dotomi.avro.mapreduce.schemas";
	
	@Override
	public boolean accept(Class<?> c) {
		return AvroMultiWrapper.class.isAssignableFrom(c);
	}

	@Override
	public Deserializer<AvroMultiWrapper<T>> getDeserializer(Class<AvroMultiWrapper<T>> c) {
		return new AvroMultiDeserializer<T>(getConf());
	}

	@Override
	public Serializer<AvroMultiWrapper<T>> getSerializer(Class<AvroMultiWrapper<T>> c) {
		return new AvroMultiSerializer<T>(getConf());
	}

	/**
	 * 
	 * @param b
	 * @return
	 */
	protected static Schema getSchemaAt(Configuration conf, int b) {	
		String schemaName = conf.getStrings(CONF_KEY_MULTI_SCHEMAS)[b];
		if (schemaName == null) throw new IllegalStateException("No avro schema registered for data.");
		Schema schema = null;
		try {
			schema = (Schema)Class.forName(schemaName).getField("SCHEMA$").get(null);
		} catch (IllegalArgumentException | IllegalAccessException
				| NoSuchFieldException | SecurityException
				| ClassNotFoundException e) {
			logger().error(e.getMessage());
			throw new IllegalStateException("Configured class [" + schemaName 
					+ "] does not contain an accessible static SCHEMA$ member.");
		}
		return schema;
	}
	

	protected static int getIndexForSchema(Configuration conf, Class<?> c) {
		int idx = 0;
		for (String name : conf.getStrings(CONF_KEY_MULTI_SCHEMAS)) {
			if (c.getName().equals(name)) {
				return idx; 
			}
			idx++;
		}
		throw new IllegalStateException("Schema for class [" + c.getName()
				+ "] was not registered.");
	}

	/**
	 * Register the schemas this serializer will ser/deser to/from.
	 * @param schemas
	 */
	public static void registerSchemas(Job job, Schema...schemas) {
		String[] names = new String[schemas.length];
		int idx = 0;
		for (Schema schema : schemas) {
			names[idx++] = schema.getFullName();
		}
		job.getConfiguration().setStrings(CONF_KEY_MULTI_SCHEMAS, names);
		
		registerSerialization(job);
	}

	/**
	 * Add this class to the list of serializers.
	 * @param job
	 */
	protected static void registerSerialization(Job job) {
		String[] strings = job.getConfiguration().getStrings("io.serializations");
		String[] newStrings = new String[strings.length +1];
		System.arraycopy( strings, 0, newStrings, 0, strings.length );
		newStrings[newStrings.length-1] = MultiSchemaAvroSerialization.class.getName();
		job.getConfiguration().setStrings("io.serializations", newStrings);
		
	}
	
	/**
	 *
	 * @return
	 */
	protected static final Logger logger() {
		return LoggerFactory.getLogger(MultiSchemaAvroSerialization.class);
	}

}
