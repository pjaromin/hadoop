package com.jaromin.mapreduce.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * @author "Patrick Jaromin <patrick@jaromin.com>"
 *
 * @param <G> Grouping/Partitioning key type
 * @param <S> Sorting key type
 */
public class CompositeSortKeySerialization<G extends WritableComparable<G>, S extends WritableComparable<S>>
	extends Configured implements Serialization<CompositeSortKey<G, S>> {
	
	public static final String CONF_KEY_GROUPKEY_CLASS = "com.jaromin.mapreduce.io.compositekey.groupclass";
	public static final String CONF_KEY_SORTKEY_CLASS = "com.jaromin.mapreduce.io.compositekey.sortclass";
	
	private Serializer<CompositeSortKey<G, S>> serializer;
	private Deserializer<CompositeSortKey<G, S>> deserializer;

	public CompositeSortKeySerialization() {}

	public CompositeSortKeySerialization(Configuration conf) {
		super(conf);
	}
	
	@Override
	public boolean accept(Class<?> c) {
		return CompositeSortKey.class.isAssignableFrom(c);
	}

	@Override @SuppressWarnings({ "unchecked", "rawtypes" })
	public Deserializer<CompositeSortKey<G, S>> getDeserializer(
			Class<CompositeSortKey<G, S>> arg0) {
		if (deserializer == null) {
			deserializer = new CompositeSortKeyDeserializer(getConf().getClass(CONF_KEY_GROUPKEY_CLASS, null),
					getConf().getClass(CONF_KEY_SORTKEY_CLASS, null));
		}
		return deserializer;
	}

	@Override  
	public Serializer<CompositeSortKey<G, S>> getSerializer(
			Class<CompositeSortKey<G, S>> arg0) {
		if (serializer == null) {
			serializer = new CompositeSortKeySerializer<G, S>();
		}
		return serializer;
	}

	/**
	 * Convenience method to configure the job for using the composite key.
	 * @param job
	 * @param groupKeyClass
	 * @param sortKeyClass
	 */
	@SuppressWarnings("rawtypes")
	public static void configureMapOutputKey(Job job, Class<? extends WritableComparable> groupKeyClass, 
			Class<? extends WritableComparable> sortKeyClass) {	
		
		// Set this class as our map output key
		job.setMapOutputKeyClass(CompositeSortKey.class);

		// Setup the partitioner and comparators.
		job.setPartitionerClass(CompositeSortKey.KeyPartitioner.class);
		job.setGroupingComparatorClass(CompositeSortKey.GroupingComparator.class);
		job.setSortComparatorClass(CompositeSortKey.NaturalSortComparator.class);

		job.getConfiguration().set(CONF_KEY_GROUPKEY_CLASS, groupKeyClass.getName());
		job.getConfiguration().set(CONF_KEY_SORTKEY_CLASS, sortKeyClass.getName());
		
		// Now setup the serialization by registering with the framework.
		Collection<String> serializations = new ArrayList<String>();
		serializations.add(CompositeSortKeySerialization.class.getName());
		serializations.addAll(job.getConfiguration().getStringCollection("io.serializations"));
		job.getConfiguration().setStrings("io.serializations", serializations.toArray(new String[]{}));

	}
	
	/**
	 * Handles serialization of the composite sort keys.
	 * @author "Patrick Jaromin <patrick@jaromin.com>"
	 *
	 * @param <G>
	 * @param <S>
	 */
	public static final class CompositeSortKeySerializer<G extends WritableComparable<G>, S extends WritableComparable<S>>
		implements Serializer<CompositeSortKey<G, S>> {
	
		private DataOutputStream out;

		@Override
		public void serialize(CompositeSortKey<G, S> key) throws IOException {
			key.getGroupKey().write(this.out);
			key.getSortKey().write(this.out);
		}	

		@Override
		public void open(OutputStream out) throws IOException {
			this.out = new DataOutputStream(out);
		}

		@Override
		public void close() throws IOException {
			IOUtils.closeStream(this.out);
		}
	}
	
	/**
	 * Handles deserialization of the sort keys.
	 * @author "Patrick Jaromin <patrick@jaromin.com>"
	 *
	 * @param <G>
	 * @param <S>
	 */
	public static final class CompositeSortKeyDeserializer<G extends WritableComparable<G>, S extends WritableComparable<S>>
			implements Deserializer<CompositeSortKey<G, S>> {

		private DataInputStream in;
		
		private Class<G> groupKeyClass;
		private Class<S> sortKeyClass;
		
		public CompositeSortKeyDeserializer(Class<G> groupKeyClass, Class<S> sortKeyClass) {
			this.groupKeyClass = groupKeyClass;
			this.sortKeyClass = sortKeyClass;
		}

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.io.serializer.Deserializer#deserialize(java.lang.Object)
		 */
		@Override
		public CompositeSortKey<G, S> deserialize(CompositeSortKey<G, S> reuse)
				throws IOException {
			if (reuse == null) {
				reuse = new CompositeSortKey<G, S>();
			}

			if (reuse.getGroupKey() == null) {
				try {
					reuse.setGroupKey(groupKeyClass.newInstance());
				} catch (InstantiationException | IllegalAccessException e) {
					throw new IOException("Unable to instantiate '" + groupKeyClass + "'");
				}
			}

			if (reuse.getSortKey() == null) {
				try {
					reuse.setSortKey(sortKeyClass.newInstance());
				} catch (InstantiationException | IllegalAccessException e) {
					throw new IOException("Unable to instantiate '" + sortKeyClass + "'");
				}
			}
			
			// Use the keys to deserialize...
			reuse.getGroupKey().readFields(this.in);
			reuse.getSortKey().readFields(this.in);

			return reuse;
		}

		@Override
		public void open(InputStream in) throws IOException {
			this.in = new DataInputStream(in);
		}

		@Override
		public void close() throws IOException {
			IOUtils.closeStream(this.in);
		}
	}
}
