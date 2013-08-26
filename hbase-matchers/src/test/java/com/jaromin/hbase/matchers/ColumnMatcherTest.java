package com.jaromin.hbase.matchers;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class ColumnMatcherTest {

	private Put put;

	private byte[] rowKey =  Bytes.toBytes("rowKey");

	private byte[] columnFamilyA = Bytes.toBytes("a");
	private byte[] columnFamilyB = Bytes.toBytes("b");
	
	@Before
	public void setUp() {	
		put = new Put(rowKey);
		try {
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("string"), 
					Bytes.toBytes("string_value")));
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("long"), 
					Bytes.toBytes(100L)));
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("double"), 
					Bytes.toBytes(100.1d)));
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("int"), 
					Bytes.toBytes(100)));
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("float"), 
					Bytes.toBytes(100.1f)));
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("short"), 
					Bytes.toBytes((short)10)));
			put.add(new KeyValue(rowKey, columnFamilyA, Bytes.toBytes("bytes"), 
					Bytes.toBytes(100L)));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testHasStringColumn() {

		Put put = new Put(rowKey);
		put.add(columnFamilyA, "column1".getBytes(), "avalue".getBytes());
		put.add(columnFamilyA, "column2".getBytes(), "avalue".getBytes());
		put.add(columnFamilyA, "column3".getBytes(), "avalue".getBytes());
		put.add(columnFamilyB, "lastColumn".getBytes(), "avalue".getBytes());
		
		assertThat(put, ColumnMatcher.column("a:column1"));
		assertThat(put, ColumnMatcher.column(is("a:column1")));
		
		assertThat(put, ColumnMatcher.column(startsWith("a:col")));
		assertThat(put, ColumnMatcher.column(startsWith("b:last")));
		assertThat(put, ColumnMatcher.column(not(startsWith("c:col"))));
		assertThat(put, ColumnMatcher.column(not(startsWith("b:col"))));
		assertThat(put, ColumnMatcher.column(endsWith("column1")));
		assertThat(put, ColumnMatcher.column(containsString("last")));
		assertThat(put, ColumnMatcher.column(containsString("col")));
		
//		assertThat(put, hasColumnValue(column(startsWith("a:prefix_"), isString(""));
//		assertThat(put, PutMatchers.hasColumn(column, matcher))

//		assertThat(put, PutMatchers.hasColumn("d:column1").withValue("string_value", String.class));
	}
}
