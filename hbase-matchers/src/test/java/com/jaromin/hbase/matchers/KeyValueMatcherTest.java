package com.jaromin.hbase.matchers;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Patrick
 *
 */
public class KeyValueMatcherTest {

	private byte[] rowKey =  Bytes.toBytes("rowKey");

	private byte[] columnFamilyA = Bytes.toBytes("a");
	private byte[] columnFamilyB = Bytes.toBytes("b");
	
	@Before
	public void setUp() {	
	}

	@Test
	public void testAllMatchers() {
		Put put = new Put(rowKey);
		put.add(columnFamilyA, "column1".getBytes(), "avalue1".getBytes());
		put.add(columnFamilyA, "column2".getBytes(), "avalue2".getBytes());
		put.add(columnFamilyA, "column3".getBytes(), "avalue3".getBytes());
		put.add(columnFamilyB, "lastColumn".getBytes(), "bvalue".getBytes());
	
		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(startsWith("a:col")), is("avalue1")));
		
		assertThat(put, KeyValueMatcher.hasKeyValue(ColumnMatcher.column(startsWith("a:col")), is("avalue1")));
		
		assertThat(put, KeyValueMatcher.hasKeyValue(is("avalue1")));
		assertThat(put, KeyValueMatcher.hasKeyValue(ColumnMatcher.column(startsWith("b:last")), is(not(startsWith("av")))));
		assertThat(put, KeyValueMatcher.hasKeyValue(is(not(startsWith("cv")))));
		
		
		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(startsWith("a:col")), is(not("avalue"))));
		
		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(is("a:column2")), is("avalue2")));
		
		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(is("a:column3")), is(not(startsWith("value")))));
		
		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(startsWith("a:col")), startsWith("avalue")));

		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(startsWith("a:col")), containsString("avalue")));
		
		assertThat(put, KeyValueMatcher.hasKeyValue(PutMatchers.hasRowKey(is("rowKey")), 
				ColumnMatcher.column(startsWith("b:lastColumn")), is("bvalue")));		
	}	
}
