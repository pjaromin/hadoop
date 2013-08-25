package com.jaromin.hbase.matchers;

import static org.junit.Assert.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * 
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 */
public class ColumnMatcherTest {

	byte[] rowKey =  Bytes.toBytes("rowKey");

	byte[] columnFamily = Bytes.toBytes("d");
	
	@Test
	public void testHasStringColumn() {

		Put put = new Put(rowKey);
		put.add(columnFamily, "column1".getBytes(), "avalue".getBytes());

//		assertThat(put, PutMatchers.hasColumn("d:column1").withValue("string_value", String.class));
	}
}
