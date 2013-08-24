package com.jaromin.hbase.matchers;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class RowKeyMatcherTest {

	@Test
	public void testBytesKey() {
		byte[] key = Bytes.toBytes("999999999");
		Put put = new Put(key);		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		byte[] notKey = Bytes.toBytes("88888888888");
		assertThat(put, PutMatchers.hasRowKey(not(notKey), byte[].class));
	}
	
	@Test
	public void testStringKey() {
		String key = "string_key";
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		assertThat(put, PutMatchers.hasRowKey(not("not-the-key")));
		assertThat(put, PutMatchers.hasRowKey(startsWith("string_")));
		assertThat(put, PutMatchers.hasRowKey(not(startsWith("!string_"))));
	}

	@Test
	public void testLongKey() {
		Long key = 9999L;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		Long notKey = 10000l;
		assertThat(put, PutMatchers.hasRowKey(not(notKey), Long.class));
		assertThat(put, PutMatchers.hasRowKey(lessThan(notKey), Long.class));
		assertThat(put, PutMatchers.hasRowKey(not(greaterThan(notKey)), Long.class));
	}

	@Test
	public void testDoubleKey() {
		Double key = 9999d;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		Double notKey = 100000d;
		assertThat(put, PutMatchers.hasRowKey(not(notKey), Double.class));
		assertThat(put, PutMatchers.hasRowKey(lessThan(notKey), Double.class));
		assertThat(put, PutMatchers.hasRowKey(not(greaterThan(notKey)), Double.class));
	}
	
	@Test
	public void testFloatKey() {
		Float key = 9999f;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		Float notKey = 10000F;
		assertThat(put, PutMatchers.hasRowKey(not(notKey), Float.class));
		assertThat(put, PutMatchers.hasRowKey(lessThan(notKey), Float.class));
		assertThat(put, PutMatchers.hasRowKey(not(greaterThan(notKey)), Float.class));
	}

	@Test
	public void testIntegerKey() {
		Integer key = 999;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		Integer notKey = 1000;
		assertThat(put, PutMatchers.hasRowKey(not(notKey), Integer.class));
		assertThat(put, PutMatchers.hasRowKey(lessThan(notKey), Integer.class));
		assertThat(put, PutMatchers.hasRowKey(not(greaterThan(notKey)), Integer.class));
	}
	
	@Test
	public void testShortKey() {
		Short key = (short)9;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, PutMatchers.hasRowKey(key));
		
		Short notKey = 10;
		assertThat(put, PutMatchers.hasRowKey(not(notKey), Short.class));
		assertThat(put, PutMatchers.hasRowKey(lessThan(notKey), Short.class));
		assertThat(put, PutMatchers.hasRowKey(not(greaterThan(notKey)), Short.class));
	}
}
