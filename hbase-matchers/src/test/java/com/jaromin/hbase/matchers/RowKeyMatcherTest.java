package com.jaromin.hbase.matchers;

/**
 * Copyright 2013 Patrick K. Jaromin <patrick@jaromin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class RowKeyMatcherTest {

	@Test
	public void testBytesKey() {
		byte[] key = Bytes.toBytes("999999999");
		Put put = new Put(key);		
		assertThat(put, Matchers.hasRowKey(key));
		
		byte[] notKey = Bytes.toBytes("88888888888");
		assertThat(put, Matchers.hasRowKey(not(notKey), byte[].class));
	}
	
	@Test
	public void testStringKey() {
		String key = "string_key";
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, Matchers.hasRowKey(key));
		
		assertThat(put, Matchers.hasRowKey(not("not-the-key")));
		assertThat(put, Matchers.hasRowKey(startsWith("string_")));
		assertThat(put, Matchers.hasRowKey(not(startsWith("!string_"))));
	}

	@Test
	public void testStringKey_Append() {
		String key = "string_key";
		Append append = new Append(Bytes.toBytes(key));
		assertThat(append, Matchers.hasRowKey(key));
		
		assertThat(append, Matchers.hasRowKey(not("not-the-key")));
		assertThat(append, Matchers.hasRowKey(startsWith("string_")));
		assertThat(append, Matchers.hasRowKey(not(startsWith("!string_"))));
	}
	
	@Test
	public void testStringKey_Delete() {
		String key = "string_key";
		Delete delete = new Delete(Bytes.toBytes(key));
		assertThat(delete, Matchers.hasRowKey(key));
		
		assertThat(delete, Matchers.hasRowKey(not("not-the-key")));
		assertThat(delete, Matchers.hasRowKey(startsWith("string_")));
		assertThat(delete, Matchers.hasRowKey(not(startsWith("!string_"))));
	}
	
	@Test
	public void testLongKey() {
		Long key = 9999L;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, Matchers.hasRowKey(key));
		
		Long notKey = 10000l;
		assertThat(put, Matchers.hasRowKey(not(notKey), Long.class));
		assertThat(put, Matchers.hasRowKey(lessThan(notKey), Long.class));
		assertThat(put, Matchers.hasRowKey(not(greaterThan(notKey)), Long.class));
	}

	@Test
	public void testDoubleKey() {
		Double key = 9999d;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, Matchers.hasRowKey(key));
		
		Double notKey = 100000d;
		assertThat(put, Matchers.hasRowKey(not(notKey), Double.class));
		assertThat(put, Matchers.hasRowKey(lessThan(notKey), Double.class));
		assertThat(put, Matchers.hasRowKey(not(greaterThan(notKey)), Double.class));
	}
	
	@Test
	public void testFloatKey() {
		Float key = 9999f;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, Matchers.hasRowKey(key));
		
		Float notKey = 10000F;
		assertThat(put, Matchers.hasRowKey(not(notKey), Float.class));
		assertThat(put, Matchers.hasRowKey(lessThan(notKey), Float.class));
		assertThat(put, Matchers.hasRowKey(not(greaterThan(notKey)), Float.class));
	}

	@Test
	public void testIntegerKey() {
		Integer key = 999;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, Matchers.hasRowKey(key));
		
		Integer notKey = 1000;
		assertThat(put, Matchers.hasRowKey(not(notKey), Integer.class));
		assertThat(put, Matchers.hasRowKey(lessThan(notKey), Integer.class));
		assertThat(put, Matchers.hasRowKey(not(greaterThan(notKey)), Integer.class));
	}
	
	@Test
	public void testShortKey() {
		Short key = (short)9;
		Put put = new Put(Bytes.toBytes(key));		
		assertThat(put, Matchers.hasRowKey(key));
		
		Short notKey = 10;
		assertThat(put, Matchers.hasRowKey(not(notKey), Short.class));
		assertThat(put, Matchers.hasRowKey(lessThan(notKey), Short.class));
		assertThat(put, Matchers.hasRowKey(not(greaterThan(notKey)), Short.class));
	}
}
