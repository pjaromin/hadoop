package com.jaromin.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author "Patrick Jaromin <patrick@jaromin.com>"
 *
 * @param <G> Grouping/Partitioning key type
 * @param <S> Sorting key type
 */
@SuppressWarnings("rawtypes")
public class CompositeSortKey<G extends WritableComparable, 
	S extends WritableComparable> implements WritableComparable<CompositeSortKey> {

	// The key to use for grouping and partitioning our keys into the 
	// reduce phase.
	private G groupKey;
	
	// The key for sorting the keys.
	private S sortKey;

	public CompositeSortKey() {}

	public CompositeSortKey(G groupKey, S sortKey) {
		this.groupKey = groupKey;
		this.sortKey = sortKey;
	}

	public G getGroupKey() {
		return groupKey;
	}

	public void setGroupKey(G groupKey) {
		this.groupKey = groupKey;
	}

	public S getSortKey() {
		return sortKey;
	}

	public void setSortKey(S sortKey) {
		this.sortKey = sortKey;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		groupKey.readFields(in);
		sortKey.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		getGroupKey().write(out);
		getSortKey().write(out);
	}

	@Override @SuppressWarnings("unchecked")
	public int compareTo(CompositeSortKey that) {
		int compare = this.groupKey.compareTo(that.groupKey); 
		if (compare == 0) {
			compare = this.sortKey.compareTo(that.sortKey);
		}
		return compare;
	}
	
	public String toString() {
		return this.getClass().getName() + "[" + this.groupKey.toString() 
				+ ", " + this.sortKey.toString() + "]";
	}
	
	/**
	 * Comparator for sorting the composite key based on the partition key
	 * and then the sort key.
	 * @author Patrick Jaromin <patrick@jaromin.com>
	 *
	 */
	public static final class NaturalSortComparator extends WritableComparator implements RawComparator {

		public NaturalSortComparator() {
			super(CompositeSortKey.class, true);
		}

		@Override @SuppressWarnings({"unchecked" })
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			CompositeSortKey key1 = (CompositeSortKey) wc1;
			CompositeSortKey key2 = (CompositeSortKey) wc2;
			int compare = key1.getGroupKey().compareTo(key2.getGroupKey());
			if (compare == 0) {
				compare = key1.getSortKey().compareTo(key2.getSortKey());
			}
			return compare;
		}
	}

	/**
	 * Comparator for sorting the composite key based on the partition key
	 * and then the sort key.
	 * @author Patrick Jaromin <patrick@jaromin.com>
	 *
	 */
	public static final class ReverseSortComparator extends WritableComparator implements RawComparator {

		public ReverseSortComparator() {
			super(CompositeSortKey.class, true);
		}

		@Override @SuppressWarnings({"unchecked" })
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			CompositeSortKey key1 = (CompositeSortKey) wc1;
			CompositeSortKey key2 = (CompositeSortKey) wc2;
			int compare = key1.getGroupKey().compareTo(key2.getGroupKey());
			if (compare == 0) {
				compare = key2.getSortKey().compareTo(key1.getSortKey());
			}
			return compare;
		}
	}
	
	/**
	 * Comparator for grouping based on the key's partition key, natural ordering.
	 * @author Patrick Jaromin <patrick@jaromin.com>
	 *
	 */
	public static final class GroupingComparator extends WritableComparator {
		public GroupingComparator() {
			super(CompositeSortKey.class, true);
		}
		@SuppressWarnings({"unchecked" })
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeSortKey aKey1 = (CompositeSortKey) w1;
			CompositeSortKey aKey2 = (CompositeSortKey) w2;
			return aKey1.getGroupKey().compareTo(aKey2.getGroupKey());
		}
	}

	/**
	 * The default partitioner for this composite key.
	 * @author "Patrick Jaromin <patrick@jaromin.com>"
	 *
	 * @param <T>
	 */
	public static final class KeyPartitioner<T> extends Partitioner<CompositeSortKey, T> {
		@Override
		public int getPartition(CompositeSortKey key, T value, int numPartitions) {
			return Math.abs(key.getGroupKey().hashCode()) % numPartitions;
		}
	}
}
