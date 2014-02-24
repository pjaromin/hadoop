package com.jaromin.mapreduce.avro.io;

/**
 * A wrapper for Avro serialization that isn't limited
 * to single key and single value schema.
 * @author Patrick Jaromin <patrick@jaromin.com>
 *
 * @param <T>
 */
public class AvroMultiWrapper<T> {

	private T datum;

	public AvroMultiWrapper() {
	}

	public AvroMultiWrapper(T datum) {
		this.datum = datum;
	}

	public void datum(T datum) {
		this.datum = datum;
	}

	public T datum() {
		return this.datum;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((datum == null) ? 0 : datum.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AvroMultiWrapper other = (AvroMultiWrapper) obj;
		if (datum == null) {
			if (other.datum != null)
				return false;
		} else if (!datum.equals(other.datum))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return datum.toString();
	}
	
	public static class StringWrapper extends AvroMultiWrapper<String> {
		
	}

}
