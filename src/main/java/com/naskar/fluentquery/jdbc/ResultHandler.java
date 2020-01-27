package com.naskar.fluentquery.jdbc;

@FunctionalInterface
public interface ResultHandler<T> {
	
	boolean next(T i);

}
