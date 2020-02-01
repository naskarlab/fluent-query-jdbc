package com.naskar.fluentquery.jdbc;

@FunctionalInterface
public interface Inserter<T> {
	void insert(T t);
}