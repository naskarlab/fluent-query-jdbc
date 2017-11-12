package com.naskar.fluentquery.jdbc;

import java.util.List;

import com.naskar.fluentquery.Into;
import com.naskar.fluentquery.Query;

public interface DAO {
	
	<T> Query<T> query(Class<T> clazz);
	
	<T> List<T> list(Query<T> query);
	
	<T> T single(Query<T> query);
	
	<T> Into<T> insert(Class<T> clazz);
	
	<T> void execute(Into<T> into);
	
	void list(String sql, List<Object> params, ResultSetHandler handler);
	
	void execute(String sql);
	
	void execute(String sql, List<Object> params, ResultSetHandler handlerKeys);
	
}
