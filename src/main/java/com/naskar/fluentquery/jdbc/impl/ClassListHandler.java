package com.naskar.fluentquery.jdbc.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.naskar.fluentquery.jdbc.ResultSetHandler;

public class ClassListHandler<T> implements ResultSetHandler {
	
	private ClassHandler<T> handler;
	private List<T> list;
	
	public ClassListHandler(Class<T> clazz) {
		this.handler = new ClassHandler<T>(clazz, this::add);
		this.list = new ArrayList<T>();
	}
	
	private Boolean add(T i) {
		this.list.add(i);
		return true;
	}
	
	public List<T> getList() {
		return list;
	}
	
	@Override
	public boolean next(ResultSet rs) {
		return this.handler.next(rs);
	}
	
}
