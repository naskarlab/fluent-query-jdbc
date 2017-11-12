package com.naskar.fluentquery.jdbc.impl;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.naskar.fluentquery.jdbc.ResultSetHandler;

public class ClassListHandler<T> implements ResultSetHandler {
	
	private Class<T> clazz;
	private Map<String, Field> fields;
	private List<T> list;
	
	public ClassListHandler(Class<T> clazz) {
		this.clazz = clazz;
		this.fields = getFields(clazz);
		this.list = new ArrayList<T>();
	}
	
	public List<T> getList() {
		return list;
	}
	
	@Override
	public boolean next(ResultSet rs) {
		try {
			T r = clazz.newInstance();
			list.add(r);
			
			ResultSetMetaData md = rs.getMetaData(); 
			for(int i = 1; i <= md.getColumnCount(); i++) {
				
				Field f = fields.get(md.getColumnLabel(i).toUpperCase());
				
				if(f != null) {
					f.setAccessible(true);
					f.set(r, rs.getObject(i, f.getType())); 
					f.setAccessible(false);
				}
				
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return true;
	}
	
	private Map<String, Field> getFields(Class<?> clazz) {
		Map<String, Field> m = new HashMap<String, Field>();
		
		for(Field f : clazz.getDeclaredFields()) {
			m.put(f.getName().toUpperCase(), f);
		}
		
		return m;
	}
	
}
