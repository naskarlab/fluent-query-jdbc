package com.naskar.fluentquery.jdbc.impl;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import com.naskar.fluentquery.jdbc.ResultHandler;
import com.naskar.fluentquery.jdbc.ResultSetHandler;

public class ClassHandler<T> implements ResultSetHandler {
	
	private Class<T> clazz;
	private Map<String, Field> fields;
	private ResultHandler<T> action;
	
	public ClassHandler(Class<T> clazz, ResultHandler<T> action) {
		this.clazz = clazz;
		this.fields = getFields(clazz);
		this.action = action;
	}
	
	@Override
	public boolean next(ResultSet rs) {
		try {
			T r = clazz.newInstance();
			
			ResultSetMetaData md = rs.getMetaData(); 
			for(int i = 1; i <= md.getColumnCount(); i++) {
				
				Field f = fields.get(md.getColumnLabel(i).toUpperCase());
				
				if(f != null) {
					f.setAccessible(true);
					f.set(r, rs.getObject(i, f.getType()));
				}
				
			}
			
			return action.next(r);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private Map<String, Field> getFields(Class<?> clazz) {
		Map<String, Field> m = new HashMap<String, Field>();
		
		for(Field f : clazz.getDeclaredFields()) {
			m.put(f.getName().toUpperCase(), f);
		}
		
		return m;
	}
	
}
