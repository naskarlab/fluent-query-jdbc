package com.naskar.fluentquery.jdbc.impl;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import com.naskar.fluentquery.jdbc.ResultSetHandler;

public class ClassFieldResultSetHandler implements ResultSetHandler {
	
	private Class<?> clazz;
	private Map<String, Field> fields;
	
	public ClassFieldResultSetHandler(Class<?> clazz) {
		this.clazz = clazz;
		this.fields = getFields(clazz);
	}
	
	@Override
	public boolean next(ResultSet rs) {
		try {
			Object r = clazz.newInstance();
			
			ResultSetMetaData md = rs.getMetaData(); 
			for(int i = 1; i < md.getColumnCount(); i++) {
				
				Field f = fields.get(md.getColumnName(i));
				
				if(f != null) {
					f.setAccessible(true);
					f.set(r, rs.getObject(i));
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
