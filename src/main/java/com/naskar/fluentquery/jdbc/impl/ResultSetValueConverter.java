package com.naskar.fluentquery.jdbc.impl;

import java.sql.ResultSet;

public interface ResultSetValueConverter {
	
	<R> R converter(ResultSet rs, String name, Class<R> clazz);

}
