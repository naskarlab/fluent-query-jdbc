package com.naskar.fluentquery.jdbc;

import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetHandler {
	
	boolean next(ResultSet rs);

}
