package com.naskar.fluentquery.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface PreparedStatementHandler {
	
	void handle(PreparedStatement st) throws SQLException;

}
