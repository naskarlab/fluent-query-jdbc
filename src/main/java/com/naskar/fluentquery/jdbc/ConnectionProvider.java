package com.naskar.fluentquery.jdbc;

import java.sql.Connection;

public interface ConnectionProvider {
	
	Connection getConnection();

}
