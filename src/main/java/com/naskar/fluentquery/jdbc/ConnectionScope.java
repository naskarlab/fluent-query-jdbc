package com.naskar.fluentquery.jdbc;

import java.sql.Connection;

public interface ConnectionScope { 
	Connection get();
	void remove(Connection c);
}
