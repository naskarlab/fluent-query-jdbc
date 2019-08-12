package com.naskar.fluentquery.jdbc;

import java.sql.Connection;

public interface ConnectionScope { 
	void begin(Object owner);
	Connection end(Object owner);
}
