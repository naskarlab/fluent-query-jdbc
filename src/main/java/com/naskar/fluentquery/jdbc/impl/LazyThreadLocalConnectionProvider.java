package com.naskar.fluentquery.jdbc.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import com.naskar.fluentquery.jdbc.ConnectionProvider;
import com.naskar.fluentquery.jdbc.ConnectionScope;

public class LazyThreadLocalConnectionProvider implements ConnectionProvider, ConnectionScope {
	
	private ThreadLocal<Connection> scope;
	private Supplier<Connection> getter;
	private boolean transacional = true;
	
	public LazyThreadLocalConnectionProvider(Supplier<Connection> getter, boolean transacional) {
		this.scope = new ThreadLocal<Connection>();
		this.getter = getter;
		this.transacional = transacional;
	}
	
	public LazyThreadLocalConnectionProvider(Supplier<Connection> getter) {
		this(getter, true);
	}
	
	public void set(Connection connection) {
		this.scope.set(connection);
	}
	
	@Override
	public Connection get() {
		return this.scope.get();
	}
	
	@Override
	public void remove(Connection c) {
		this.scope.remove();
	}

	@Override
	public Connection getConnection() {
		try {
			Connection conn = this.scope.get();
			
			if(conn == null) {
				conn = getter.get();
				
				if(transacional) {
					conn.setAutoCommit(false);
				}
				
				this.scope.set(conn);
			}
			
			return conn;
			
		} catch (SQLException e) {
			throw new RuntimeException(e);
			
		}
	}
	
	public boolean isTransacional() {
		return transacional;
	}

}
