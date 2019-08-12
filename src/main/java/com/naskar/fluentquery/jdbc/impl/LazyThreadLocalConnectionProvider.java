package com.naskar.fluentquery.jdbc.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import com.naskar.fluentquery.jdbc.ConnectionProvider;
import com.naskar.fluentquery.jdbc.ConnectionScope;

public class LazyThreadLocalConnectionProvider implements ConnectionProvider, ConnectionScope {
	
	private ThreadLocal<Connection> scope;
	private ThreadLocal<Object> scopeOwner;
	private Supplier<Connection> getter;
	private boolean transacional = true;
	
	public LazyThreadLocalConnectionProvider(Supplier<Connection> getter, boolean transacional) {
		this.scope = new ThreadLocal<Connection>();
		this.scopeOwner = new ThreadLocal<Object>();
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
	public void begin(Object owner) {
		if(scopeOwner.get() == null) {
			scopeOwner.set(owner);
		}
	}
	
	@Override
	public Connection end(Object owner) {
		if(scopeOwner.get() == owner) {
			Connection conn = scope.get();
			scope.remove();
			scopeOwner.remove();
			return conn;
		} else {
			return null;
		}
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
