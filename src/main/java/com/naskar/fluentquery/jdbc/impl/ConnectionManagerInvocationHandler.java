package com.naskar.fluentquery.jdbc.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;

import com.naskar.fluentquery.jdbc.ConnectionScope;

public class ConnectionManagerInvocationHandler implements InvocationHandler {
	
	private Object target;
	private ConnectionScope scope;
	private boolean transacional = true;
	
	public ConnectionManagerInvocationHandler(Object target, ConnectionScope scope) {
		this(target, scope, true);
	}
	
	public ConnectionManagerInvocationHandler(Object target, ConnectionScope scope, boolean transacional) {
		this.target = target;
		this.scope = scope;
		this.transacional = transacional;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object result = null;
		
		Connection conn = null;
		try {
			result = method.invoke(target, args);
			
			conn = scope.get();
			if(conn != null) {
				if(transacional) {
					conn.commit();
				}
			}
			
		} catch(Exception e) {
			
			if(conn != null) {
				if(transacional) {
					try {
						conn.rollback();
					} catch(Exception et) {
						// TODO: logger;
						et.printStackTrace();
					}
				}
			}
			
			if(e instanceof InvocationTargetException) {
				throw ((InvocationTargetException)e).getTargetException();
			} else {
				throw e;
			}
			
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch(Exception et) {
					// TODO: logger;
					et.printStackTrace();
				}
				
				scope.remove(conn);
			}
		}
		
		return result;
	}
	
	public Object getTarget() {
		return this.target;
	}

}
