package com.naskar.fluentquery.jdbc.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.naskar.fluentquery.InsertBuilder;
import com.naskar.fluentquery.Into;
import com.naskar.fluentquery.Query;
import com.naskar.fluentquery.QueryBuilder;
import com.naskar.fluentquery.conventions.MappingConvention;
import com.naskar.fluentquery.converters.NativeSQL;
import com.naskar.fluentquery.converters.NativeSQLInsertInto;
import com.naskar.fluentquery.converters.NativeSQLResult;
import com.naskar.fluentquery.jdbc.ConnectionProvider;
import com.naskar.fluentquery.jdbc.DAO;
import com.naskar.fluentquery.jdbc.ResultSetHandler;
import com.naskar.fluentquery.mapping.MappingValueProvider;
import com.naskar.fluentquery.mapping.MappingValueProvider.ValueProvider;

public class DAOImpl implements DAO {
	
	private static final Logger logger = Logger.getLogger(DAOImpl.class.getName());
	
	private ConnectionProvider connectionProvider;
	private MappingConvention mappings;
	
	private NativeSQL nativeSQL;	
	private QueryBuilder queryBuilder;
	
	private NativeSQLInsertInto insertSQL;
	private InsertBuilder insertBuilder;
	
	public DAOImpl(ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
		
		this.mappings = new MappingConvention();
		
		this.nativeSQL = new NativeSQL();
		this.nativeSQL.setConvention(mappings);
		this.queryBuilder = new QueryBuilder();
		
		this.insertSQL = new NativeSQLInsertInto();
		this.insertSQL.setConvention(mappings);
		this.insertBuilder = new InsertBuilder();
	}
		
	public NativeSQL getNativeSQL() {
		return nativeSQL;
	}
	
	public <T> void addMapping(MappingValueProvider<T> mapping) {
		this.mappings.add(mapping);
	}
	
	@Override
	public <T> Query<T> query(Class<T> clazz) {
		return queryBuilder.from(clazz);
	}
	
	@Override
	public <T> T single(Query<T> query) {
		T o = null;
		
		List<T> l = list(query);
		if(l != null && !l.isEmpty()) {
			o = l.get(0);
		}
		
		return o;
	}
	
	@Override
	public <T> List<T> list(Query<T> query) {
		NativeSQLResult result = query.to(nativeSQL);
		
		final List<T> l = new ArrayList<T>();
		
		list(result.sqlValues(), result.values(), (ResultSet rs) -> {
							
			try {
				MappingValueProvider<T> map = (MappingValueProvider<T>) mappings.get(query.getClazz());
				if(map == null) {
					throw new IllegalArgumentException("No mapping for: " + query.getClazz().getName());
				}
				
				T t = query.getClazz().newInstance();
				
				map.fill(t, new ValueProvider() {
					
					@Override
					public <R> R get(String name, Class<R> clazz) {
						try {
							return rs.getObject(name, clazz);
						} catch(Exception e) {
							throw new RuntimeException(e);
						}
					}
					
				});
				
				l.add(t);
				
				return true;
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
			
		});
		
		return l;
	}
	
	@Override
	public <T> Into<T> insert(Class<T> clazz) {
		return insertBuilder.into(clazz);
	}
	
	@Override
	public <T> void execute(Into<T> into) {
		NativeSQLResult result = into.to(insertSQL);
		execute(result.sqlValues(), result.values());
	}
	
	@Override
	public void list(String sql, List<Object> params, ResultSetHandler handler) { 
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = connectionProvider.getConnection()
					.prepareStatement(sql);
			
			addParams(st, params);
			
			rs = st.executeQuery();
			
			forEachHandler(handler, rs);
			
		} catch(Exception e) {
			logger.log(Level.SEVERE, "SQL:" + sql + "\nParams:" + params, e);
			throw new RuntimeException(e);
			
		} finally {
			
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					logger.log(Level.SEVERE, "Error on close ResultSet.", e);
				}
			}
			if(st != null) {
				try {
					st.close();
				} catch(Exception e) {
					logger.log(Level.SEVERE, "Error on close Statement.", e);
				}
			}
		}
	}

	private void addParams(PreparedStatement st, List<Object> params) throws SQLException {
		if(params != null) {
			for(int i = 0; i < params.size(); i++) {
				Object o = params.get(i);
				if(o instanceof Date) {
					st.setTimestamp(i + 1, new java.sql.Timestamp(((java.util.Date)o).getTime()));
				} else if(o instanceof File) {
					try {
						st.setBinaryStream(i + 1, new FileInputStream((File)o));
					} catch(Exception e) {
						throw new RuntimeException(e);
					}
				} else if(o instanceof InputStream) {
					try {
						st.setBinaryStream(i + 1, (InputStream)o);
					} catch(Exception e) {
						throw new RuntimeException(e);
					}				
				} else {
					st.setObject(i + 1, o);
				}
			}
		}
	}
	
	@Override
	public void execute(String sql) {
		this.execute(sql, null, null);
	}
	
	private void execute(String sql, List<Object> params) {
		execute(sql, params, null);
	}

	@Override
	public void execute(String sql, List<Object> params, ResultSetHandler handlerKeys) { 
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = connectionProvider.getConnection()
					.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			
			addParams(st, params);
			
			logger.fine("SQL:" + sql + "\nParams:" + params);
			
			st.executeUpdate();

			if(handlerKeys != null) {
				rs = st.getGeneratedKeys();
				if(rs != null) {
					forEachHandler(handlerKeys, rs);
				}
			}
			
		} catch(Exception e) {
			logger.log(Level.SEVERE, "SQL:" + sql + " params: " + params, e);
			throw new RuntimeException(e);
			
		} finally {
			
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					logger.log(Level.SEVERE, "Error on close ResultSet.", e);
				}
			}
			
			if(st != null) {
				try {
					st.close();
				} catch(Exception e) {
					logger.log(Level.SEVERE, "Error on close Statement.", e);
				}
			}
		}
	}
	
	private void forEachHandler(ResultSetHandler handler, ResultSet rs) throws SQLException {
		while(rs.next()) {
			if(!handler.next(rs)) {
				break;
			}
		}
	}
	
}
