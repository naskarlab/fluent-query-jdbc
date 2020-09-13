package com.naskar.fluentquery.jdbc.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.naskar.fluentquery.Delete;
import com.naskar.fluentquery.DeleteBuilder;
import com.naskar.fluentquery.InsertBuilder;
import com.naskar.fluentquery.Into;
import com.naskar.fluentquery.Query;
import com.naskar.fluentquery.QueryBuilder;
import com.naskar.fluentquery.Update;
import com.naskar.fluentquery.UpdateBuilder;
import com.naskar.fluentquery.binder.BinderSQL;
import com.naskar.fluentquery.binder.BinderSQLBuilder;
import com.naskar.fluentquery.conventions.MappingConvention;
import com.naskar.fluentquery.converters.NativeSQL;
import com.naskar.fluentquery.converters.NativeSQLDelete;
import com.naskar.fluentquery.converters.NativeSQLInsertInto;
import com.naskar.fluentquery.converters.NativeSQLResult;
import com.naskar.fluentquery.converters.NativeSQLUpdate;
import com.naskar.fluentquery.impl.MethodRecordProxy;
import com.naskar.fluentquery.impl.TypeUtils;
import com.naskar.fluentquery.jdbc.ConnectionProvider;
import com.naskar.fluentquery.jdbc.DAO;
import com.naskar.fluentquery.jdbc.Inserter;
import com.naskar.fluentquery.jdbc.PreparedStatementHandler;
import com.naskar.fluentquery.jdbc.ResultHandler;
import com.naskar.fluentquery.jdbc.ResultSetHandler;
import com.naskar.fluentquery.mapping.MappingValueProvider;
import com.naskar.fluentquery.mapping.MappingValueProvider.ValueProvider;

public class DAOImpl implements DAO {
	
	private static final Logger logger = Logger.getLogger(DAOImpl.class.getName());
	
	private static final List<Integer> BINARY_TYPES = Arrays.asList(
			Types.BINARY, Types.LONGVARBINARY, Types.VARBINARY 
	);
	
	private ConnectionProvider connectionProvider;
	private MappingConvention mappings;
	private ResultSetValueConverter resultSetConverter;
	private PreparedStatementHandler defaultStatementHandler; 
	
	private NativeSQL nativeSQL;	
	private QueryBuilder queryBuilder;
	
	private NativeSQLInsertInto insertSQL;
	private InsertBuilder insertBuilder;
	
	private NativeSQLUpdate updateSQL;
	private NativeSQLUpdate updateSQLOnConflict;
	private UpdateBuilder updateBuilder;
	
	private NativeSQLDelete deleteSQL;
	private DeleteBuilder deleteBuilder;
	
	private BinderSQLBuilder binderBuilder;
	
	public DAOImpl(ConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
		
		this.mappings = new MappingConvention();
		
		this.nativeSQL = new NativeSQL();
		this.nativeSQL.setConvention(mappings);
		this.queryBuilder = new QueryBuilder();
		
		this.insertSQL = new NativeSQLInsertInto();
		this.insertSQL.setConvention(mappings);
		this.insertBuilder = new InsertBuilder();
		
		this.updateSQL = new NativeSQLUpdate();
		this.updateSQL.setConvention(mappings);
		this.updateBuilder = new UpdateBuilder();
		
		this.updateSQLOnConflict = new NativeSQLUpdate();
		this.updateSQLOnConflict.setConvention(mappings);
		this.updateSQLOnConflict.setWithoutAlias(true);
		this.updateSQLOnConflict.setWithoutTableName(true);
		
		this.deleteSQL = new NativeSQLDelete();
		this.deleteSQL.setConvention(mappings);
		this.deleteBuilder = new DeleteBuilder();
		
		this.binderBuilder = new BinderSQLBuilder();
	}
	
	public NativeSQL getNativeSQL() {
		return nativeSQL;
	}
	
	public NativeSQLInsertInto getInsertSQL() {
		return insertSQL;
	}
	
	public NativeSQLUpdate getUpdateSQL() {
		return updateSQL;
	}
	
	public NativeSQLDelete getDeleteSQL() {
		return deleteSQL;
	}
	
	public <T> void addMapping(MappingValueProvider<T> mapping) {
		this.mappings.add(mapping);
	}
	
	public void setResultSetValueConverter(ResultSetValueConverter resultSetConverter) {
		this.resultSetConverter = resultSetConverter;
	}
	
	public void setDefaultStatementHandler(PreparedStatementHandler defaultStatementHandler) {
		this.defaultStatementHandler = defaultStatementHandler;
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
		return list(query, (PreparedStatementHandler)null);
	}
	
	@Override
	public <T> void list(Query<T> query, Function<T, Boolean> tHandler) {
		list(query, tHandler, (PreparedStatementHandler)null);
	}
	
	@Override
	public <T> List<T> list(Query<T> query, PreparedStatementHandler stHandler) {
		final List<T> l = new ArrayList<T>();
		
		list(query, (t) -> {
			l.add(t);
			return true;
		}, stHandler);
		
		return log(l);
	}
	
	@Override
	public <T> void list(Query<T> query, Function<T, Boolean> tHandler, PreparedStatementHandler stHandler) {
		NativeSQLResult result = query.to(nativeSQL);
		
		Map<String, Integer> columns = new HashMap<String, Integer>();
		
		list(result.sqlValues(), result.values(), (ResultSet rs) -> {
							
			try {
				MappingValueProvider<T> map = (MappingValueProvider<T>) mappings.get(query.getClazz());
				if(map == null) {
					throw new IllegalArgumentException("No mapping for: " + query.getClazz().getName());
				}
				
				T t = query.getClazz().newInstance();
				
				if(columns.isEmpty()) {
					columns.putAll(getColumns(rs));
				}
								
				map.fill(t, new ValueProvider() {
					
					@Override
					public <R> R get(String name, Class<R> clazz) {
						try {
							if(!columns.keySet().contains(name.toLowerCase())) {
								return null;
							}
							
							if(resultSetConverter == null) {
								return getValue(columns, rs, name, clazz);
								
							} else {
								return resultSetConverter.converter(rs, name, clazz);
							}
						} catch(Exception e) {
							throw new RuntimeException(
								"ERROR on converter: " + name + " from " + clazz, e);
						}
					}

				});
				
				return tHandler.apply(t);
				
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
			
		}, stHandler);
	}
	
	@SuppressWarnings("unchecked")
	private <R> R getValue(Map<String, Integer> columns, 
			ResultSet rs, String name, Class<R> clazz) throws SQLException, IOException {
		
		if(BINARY_TYPES.contains(columns.get(name.toLowerCase()))) {
			if(InputStream.class.isAssignableFrom(clazz)) {
				return (R) rs.getBinaryStream(name);
			} else {
				return (R) getBytes(rs.getBinaryStream(name));
			}
		} else {
			return rs.getObject(name, clazz);
		}
		
	}

	private static byte[] getBytes(InputStream is) throws IOException {
	    ByteArrayOutputStream os = new ByteArrayOutputStream();
	    
	    if(is != null) {
		    byte[] buffer = new byte[4096];
		    for (int len = is.read(buffer); len != -1; len = is.read(buffer)) { 
		        os.write(buffer, 0, len);
		    }
	    }
	    
	    return os.toByteArray();
	}
	
	private Map<String, Integer> getColumns(ResultSet rs) throws SQLException {
		Map<String, Integer> m = new HashMap<String, Integer>();
		ResultSetMetaData rsmd = rs.getMetaData();
		
		for(int i = 1; i <= rsmd.getColumnCount(); i++) {
			m.put(rsmd.getColumnName(i).toLowerCase(), rsmd.getColumnType(i));
		}
		
		return m;
	}
	
	@Override
	public <T> Into<T> insert(Class<T> clazz) {
		return insertBuilder.into(clazz);
	}
	
	@Override
	public <T> Update<T> update(Class<T> clazz) {
		return updateBuilder.entity(clazz);
	}
	
	@Override
	public <T> Delete<T> delete(Class<T> clazz) {
		return deleteBuilder.entity(clazz);
	}
	
	@Override
	public <P, T> Inserter<P> binder(Class<P> clazz, Function<BinderSQL<P>, Into<T>> into) {
		BinderSQL<P> binder = binderBuilder.from(clazz);
		binder.configure(into.apply(binder).to(insertSQL));
		return (P p) -> {
			NativeSQLResult result = binder.bind(p);
			execute(result.sqlValues(), result.values());
		};
	}
	
	@Override
	public <T, R> List<R> list(Query<T> query, Class<R> clazz) {
		ClassListHandler<R> handler = new ClassListHandler<R>(clazz);
		list(query, handler);
		return log(handler.getList());
	}
	
	@Override
	public <T> void list(Query<T> query, ResultSetHandler handler) {
		NativeSQLResult result = query.to(nativeSQL);
		list(result.sqlValues(), result.values(), handler);
	}
	
	@Override
	public <T> void listWith(Query<T> query, ResultSetHandler handler, PreparedStatementHandler stHandler) {
		NativeSQLResult result = query.to(nativeSQL);
		list(result.sqlValues(), result.values(), handler, stHandler);
	}
	
	@Override
	public <T> void list(String sql, List<Object> params, Class<T> clazz, ResultHandler<T> action) {
		list(sql, params, new ClassHandler<T>(clazz, action), null);
	}
	
	@Override
	public void list(String sql, List<Object> params, ResultSetHandler handler) {
		list(sql, params, handler, null);
	}
	
	@Override
	public void list(String sql, List<Object> params, 
			ResultSetHandler handler, PreparedStatementHandler stHandler) { 
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = connectionProvider.getConnection()
					.prepareStatement(sql);
			
			addParams(st, params);
			
			log(sql, params);
			
			if(defaultStatementHandler != null) {
				defaultStatementHandler.handle(st);
			}
			
			if(stHandler != null) {
				stHandler.handle(st);
			}
			
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

	private void log(String sql, List<Object> params) {
		if(logger.isLoggable(Level.INFO)) {
			logger.info("SQL:" + sql + "\nParams:" + params);
		}
	}
	
	private <R> List<R> log(List<R> l) {
		if(logger.isLoggable(Level.INFO)) {
			logger.info("SQL: Count: " + l.size());
		}
		return l;
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
	public <T> void execute(Into<T> into) {
		NativeSQLResult result = into.to(insertSQL);
		execute(result.sqlValues(), result.values());
	}
	
	@Override
	public <T> void execute(Into<T> into, ResultSetHandler handlerKeys) {
		NativeSQLResult result = into.to(insertSQL);
		execute(result.sqlValues(), result.values(), handlerKeys);
	}
	
	@Override
	public <T, R> void executeOnConflict(Into<T> into, Update<T> update, Function<T, R> property) {
		
		String name = getColumnName(into.getClazz(), property);
		
		NativeSQLResult resultInsert = into.to(insertSQL);
		NativeSQLResult resultUpdate = update.to(updateSQLOnConflict);
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(resultInsert.sqlValues());
		sb.append(" on conflict (" + name + ") do ");
		sb.append(resultUpdate.sqlValues());
		
		List<Object> values = new ArrayList<Object>(resultInsert.values());
		values.addAll(resultUpdate.values());
		
		execute(sb.toString(), values);
	}
	
	@Override
	public <T, R> void executeOnConflictDoNothing(Into<T> into, Function<T, R> property) {
		
		String name = getColumnName(into.getClazz(), property);
		
		NativeSQLResult resultInsert = into.to(insertSQL);
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(resultInsert.sqlValues());
		sb.append(" on conflict (" + name + ") do nothing ");
		
		execute(sb.toString(), resultInsert.values());
	}
	
	@Override
	public <T> void execute(Update<T> update) {
		NativeSQLResult result = update.to(updateSQL);
		execute(result.sqlValues(), result.values());
	}
	
	@Override
	public <T> void execute(Delete<T> delete) {
		NativeSQLResult result = delete.to(deleteSQL);
		execute(result.sqlValues(), result.values());
	}
	
	@Override
	public void execute(String sql) {
		this.execute(sql, null, null);
	}
	
	@Override
	public void execute(String sql, List<Object> params) {
		this.execute(sql, params, null);
	}
	
	@Override
	public void execute(String sql, List<Object> params, ResultSetHandler handlerKeys) { 
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = connectionProvider.getConnection()
					.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			
			addParams(st, params);
			
			log(sql, params);
			
			int count = st.executeUpdate();
			
			logger.info("SQL: Count: " + count);

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
	
	private <T, R> String getColumnName(Class<T> clazz, Function<T, R> property) {
		MethodRecordProxy<T> proxy = TypeUtils.createProxy(clazz);
		property.apply(proxy.getProxy());
		Method m = proxy.getCalledMethod();
		return mappings.getNameFromMethod(m);
	}
	
}
