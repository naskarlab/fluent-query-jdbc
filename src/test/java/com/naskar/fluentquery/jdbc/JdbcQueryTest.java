package com.naskar.fluentquery.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.naskar.fluentquery.domain.Customer;
import com.naskar.fluentquery.jdbc.impl.DAOImpl;
import com.naskar.fluentquery.mapping.MappingValueProvider;

public class JdbcQueryTest {
	
	private ConnectionProvider connectionProvider;
	private Connection conn;
	
	private DAOImpl dao;
	
	@Before
	public void setup() throws Exception {
		conn = DriverManager.getConnection("jdbc:h2:mem:test");
		connectionProvider = new ConnectionProvider() {
			
			@Override
			public Connection getConnection() {
				return conn;
			}
		};
		dao = new DAOImpl(connectionProvider);
	}
	
	@After
	public void cleanup() throws Exception {
		conn.close();
	}
	
	@Test
	public void testQuery() {
		dao.execute("CREATE TABLE TB_CUSTOMER("
						+ "CD_CUSTOMER BIGINT PRIMARY KEY, "
						+ "DS_NAME VARCHAR(128), "
						+ "VL_MIN_BALANCE DOUBLE, "
						+ "NU_REGION_CODE INT, "
						+ "DT_CREATED DATE"
						+ ")");
		
		dao.addMapping(new MappingValueProvider<Customer>().
				to(Customer.class, "TB_CUSTOMER")
					.map(i -> i.getId(), "CD_CUSTOMER", (i, v) -> i.setId(v))
					.map(i -> i.getName(), "DS_NAME", (i, v) -> i.setName(v))
					.map(i -> i.getMinBalance(), "VL_MIN_BALANCE", (i, v) -> i.setMinBalance(v))
					.map(i -> i.getRegionCode(), "NU_REGION_CODE", (i, v) -> i.setRegionCode(v))
					.map(i -> i.getCreated(), "DT_CREATED", (i, v) -> i.setCreated(v))	
				);
		
		dao.execute(dao.insert(Customer.class)
				.value(i -> i.getId()).set(1L)
				.value(i -> i.getName()).set("teste1")
				);
		dao.execute(dao.insert(Customer.class)
				.value(i -> i.getId()).set(2L)
				.value(i -> i.getName()).set("teste2")
				);
		
		List<Customer> actual = dao.list(dao.query(Customer.class)
			.where(i -> i.getName()).like("t%"));
		
		Assert.assertEquals(actual.size(), 2);
		
		Assert.assertEquals((long)actual.get(0).getId(), 1L);
		Assert.assertEquals((long)actual.get(1).getId(), 2L);
		
		Assert.assertEquals(actual.get(0).getName(), "teste1");
		Assert.assertEquals(actual.get(1).getName(), "teste2");
	}

}
