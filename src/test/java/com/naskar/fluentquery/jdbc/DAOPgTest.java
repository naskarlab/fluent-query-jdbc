package com.naskar.fluentquery.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.h2.Driver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.naskar.fluentquery.domain.Customer;
import com.naskar.fluentquery.jdbc.impl.DAOImpl;
import com.naskar.fluentquery.mapping.MappingValueProvider;

public class DAOPgTest {
	
	private ConnectionProvider connectionProvider;
	private Connection conn;
	
	private DAOImpl dao;
	
	@Before
	public void setup() throws Exception {
		
		Driver.class.getName();
		
		conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "postgres", "123");
		connectionProvider = new ConnectionProvider() {
			
			@Override
			public Connection getConnection() {
				return conn;
			}
			
		};
		dao = new DAOImpl(connectionProvider);
		
		createCustomer();
	}
	
	@After
	public void cleanup() throws Exception {
		conn.close();
	}
	
	@Test
	public void testSuccessOnConflict() {
		Double expectedBalance = 100.0;
		insert(expectedBalance);
		
		List<Customer> actual = dao.list(dao.query(Customer.class)
				.where(i -> i.getId()).eq(100L));
		
		insert(expectedBalance);
		
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals(expectedBalance, actual.get(0).getBalance());
		
		//
		
		expectedBalance = 200.0;
		insert(expectedBalance);
		
		actual = dao.list(dao.query(Customer.class)
				.where(i -> i.getId()).eq(100L));
		
		insert(expectedBalance);
		
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals(expectedBalance, actual.get(0).getBalance());
	}

	private void insert(Double balance) {
		dao.executeOnConflict(
			dao.insert(Customer.class)
				.value(i -> i.getId()).set(100L)
				.value(i -> i.getRegionCode()).set(1000L)
				.value(i -> i.getBalance()).set(balance)
			, 
			dao.update(Customer.class)
			.value(i -> i.getRegionCode()).set(1000L)
			.value(i -> i.getBalance()).set(balance),
			x -> x.getId()
		);
	}

	private void createCustomer() {
		dao.execute("DROP TABLE IF EXISTS TB_CUSTOMER");
		
		dao.execute("CREATE TABLE TB_CUSTOMER("
						+ "CD_CUSTOMER BIGINT PRIMARY KEY, "
						+ "DS_NAME VARCHAR(128), "
						+ "VL_BALANCE FLOAT8, "
						+ "NU_REGION_CODE INT8, "
						+ "DT_CREATED TIMESTAMP"
						+ ")");
		
		dao.addMapping(new MappingValueProvider<Customer>().
				to(Customer.class, "TB_CUSTOMER")
					.map(i -> i.getId(), "CD_CUSTOMER", (i, v) -> i.setId(v))
					.map(i -> i.getName(), "DS_NAME", (i, v) -> i.setName(v))
					.map(i -> i.getBalance(), "VL_BALANCE", (i, v) -> i.setBalance(v))
					.map(i -> i.getRegionCode(), "NU_REGION_CODE", (i, v) -> i.setRegionCode(v))
					.map(i -> i.getCreated(), "DT_CREATED", (i, v) -> i.setCreated(v))	
				);
	}

}
