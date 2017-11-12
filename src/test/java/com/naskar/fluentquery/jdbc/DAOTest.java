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
import com.naskar.fluentquery.model.RegionSummary;

public class DAOTest {
	
	private ConnectionProvider connectionProvider;
	private Connection conn;
	
	private DAOImpl dao;
	
	@Before
	public void setup() throws Exception {
		
		Driver.class.getName();
		
		conn = DriverManager.getConnection("jdbc:h2:mem:test");
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
	public void testSuccessInsertQuery() {
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
	
	@Test
	public void testSuccessClassResult() {
		dao.execute(dao.insert(Customer.class)
				.value(i -> i.getId()).set(1L)
				.value(i -> i.getRegionCode()).set(1L)
				.value(i -> i.getBalance()).set(100.0)
				);
		dao.execute(dao.insert(Customer.class)
				.value(i -> i.getId()).set(2L)
				.value(i -> i.getRegionCode()).set(1L)
				.value(i -> i.getBalance()).set(100.0)
				);
		dao.execute(dao.insert(Customer.class)
				.value(i -> i.getId()).set(3L)
				.value(i -> i.getRegionCode()).set(2L)
				.value(i -> i.getBalance()).set(300.0)
				);
		
		List<RegionSummary> actual = dao.list(dao.query(Customer.class)
			.select(i -> i.getRegionCode(), s -> s.func(c -> c, "region"))
			.select(x -> x.getBalance(), s -> s.func(c -> "sum(" + c + ")", "balance"))
			.groupBy(i -> i.getRegionCode()), 
			RegionSummary.class);
		
		Assert.assertEquals(actual.size(), 2);
		
		Assert.assertEquals((long)actual.get(0).getRegion(), 1L);
		Assert.assertEquals((long)actual.get(1).getRegion(), 2L);
		
		Assert.assertEquals(actual.get(0).getBalance().intValue(), 200);
		Assert.assertEquals(actual.get(1).getBalance().intValue(), 300);
	}

	private void createCustomer() {
		dao.execute("CREATE TABLE TB_CUSTOMER("
						+ "CD_CUSTOMER BIGINT PRIMARY KEY, "
						+ "DS_NAME VARCHAR(128), "
						+ "VL_BALANCE DOUBLE, "
						+ "NU_REGION_CODE INT, "
						+ "DT_CREATED DATE"
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
