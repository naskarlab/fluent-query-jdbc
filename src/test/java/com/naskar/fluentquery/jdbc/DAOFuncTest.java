package com.naskar.fluentquery.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.h2.Driver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.naskar.fluentquery.domain.Account;
import com.naskar.fluentquery.domain.Customer;
import com.naskar.fluentquery.jdbc.impl.DAOImpl;
import com.naskar.fluentquery.mapping.MappingValueProvider;

public class DAOFuncTest {
	
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
		createAccount();
	}
	
	@After
	public void cleanup() throws Exception {
		conn.close();
	}
	
	@Test
	public void testSuccessInsertQuery() {
		// Arrange
		Inserter<Customer> inserterCustomer = 
				dao.binder(Customer.class, (b) -> 
					dao.insert(Customer.class)
						.value(i -> i.getId()).set(b.get(i -> i.getId()))
						.value(i -> i.getName()).set(b.get(i -> i.getName()))
				);
		
		Inserter<Account> inserterAccount = 
				dao.binder(Account.class, (b) -> 
					dao.insert(Account.class)
						.value(i -> i.getId()).set(b.get(i -> i.getId()))
						.value(i -> i.getBalance()).set(b.get(i -> i.getBalance()))
				);
		
		// Act
		inserterCustomer.insert(new Customer() {{ setId(1L); setName("customer1"); }});
		inserterCustomer.insert(new Customer() {{ setId(2L); setName("customer2"); }});
		
		inserterAccount.insert(new Account() {{ setId(1L); setBalance(1.0); }});
		inserterAccount.insert(new Account() {{ setId(2L); setBalance(2.0); }});
		
		List<Customer> actualCustomer = dao.list(dao.query(Customer.class)
				.where(i -> i.getName()).like("c%"));
		
		List<Account> actualAccount = dao.list(dao.query(Account.class)
				.where(i -> i.getBalance()).gt(0.0));
				
		// Assert
		Assert.assertEquals(actualCustomer.size(), 2);
		
		Assert.assertEquals((long)actualCustomer.get(0).getId(), 1L);
		Assert.assertEquals((long)actualCustomer.get(1).getId(), 2L);
		
		Assert.assertEquals(actualCustomer.get(0).getName(), "customer1");
		Assert.assertEquals(actualCustomer.get(1).getName(), "customer2");
		
		Assert.assertEquals(actualAccount.size(), 2);
		
		Assert.assertEquals((long)actualAccount.get(0).getId(), 1L);
		Assert.assertEquals((long)actualAccount.get(1).getId(), 2L);
		
		Assert.assertEquals(actualAccount.get(0).getBalance(), new Double(1.0));
		Assert.assertEquals(actualAccount.get(1).getBalance(), new Double(2.0));
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
	
	private void createAccount() {
		dao.execute("CREATE TABLE TB_ACCOUNT("
						+ "CD_ACCOUNT BIGINT PRIMARY KEY, "
						+ "VL_BALANCE DOUBLE "
						+ ")");
		
		dao.addMapping(new MappingValueProvider<Account>().
				to(Account.class, "TB_ACCOUNT")
					.map(i -> i.getId(), "CD_ACCOUNT", (i, v) -> i.setId(v))
					.map(i -> i.getBalance(), "VL_BALANCE", (i, v) -> i.setBalance(v))
				);
	}

}
