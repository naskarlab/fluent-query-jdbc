# Fluent Query JDBC

Create Queries using only POJO classes and JDBC Drivers.  

See more example in: [Fluent Query](https://github.com/naskarlab/fluent-query)

## Features

* Configuration over code: independence business code of the infrastructure code;
* Intrusive-less: zero or less changes for your code;
* Glue code: it’s only a small and simple classes set;
* Fluent Builder: code complete is your friend!


## Examples

```

Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
ConnectionProvider connectionProvider = new ConnectionProvider() {
	
	@Override
	public Connection getConnection() {
		return conn;
	}
};
DAOImpl dao = new DAOImpl(connectionProvider);

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
		
List<Customer> actual = dao.list(dao.query(Customer.class)
		.where(i -> i.getName()).like("t%"));
			
```

## Usage with Maven

```
<repositories>
	<repository>
	    <id>jitpack.io</id>
	    <url>https://jitpack.io</url>
	</repository>
</repositories>

<dependency>
    <groupId>com.github.naskarlab</groupId>
    <artifactId>fluent-query-jdbc</artifactId>
    <version>0.0.1</version>
</dependency>
```

