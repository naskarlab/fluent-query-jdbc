package com.naskar.fluentquery.domain;

import java.util.Date;

public class Customer {
	
	private Long id;
	private String name;
	private Long regionCode;
	private Double minBalance;
	private Date created;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getRegionCode() {
		return regionCode;
	}
	public void setRegionCode(Long regionCode) {
		this.regionCode = regionCode;
	}
	public Double getMinBalance() {
		return minBalance;
	}
	public void setMinBalance(Double minBalance) {
		this.minBalance = minBalance;
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}
	
}
