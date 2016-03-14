	/*
	 Copyright 2015 Ivan Balen  
	 This file is part of the EasyORM library.

    The EasyORM library is free software: you can redistribute it and/or modify
    it under the terms of the Lesser GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    The EasyORM library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the Lesser GNU General Public License
    along with The EasyORM library.  If not, see <http://www.gnu.org/licenses/>.*/

package ib.easyorm.db;

public class ConnectionProp{
	private  String jdbcDriver;
	private  String jdbcURL;
	private  String user;
	private  String password;
	private	 String dbDataSource;
	
	public String getJdbcDriver(){
		return jdbcDriver;
	}
	public void setJdbcDriver(String jdbcDriver){
		this.jdbcDriver=jdbcDriver;
	}
	public String getDbURL(){
		return jdbcURL;
	}
	public void setDbURL(String jdbcURL){
		this.jdbcURL=jdbcURL;
	}
	public String getUsername(){
		return user;
	}
	public void setUsername(String username){
		this.user=username;
	}
	public String getPassword(){
		return password;
	}
	public void setPassword(String password){
		this.password=password;
	}
	public String getDataSource(){
		return this.dbDataSource;
	}
	public void setDataSource(String dataSrc){
		this.dbDataSource=dataSrc;
	}
	}