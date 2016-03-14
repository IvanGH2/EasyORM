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

import ib.easyorm.annotation.TableInfo;
import ib.easyorm.exception.EasyORMException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class DBMetaData{

	private List<String> columnNames;
	private List<String> columnTypes;
	private List<Integer> nullColumns;
	private List<String> tableNames=new ArrayList<String>();
	private HashMap<String,String>columnInfo=new HashMap<String,String>();
	private DatabaseMetaData databaseMetaData;
	private Connection conn;
	private ResultSet rs;

	<T>DBMetaData(Connection conn, Class<T> target) throws  EasyORMException{
		try{
			this.conn = conn;
			databaseMetaData = conn.getMetaData();
		}catch(SQLException e){
			throw new EasyORMException(e);
		}

	}
	<T>DBMetaData(Connection conn) throws  EasyORMException{
		try{
			this.conn = conn;
			databaseMetaData = conn.getMetaData();
		}catch(SQLException e){
			throw new EasyORMException(e);
		}

	}
	private <T>List<String> getColumns(String tableName, String schemaName) throws  EasyORMException{
		ResultSet rs=null;
		ResultSetMetaData rsmd=null;
		PreparedStatement stmt=null;
		String qualifiedName = (schemaName!=null&&!schemaName.isEmpty())?(schemaName+"."+tableName):tableName;
		try{
			stmt=conn.prepareStatement("select * from "+qualifiedName+" where 0=1");
			rs=stmt.executeQuery();
			rsmd=rs.getMetaData();
			columnNames = new ArrayList<String>(); 
			for(int i=1;i<=rsmd.getColumnCount();i++)
				columnNames.add(rsmd.getColumnLabel(i));	
		}catch(SQLException e){
			throw new EasyORMException(e);
		}
		finally{
			if(rs!=null)
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					throw new EasyORMException(e);
				}
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					throw new EasyORMException(e);
				}
		}
		return columnNames;
	}
	List<String> getColumnNames(String tableName, String schemaName) throws  EasyORMException{
		return getColumns(tableName, schemaName);
	}
	<T>List<String> getColumnNames(Class<T> target) throws  EasyORMException{
		return getColumns(target.getAnnotation(TableInfo.class).tableName(),null);
	}
	List<String> getColumnTypes(String tableName, String schemaName) throws  EasyORMException{
		return getTypes(tableName,schemaName);
	}
	<T>List<String> getColumnTypes(Class<T> target ) throws  EasyORMException{
		return getTypes(target.getAnnotation(TableInfo.class).tableName(),null);
	}
	List<Integer> getNullableColumns(String tableName, String schemaName) throws  EasyORMException{
		return getNullColumns(tableName,schemaName);
	}
	<T>List<Integer> getNullableColumns(Class<T> target ) throws  EasyORMException{
		return getNullColumns(target.getAnnotation(TableInfo.class).tableName(),null);
	}

	 List<String> getTypes(String tableName,String schemaName) throws  EasyORMException{
		ResultSet rs=null;
		ResultSetMetaData rsmd=null;
		PreparedStatement stmt=null;
		
		String qualifiedName = (schemaName!=null&&!schemaName.isEmpty())?(schemaName+"."+tableName):tableName;
		try{
			stmt=conn.prepareStatement("select * from "+qualifiedName+" where 0=1");
			//rs= databaseMetaData.getColumns(null, null, tableName, null);
			rs = stmt.executeQuery();
			rsmd = rs.getMetaData();
			columnTypes = new ArrayList<String>();
			//while(rs.next()){
				for(int i=1;i<=rsmd.getColumnCount();i++){
				columnTypes.add(String.valueOf(rsmd.getColumnType(i)));		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new EasyORMException(e);
		}
		finally{
			if(rs!=null)
				try {
					rs.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
		}
		return columnTypes;
	}
	DatabaseMetaData getMetaData() {

		return databaseMetaData;

	}
	String getDatabaseName() throws  EasyORMException{
		try{
			return databaseMetaData.getDatabaseProductName();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new EasyORMException(e);
		}
	}
	<T>HashMap<String,String> getTableColumnsInfo(Class<T> target) throws  EasyORMException{
		try{		
			rs= databaseMetaData.getColumns(null, null, target.getAnnotation(TableInfo.class).tableName(), null);
			while(rs.next()){
				columnInfo.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
			}
		}catch (SQLException e) {
			throw new EasyORMException(e);
		}
		finally{
			if(rs!=null)
				try {
					rs.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
		}
		return columnInfo;
	}
	List<String> getTableNames(String dbSchema) throws  EasyORMException{
		try{

			rs= databaseMetaData.getTables(null, dbSchema, null, null);
			while(rs.next()){
				tableNames.add(rs.getString(3));
			}
		}catch (SQLException e) {
			throw new EasyORMException(e);
		}
		finally{
			if(rs!=null)
				try {
					rs.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
		}
		return tableNames;
	}
	List<Integer> getNullColumns(String tableName,String schemaName) throws  EasyORMException{
		ResultSet rs=null;
		ResultSetMetaData rsmd=null;
		PreparedStatement stmt=null;
		
		String qualifiedName = (schemaName!=null&&!schemaName.isEmpty())?(schemaName+"."+tableName):tableName;
		try{
			stmt=conn.prepareStatement("select * from "+qualifiedName+" where 0=1");
			rs = stmt.executeQuery();
			rsmd = rs.getMetaData();
			nullColumns = new ArrayList<Integer>();

				for(int i=1;i<=rsmd.getColumnCount();i++){	
				nullColumns.add(rsmd.isNullable(i));		
			}
		} catch (SQLException e) {
			throw new EasyORMException(e);
		}
		finally{
			if(rs!=null)
				try {
					rs.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
		}
		return nullColumns;
	}
	
}