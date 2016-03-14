	
/*	Copyright 2015 Ivan Balen 
 	This file is part of the EasyORM library.

    The EasyORM library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    The EasyORM library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the Lesser GNU General Public License
    along with The EasyORM library.  If not, see <http://www.gnu.org/licenses/>.*/

package ib.easyorm.db;

import ib.easyorm.exception.EasyORMException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;


public class ConnectionPool{
	private static String jdbcDriver;
	private static String jdbcURL;
	private static String user;
	private static String password ;
	private int connectionCount=20;
	private static int initialConnCount=3;
	private  List<Connection> connections;
	public static ConnectionPool connPool;
	private static boolean loadFromDataSource;
	private static DataSource dataSource;
	private final static String NEW_LINE = System.getProperty("line.separator");
	static ConnectionPool getInstance(){ 
		return connPool; 
	}
	public static ConnectionPool getInstance(String propertyFile,boolean loadFromJndi) throws EasyORMException{

		ConnectionProp cp=readPropFromFile(propertyFile);

		if(loadFromJndi){
			dataSource=createDatasource(cp.getDataSource());
			loadFromDataSource=true;			 
		}
		return ConnectionPool.createConnectionPool(cp.getJdbcDriver(),cp.getDbURL(), cp.getUsername(), cp.getPassword(),cp.getDataSource());
	}
	public static ConnectionPool getInstance(ConnectionProp cp) throws EasyORMException{
		return ConnectionPool.createConnectionPool(cp.getJdbcDriver(),cp.getDbURL(), cp.getUsername(), cp.getPassword(),cp.getDataSource());
	}
	public static ConnectionPool getInstance(String jndiName) throws EasyORMException{ 

		dataSource=createDatasource(jndiName);
		loadFromDataSource=true;
		return ConnectionPool.createConnectionPool(null,null, null, null,jndiName);
	}
	public static ConnectionPool getInstance(String jdbcDriver, String jdbcURL, String user, String password) throws EasyORMException{ 

		return ConnectionPool.createConnectionPool(jdbcDriver,jdbcURL, user, password,null);
	}
	private static ConnectionPool createConnectionPool(String jdbcDriver, String jdbcURL, String user, String password,String dbDataSource) throws EasyORMException{
		setConnectionParameters(jdbcDriver,jdbcURL, user, password,null);
		if(connPool==null)		
			connPool=new ConnectionPool();

		return connPool; 
	}
	private static void setConnectionParameters(String jdbcDriver, String jdbcURL, String user, String password,String dbDataSource){
		ConnectionPool.jdbcDriver=jdbcDriver;
		ConnectionPool.jdbcURL=jdbcURL;
		ConnectionPool.user=user;
		ConnectionPool.password=password;
		//ConnectionPool.jndiDataSource=dbDataSource;
	}
	private ConnectionPool() throws EasyORMException{
		connections= new ArrayList<Connection>();
		for(int i=0;i<initialConnCount;i++)
			connections.add((ConnectionPool.loadFromDataSource)?getConnectionFromDataSource() :getConnection());
	}
	synchronized Connection getAvailableConnection() throws EasyORMException {
		Connection conn=null;
		int connSize = connections.size();
		if(connSize>0){
			conn=connections.remove(connSize-1);
		}else{
			if(connSize<connectionCount){
				for(int i=0;i<initialConnCount;i++)
					conn=(ConnectionPool.loadFromDataSource)?getConnectionFromDataSource() :getConnection();
			}else{
					throw new EasyORMException(EasyORMException.CONNECTION_NUM_EXCEEDED);
				}
		}
		return conn;
	}
	
	public void closeConnections() throws EasyORMException {
	
		for(Connection conn : connections)
			try {
				conn.close();
			} catch (SQLException e) {
				throw new EasyORMException(e);
			}
		connections.clear();
	}
	synchronized void returnConnection(Connection conn){
		connections.add(conn);
	}
	private Connection getConnection() throws EasyORMException {

		Connection conn=null;
		try {
			Class.forName(jdbcDriver);
			conn = DriverManager.getConnection(jdbcURL, user, password);
		} catch (ClassNotFoundException e) {
			throw new EasyORMException(e);
		} catch (SQLException e) {
			throw new EasyORMException(e);
		}

		return conn;
	}
	private  Connection getConnectionFromDataSource() throws EasyORMException  {
		try{
			return dataSource.getConnection();
		}catch(SQLException e){
			throw new EasyORMException(e);
		}
	}
	public static ConnectionProp readPropFromFile(String config) throws EasyORMException{

		if(config==null||"".equals(config)){
			return null;
		}
		ConnectionProp cp=new ConnectionProp();
		String completeFile="";
		try{
			InputStream is=new FileInputStream(config); 
			InputStreamReader isr=new InputStreamReader(is);
			BufferedReader br=new BufferedReader(isr);
			String line;
			while ((line=br.readLine())!=null){

				completeFile+=line+NEW_LINE;
			}
			br.close(); 
		}       
		catch (Exception e){
			throw new EasyORMException(e);
		}
		completeFile=completeFile.substring(1);
		String[] props=completeFile.split("[=\n]");
		for(int i=0;i<props.length-1;i+=2){
			String s=props[i].trim();
			if(s.equals("dbUrl"))
				cp.setDbURL(props[i+1]);
			else if(s.equals("dbDriverName"))
				cp.setJdbcDriver(props[i+1]);
			else if(s.equals("dbUserName"))
				cp.setUsername(props[i+1]);
			else if(s.equals("dbPassword"))
				cp.setPassword(props[i+1]);
			else if(s.equals("dbDataSource"))
				cp.setDataSource(props[i+1]);
		}
		return cp;

	}
	public void setNumberOfConnections(int count){
		this.connectionCount=count;
	}
	public int getNumberOfConnections(){
		return connectionCount;
	}
	public static void setInitialNumberOfConnections(int count){
		initialConnCount=count;
	}
	public static int getInitialNumberOfConnections(){
		return initialConnCount;
	}
	private static DataSource createDatasource(String jndiDb)throws EasyORMException{
		InitialContext initCtx=null;
		try {
			initCtx = new InitialContext();
			return (DataSource)initCtx.lookup("java:comp/env/"+jndiDb);
		} catch (NamingException e) {
			throw new EasyORMException(e);
		}finally{
			if(initCtx!=null){
				try {
					initCtx.close();
				} catch (NamingException e) {
					throw new EasyORMException(e);
				}
			}
		}
	}
}