	
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * You can get an instance of this object by calling the static getInstance method. Once you have an 
 * instance, you can start using this library. Since ConnectionPool is implemented as a singleton, you
 * often don't need to explicitly pass the retrieved instance to other methods or constructors because they have
 * access to this single instance of ConnectionPool. However, when using transactions, you will need to explicitly pass the ConnectionPool
 * instance to a DBTransaction object (e.g. new DBTransaction(connPool). 
 * @author XXX
 *
 */
public class ConnectionPool{
	private static String jdbcDriver;
	private static String jdbcURL;
	private static String user;
	private static String password ;
	private int connectionCount = 20;
	private static int initialConnCount = 3;
	private  List<Connection> connections;
	private  List<Connection> addConnections;//additional connections (e.g. if you have two different databases)
	public static ConnectionPool connPool;
	private static boolean loadFromDataSource;
	private static DataSource dataSource;
	private ConnType useConnType = ConnType.BASIC;
	private final static String NEW_LINE = System.getProperty("line.separator");
	static ConnectionPool getInstance(){ 
		return connPool; 
	}
	/**
	 * 
	 * @param propertyFile - a property file that holds information necessary to connect to the database
	 * @param loadFromJndi - 
	 * @return
	 * @throws EasyORMException
	 */
	public static ConnectionPool getInstance(String propertyFile,boolean loadFromJndi) throws EasyORMException{

		ConnectionProp cp=readPropFromFile(propertyFile);

		if(loadFromJndi){
			dataSource=createDatasource(cp.getDataSource());
			loadFromDataSource=true;			 
		}
		return ConnectionPool.createConnectionPool(cp.getJdbcDriver(),cp.getDbURL(), cp.getUsername(), cp.getPassword(),cp.getDataSource());
	}
	/**
	 * 
	 * @param cp - a ConnectionProp instance with data necessary to connect to the database (e.g. the JDBC drive,URL, user/psw)
	 * @return
	 * @throws EasyORMException
	 */
	public static ConnectionPool getInstance(ConnectionProp cp) throws EasyORMException{
		return ConnectionPool.createConnectionPool(cp.getJdbcDriver(),cp.getDbURL(), cp.getUsername(), cp.getPassword(),cp.getDataSource());
	}
	/**
	 * 
	 * @param jndiName - the name of the JNDI resource specifying the database
	 * @return
	 * @throws EasyORMException
	 */
	public static ConnectionPool getInstance(String jndiName) throws EasyORMException{ 

		dataSource=createDatasource(jndiName);
		loadFromDataSource=true;
		return ConnectionPool.createConnectionPool(null,null, null, null,jndiName);
	}
	/**
	 * This method can be used when we need to access an additional database (e.g. when migrating data between databases)
	 * @param jndiName - the name of the JNDI resource specifying the database
	 * @throws EasyORMException
	 */
	public void setAdditionalConnections(String jndiName) throws EasyORMException{
		dataSource=createDatasource(jndiName);
		loadFromDataSource=true;
		setConnectionParameters(null,null, null, null,jndiName);
		initConnections(ConnType.ADDITIONAL);
	}
	/**
	 * This method can be used when we need to access an additional database (e.g. when migrating data between databases)
	 * @param cp - a ConnectionProp instance with data necessary to connect to the database
	 * @throws EasyORMException
	 */
	public void setAdditionalConnections(ConnectionProp cp) throws EasyORMException{
		setConnectionParameters(cp.getJdbcDriver(),cp.getDbURL(), cp.getUsername(), cp.getPassword(),cp.getDataSource());
		initConnections(ConnType.ADDITIONAL);
	}
	/**
	 * This method can be used when we need to access an additional database (e.g. when migrating data between databases)
	 * @param jdbcDriver
	 * @param jdbcURL
	 * @param user
	 * @param password
	 * @throws EasyORMException
	 */
	public void setAdditionalConnections(String jdbcDriver, String jdbcURL, String user, String password) throws EasyORMException{ 
		setConnectionParameters(jdbcDriver,jdbcURL, user, password, null);
		initConnections(ConnType.ADDITIONAL);
	}
	/**
	 * 
	 * @param jdbcDriver
	 * @param jdbcURL
	 * @param user
	 * @param password
	 * @return
	 * @throws EasyORMException
	 */
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
		/*connections= new ArrayList<Connection>();
		for(int i=0;i<initialConnCount;i++)
			connections.add((ConnectionPool.loadFromDataSource)?getConnectionFromDataSource() :getConnection());*/
		initConnections(ConnType.BASIC);
	}
	/**
	 * BASIC - this is the default 
	 * ADDITIONAL - this can be set when creating additional connections for an extra database. This is useful
	 * when migrating data from two (or multiple) databases
	 * @author XXX
	 *
	 */
	public static enum ConnType { BASIC, ADDITIONAL };
	
	private void initConnections(ConnType connType) throws EasyORMException{
		List<Connection> conn = null;//= connType == ConnType.BASIC ? connections : addConnections;
		//if(conn != null && !conn.isEmpty()) return;
		conn = new ArrayList<Connection>();
		for(int i=0;i<initialConnCount;i++)
			conn.add((ConnectionPool.loadFromDataSource) ? getConnectionFromDataSource() : getConnection());
		
		if(connType == ConnType.BASIC){
			connections = conn;
		}else{
			addConnections = conn;
		}
		
	}
	/**
	 * returns a connection type (either BASIC or ADDITIONAL). When working with a single database at a time, you
	 * will not need to use anything else but BASIC (the default)
	 * @return
	 */
	public ConnType getConnType(){
		return useConnType;
	}
	/**
	 * sets the connection type (set this to ConnType.ADDITIONAL only when using multiple databases)
	 * @param connType
	 */
	public void setConnType(ConnType connType){
		useConnType = connType;
	}
	synchronized Connection getAvailableConnection() throws EasyORMException {
		Connection conn=null;
		List<Connection> connections = useConnType == ConnType.BASIC ? this.connections : addConnections;
		int connSize = connections.size();
		if(connSize>0){
			conn=connections.remove(connSize-1);
		}else{
			if(connSize<connectionCount){
				for(int i=0;i<initialConnCount;i++)
					conn=(ConnectionPool.loadFromDataSource) ? getConnectionFromDataSource() : getConnection();
			}else{
					throw new EasyORMException(EasyORMException.CONNECTION_NUM_EXCEEDED);
				}
		}
		return conn;
	}
	
	 public void closeConnections() throws EasyORMException {
	
		List<Connection> connections = useConnType == ConnType.BASIC ? this.connections : addConnections;
		for(Connection conn : connections)
			try {
				conn.close();
			} catch (SQLException e) {
				throw new EasyORMException(e);
			}
		connections.clear();
	}
	synchronized void returnConnection(Connection conn){
		List<Connection> connections = useConnType == ConnType.BASIC ? this.connections : addConnections;
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
	/**
	 * This method reads connection information from the configuration file
	 * @param configFile - the path to the config file
	 * @return
	 * @throws EasyORMException
	 */
	public static ConnectionProp readPropFromFile(String configFile) throws EasyORMException{

		if(configFile==null||"".equals(configFile)){
			return null;
		}
		ConnectionProp cp=new ConnectionProp();
		String completeFile="";
		try{
			InputStream is=new FileInputStream(configFile); 
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
	/**
	 * 
	 * @param sqlTypeName - the name of the type as defined in the database
	 * @param javaTypeName - the name of the type as defined in your code (this is your POJO that implements the SQLData interface)
	 * @throws EasyORMException
	 */
	public void setCustomType(String sqlTypeName, String javaTypeName) throws EasyORMException{
		List<Connection> connections = useConnType == ConnType.BASIC ? this.connections : addConnections;
		for( Connection conn : connections)
			this.setTypeMap(conn, sqlTypeName, javaTypeName);
	}

	private void setTypeMap(Connection conn, String sqlTypeName, String javaTypeName) throws EasyORMException{
		
		try {
			Map<String,Class<?>> typeMap = conn.getTypeMap();
			if(typeMap == null)
				typeMap = new HashMap<String,Class<?>>();
			
			typeMap.put(sqlTypeName, Class.forName(javaTypeName));
			conn.setTypeMap(typeMap);
		} catch (SQLException e) {
			throw new EasyORMException(e);
		} catch (ClassNotFoundException e) {
			throw new EasyORMException(e);
		}		
	}
	/**
	 * This method  returns a database connection from the pool. Use this only if you need to do database 
	 * stuff not currently supported via EasyORM. Call returnBorrowedConnection once you're no longer need the connection so
	 * that it can be reused.
	 * @return
	 * @throws EasyORMException
	 */
	public Connection borrowDbConnection() throws EasyORMException{
		return this.getAvailableConnection();
	}
	/**
	 * This method returns a connection to the connection pool. 
	 * @param conn
	 */
	public void returnBorrowedConnection(Connection conn){
		this.returnConnection(conn);
	}
}