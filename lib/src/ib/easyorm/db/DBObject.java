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
import ib.easyorm.annotation.util.AnnotationUtil;
import ib.easyorm.exception.EasyORMException;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class DBObject implements Serializable{

	private Connection conn;
	private List<String> paramsForUpdate=new ArrayList<String>();
	private Map<String, Object> cachedResults=new HashMap<String, Object>();
	private static String stringEncoding;
	private List<Object> params;

	public void setEncoding(String encoding){
		stringEncoding=encoding;
	}
	public String getEncoding(){
		return stringEncoding;
	}
	private String dbSchema;


	public DBObject() throws EasyORMException{
		conn=ConnectionPool.getInstance().getAvailableConnection();	
	}

	protected <T>DBObject(Class<T>target,Object enclosingCls) {

		cachedResults=((DBObject)enclosingCls).cachedResults;
	}
	public DBObject(ResultSet rs) throws EasyORMException{
		try{
			for (int i=1;i<=rs.getMetaData().getColumnCount();i++) {
				Object obj=rs.getObject(i);
				/*if(obj instanceof Clob){
					cachedResults.put(rs.getMetaData().getColumnName(i), clobToString((Clob)obj));
				}else if(obj instanceof Blob){
					cachedResults.put(rs.getMetaData().getColumnName(i), blobToBytes((Blob)obj));
				}else */
				if(obj instanceof String && stringEncoding != null ) {
					try{
						cachedResults.put(rs.getMetaData().getColumnName(i), new String(((String)obj).getBytes(stringEncoding),stringEncoding));
					}catch(UnsupportedEncodingException e){
						throw new EasyORMException(e);
					}
				}else{
					cachedResults.put(rs.getMetaData().getColumnName(i), obj);
				}

			}
		}catch (Exception e) {
			throw new EasyORMException(e);
		}
	}
	/*public DBObject(ResultSet rs) throws EasyORMException{
		try{
			for (int i=1;i<=rs.getMetaData().getColumnCount();i++) {
				Object obj=rs.getObject(i);
				if(stringEncoding!=null&&obj!=null&&obj instanceof String){
					if(obj!=null&&obj instanceof String){
						try{
							cachedResults.put(rs.getMetaData().getColumnName(i), new String(((String)obj).getBytes(),stringEncoding));
						}catch(UnsupportedEncodingException e){
							throw new EasyORMException(e);
						}
					}
				}else{
					cachedResults.put(rs.getMetaData().getColumnName(i), obj);
				}
			}
		}catch (Exception e) {
			throw new EasyORMException(e);
		}
	}*/
	public DBObject(Connection conn){
		this.conn=conn;		
	}
	public DBObject(DBTransaction dbTrx) throws  EasyORMException{

		conn=dbTrx.getTransactionConnection();
	}
	void setConnection( Connection conn){
		this.conn=conn;
	}
	public String getDbSchema(){
		return dbSchema;
	}
	public void setDbSchema(String schemaName){
		dbSchema=schemaName;
	}
	public <T>Object createChildObject(Class<T> target) throws  EasyORMException{
		try{
			return target.getConstructor(Object.class).newInstance(this);
		}
		catch(Exception e){
			throw new EasyORMException(target.getName()+EasyORMException.CONSTRUCTOR_ARGS_MISSING_OBJECT);
		}
	}
	protected Object getValue(String name){		

		Object obj=cachedResults.get(name);
		if(obj==null) obj=cachedResults.get(name.toUpperCase());
		return obj;
	}

	protected void setValue(String name,Object value) {
		if(value!=null){
			if(!paramsForUpdate.contains(name)){
				paramsForUpdate.add(name);
			}
			if(stringEncoding!=null&&value instanceof String){
				try {
					cachedResults.put(name, new String(((String)value).getBytes(stringEncoding),stringEncoding));
				} catch (UnsupportedEncodingException e) {
					//throw new EasyORMException(e);
				}
			}else{
				cachedResults.put(name,value);
			}
		}
	}
	public static void executeDDLQuery(String ddlSql) throws EasyORMException{

		try{			  
			Connection conn = ConnectionPool.getInstance().getAvailableConnection();
			Statement stmt = conn.createStatement();
			stmt.execute(ddlSql);
		}catch(SQLException sqle){
			throw new EasyORMException(sqle);
		}
	}
	public static int[] executeBatchUpdate(String sqlStatement,List<Object[]> params) throws EasyORMException{

		int[] batchResults=null;
		PreparedStatement stmt=null;
		Connection conn = ConnectionPool.getInstance().getAvailableConnection();

		for (int i=1; i<=params.size();i++){
			try{
				if(stmt==null)
					stmt = conn.prepareStatement(sqlStatement);
				Object[] vals = params.get(i-1);
				for(int j=1;j<=vals.length;j++)   
					stmt.setObject(j, vals[j-1]);{
						stmt.addBatch();
					}

			}catch(SQLException sqle){
				throw new EasyORMException(sqle);
			}
		}

		try {

			batchResults = stmt.executeBatch();
		} catch (SQLException sqle) {
			throw new EasyORMException(sqle);
		}finally{
			if(stmt!=null)
				try {
					stmt.close();

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					throw new EasyORMException(e);
				}
			if(conn!=null)
				ConnectionPool.getInstance().returnConnection(conn);

		}
		return batchResults;
	}
	public static int[] executeBatchUpdate(List<String> sqlStatements) throws EasyORMException{

		int[] batchResults=null;
		Statement stmt=null;
		Connection conn = ConnectionPool.getInstance().getAvailableConnection();

		for (int i=0; i<sqlStatements.size();i++){
			try{
				if(stmt==null)
					stmt = conn.createStatement();
				stmt.addBatch(sqlStatements.get(i));

			}catch(SQLException sqle){
				throw new EasyORMException(sqle);
			}
		}          
		try {
			batchResults = stmt.executeBatch();
		} catch (SQLException sqle) {
			throw new EasyORMException(sqle);
		}finally{
			if(stmt!=null)
				try {
					stmt.close();

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					throw new EasyORMException(e);
				}
			if(conn!=null)
				ConnectionPool.getInstance().returnConnection(conn);

		}
		return batchResults;
	}
	public int insertAndReturnGenKey() throws  EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		Object id=null;
		List<Object> params=new ArrayList<Object>();	
		String names="";
		String values="";
		String query="";
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		if(paramsForUpdate.size()>0){
			String name= paramsForUpdate.get(0);
			names+=name;
			values+="?";
			params.add(cachedResults.get(name));
			for (int i=1;i<paramsForUpdate.size();i++) 
			{
				name= paramsForUpdate.get(i);
				names+=","+name;
				values+=","+"?";
				params.add(cachedResults.get(name));

			}
			query="insert into "+qualifiedName+" ("+names+") values ("+values+")";
		}else{
			throw new EasyORMException(EasyORMException.NO_UPDATE_PARAMETERS);
		}
		if(query.length()>0)
		{
			id=this.doInsertReturnGenKey(query, params);
			if(id instanceof Number)
				id=((Number)id).intValue();
			setValue(getClass().getAnnotation(TableInfo.class).idColumnName(), id);
		}
		paramsForUpdate.clear();
		return id!=null?((Integer)id).intValue():-1;
	}
	public int insert() throws  EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		int result = -1;
		List<Object> params=new ArrayList<Object>();	
		String names="";
		String values="";
		String query="";
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		if(paramsForUpdate.size()>0){
			String name= paramsForUpdate.get(0);
			names+=name;
			values+="?";
			params.add(cachedResults.get(name));
			for (int i=1;i<paramsForUpdate.size();i++) 
			{
				name= paramsForUpdate.get(i);
				names+=","+name;
				values+=","+"?";
				params.add(cachedResults.get(name));

			}
			query="insert into "+qualifiedName+" ("+names+") values ("+values+")";
		}else{
			throw new EasyORMException(EasyORMException.NO_UPDATE_PARAMETERS);
		}
		if(query.length()>0)
		{
			result = doInsert(query, params);		
		}
		paramsForUpdate.clear();
		return result;
	}
	private String prepareUpdate() throws EasyORMException{
		params=new ArrayList<Object>();
		String updateString="";
		String name="";
		if(paramsForUpdate.size()>0){
			name=paramsForUpdate.get(0);
			updateString+=name+"=?";
			params.add(cachedResults.get(name));
			for (int i=1;i<paramsForUpdate.size();i++) 
			{
				name= paramsForUpdate.get(i);
				updateString+=", "+name+"=?";
				params.add(cachedResults.get(name));
			}
		}else{
			return null;
		}			
		return updateString;
	}
	private int update(boolean useAutoGenColumn, String updateByColumn) throws EasyORMException{

		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		String query=null;
		String updateString=prepareUpdate();
		String colName = useAutoGenColumn ? getClass().getAnnotation(TableInfo.class).idColumnName() : updateByColumn;
		if(updateString!=null){
			query="update "+qualifiedName+" set "+updateString+" where "+colName+"="+getValue(colName);
		}else{
			throw new EasyORMException(EasyORMException.NO_UPDATE_PARAMETERS);
		}
		return doUpdate(query,params);
	}
	public int update(String updateByColumn) throws EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		return update(false, updateByColumn);

	}
	public int update() throws  EasyORMException{
		AnnotationUtil.checkAnnotations(getClass());
		return update(true, null);
	}
	public static int update(String updateStatement, HashMap<String,Object>paramValues) throws  EasyORMException{
		
		return DBObject.doUpdateRecord(updateStatement, paramValues);
	}
	public static int delete(String deleteStatement, HashMap<String,Object>paramValues) throws  EasyORMException{
		
		return DBObject.doUpdateRecord(deleteStatement, paramValues);
	}
	private static int doUpdateRecord(String updateStatement, HashMap<String,Object>paramValues) throws  EasyORMException{
		List<String> uParams=DBHelper.parseQueryForParams(updateStatement);
		updateStatement = DBHelper.replaceQueryParams(updateStatement);
		List<Object> updateParams = new ArrayList<Object>();
		for(String param : uParams)
			updateParams.add(paramValues.get(param));

		return doUpdateWhere(updateStatement,updateParams);
	}
	private static  int doUpdateWhere(String query,List<Object> params)throws  EasyORMException
	{
		int updateCount=0;
		PreparedStatement stmt=null;
		Connection conn = null;
		try
		{
			conn = ConnectionPool.getInstance().getAvailableConnection();
			stmt=conn.prepareStatement(query);
			int i=1;
			for (Object param : params) {	
				stmt.setObject(i++, param);
			}
			updateCount=stmt.executeUpdate();
		}
		catch(SQLException e){
			throw new EasyORMException(e);
		}finally{
			if(stmt!=null){
				try {
					stmt.close();
				} catch (SQLException e) {
					throw new EasyORMException(e);
				}
			}
			if(conn!=null){
				ConnectionPool.getInstance().returnConnection(conn);
			}
		}
		return updateCount;
	}
	/*public int update(String whereClause, HashMap<String,Object>paramValues) throws  EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		if(whereClause==null||whereClause.isEmpty())
			throw new EasyORMException(EasyORMException.NO_WHERE_CLAUSE_PARAMETERS);
		String query=null;
		String updateString=prepareUpdate();
		List<String> whereParams=DBHelper.parseQueryForParams(whereClause);
		whereClause = DBHelper.replaceQueryParams(whereClause);

		for(String param : whereParams)
			params.add(paramValues.get(param));

		if(updateString!=null){
			query="UPDATE "+qualifiedName+" SET "+updateString+" WHERE "+whereClause;

		}else{
			throw new EasyORMException(EasyORMException.NO_UPDATE_PARAMETERS);
		}
		return doUpdate(query,params);
	}*/
	public int updateRange(Integer[] ids,String idColumnName) throws  EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		return updateRange(ids, false, idColumnName);
	}
	public int updateRange(Integer[] ids) throws  EasyORMException{
		AnnotationUtil.checkAnnotations(getClass());
		return updateRange(ids, true, null);
	}
	private int updateRange(Integer[] ids, boolean useAutoGenColumn, String updateByColumn) throws EasyORMException{
		String colName = useAutoGenColumn ? getClass().getAnnotation(TableInfo.class).idColumnName() : updateByColumn;
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName() : getClass().getAnnotation(TableInfo.class).tableName();
		String query=null;
		String updateString=prepareUpdate();
		if(updateString!=null){
			query="UPDATE "+qualifiedName+" SET "+updateString+" WHERE "+colName+" IN ("+getIds(ids)+")";
		}else{
			throw new EasyORMException(EasyORMException.NO_UPDATE_PARAMETERS);
		}
		return doUpdate(query,params);
	}
	private String getIds(Integer [] ids){
		String idSeq="";
		idSeq+=cachedResults.get(getClass().getAnnotation(TableInfo.class).idColumnName());
		for(int id:ids){
			idSeq+=",";
			idSeq+=id;
		}
		return idSeq;
	}
	private int deleteRange(Integer[] ids, boolean useAutoGenColumn, String updateByColumn) throws EasyORMException{

		String colName = useAutoGenColumn ? getClass().getAnnotation(TableInfo.class).idColumnName() : updateByColumn;
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		return doUpdate("delete from "+qualifiedName+" where "+colName+" IN ("+getIds(ids)+")");
	}
	public int deleteRange(Integer[] ids, String updateByColumn) throws EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		return deleteRange(ids, false, updateByColumn);
	}
	public int deleteRange(Integer[] ids) throws  EasyORMException{
		AnnotationUtil.checkAnnotations(getClass());
		return deleteRange(ids, true, null);
	}
	public int delete()  throws EasyORMException {
		AnnotationUtil.checkAnnotations(getClass());
		return delete(true, null);
	}
	public int delete(String deleteByColumn) throws EasyORMException{
		AnnotationUtil.checkTableAnnotation(getClass());
		return delete(false, deleteByColumn);
	}
	private int delete(boolean useAutoGenColumn, String deleteByColumn) throws EasyORMException{

		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		String colName = useAutoGenColumn ? getClass().getAnnotation(TableInfo.class).idColumnName() : deleteByColumn;

		return doUpdate("delete from "+qualifiedName+" where "+colName+"="+cachedResults.get(colName));
	}

	/*public int delete(String whereClause,HashMap<String,Object>paramValues)  throws EasyORMException {
		AnnotationUtil.checkTableAnnotation(getClass());
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		return doUpdate("delete from "+qualifiedName+" where "+whereClause);
	}*/

	private Object doInsertReturnGenKey(String query,List<Object> params)throws  EasyORMException{
		PreparedStatement stmt=null;
		Object id=null;
		try{	
			boolean genKeysReturn=this.supportsGeneratedKeysReturn();
			if(genKeysReturn)
				stmt=conn.prepareStatement(query,Statement.RETURN_GENERATED_KEYS);
			else
				stmt=conn.prepareStatement(query);
			int i=1;
			for (Object param : params) {	
				stmt.setObject(i++, param);
			}
			stmt.execute();
			if(genKeysReturn){
				ResultSet rs = stmt.getGeneratedKeys();
				if (rs.next()) 
					id = rs.getObject(1);
				if(rs!=null)rs.close();
				if(stmt!=null)stmt.close();
			}else{
				id = (Integer)this.getLastGeneratedKey();
			}

		}catch(SQLException e){
			throw new EasyORMException(e);
		}
		return id;
	}
	private int doInsert(String query,List<Object> params)throws  EasyORMException{
		PreparedStatement stmt=null;
		int result = -1;
		try{	
			stmt=conn.prepareStatement(query);
			int i=1;
			for (Object param : params) {	
				stmt.setObject(i++, param);
			}
			result = stmt.executeUpdate();	

		}catch(SQLException e){
			throw new EasyORMException(e);
		}
		return result;
	}
	final private  boolean supportsGeneratedKeysReturn() throws  EasyORMException{

		try{
		return conn.getMetaData().supportsGetGeneratedKeys();
		}catch(SQLException sqle){
			throw new EasyORMException(sqle);
		}
	}
	private Object getLastGeneratedKey() throws EasyORMException{

		Object o = null;
		DBSelect dbSelect = new DBSelect();
		String qualifiedName = dbSchema!=null?dbSchema+"."+getClass().getAnnotation(TableInfo.class).tableName():getClass().getAnnotation(TableInfo.class).tableName();
		String id = getClass().getAnnotation(TableInfo.class).idColumnName();
		String query = "SELECT "+id+" FROM "+qualifiedName +" ORDER BY "+id+" DESC";
		o = dbSelect.getScalarValueForCustomQuery(query, false);
		return o;
	}
	private int doUpdate(String query)throws  EasyORMException
	{
		int updateCount=0;
		Statement stmt=null;
		try
		{	
			stmt=conn.createStatement();
			updateCount=stmt.executeUpdate(query);
			stmt.close();
			stmt=null;
		}
		catch(SQLException e){
			throw new EasyORMException(e);
		}
		return updateCount;
	}
	private int doUpdate(String query,List<Object> params)throws  EasyORMException
	{
		int updateCount=0;
		PreparedStatement stmt=null;
		try
		{
			stmt=conn.prepareStatement(query);
			int i=1;
			for (Object param : params) {	
				stmt.setObject(i++, param);
			}
			updateCount=stmt.executeUpdate();

			stmt.execute();
			stmt.close();
			stmt=null;
		}
		catch(SQLException e){
			throw new EasyORMException(e);
		}
		return updateCount;
	}
	public  String getPackageName(){
		return getClass().getPackage().getName();
	}
	public void returnActiveConnectionToPool(){

		ConnectionPool.getInstance().returnConnection(conn);
	}
	private String clobToString(Clob data) throws EasyORMException  {
		StringBuilder sb = new StringBuilder();
		Reader reader=null;
		BufferedReader br =null;
		try {
			reader = data.getCharacterStream();

			br = new BufferedReader(reader);

			String line;
			while(null != (line = br.readLine())) {
				sb.append(line);
			}
			br.close();
		} catch (SQLException e) {
			throw new EasyORMException(e);
		} catch (IOException e) {
			throw new EasyORMException(e);
		}finally{
			if(reader!=null)
				try {
					reader.close();
				} catch (IOException e) {
					throw new EasyORMException(e);
				}
			if(br!=null)
				try {
					br.close();
				} catch (IOException e) {
					throw new EasyORMException(e);
				}	
		}
		return sb.toString();
	}
	private byte[] blobToBytes(Blob data) throws EasyORMException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] bytes = null;
		int n = 0;
		InputStream in=null;
		try {
			byte[] buf = new byte[(int) data.length()];
			in = data.getBinaryStream();
			while ((n=in.read(buf))>=0)
			{
				baos.write(buf, 0, n);		
			}
			bytes = baos.toByteArray();
		} catch (IOException e) {
			throw new EasyORMException(e);
		} catch (SQLException e) {
			throw new EasyORMException(e);
		}finally{
			try {
				in.close();
			} catch (IOException e) {
				throw new EasyORMException(e);
			}
		}			
		return bytes;
	}
}
