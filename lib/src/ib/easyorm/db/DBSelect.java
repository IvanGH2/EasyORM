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

import ib.easyorm.annotation.AttributeInfo;
import ib.easyorm.annotation.TableInfo;
import ib.easyorm.annotation.util.AnnotationUtil;
import ib.easyorm.exception.EasyORMException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DBSelect{

	private Connection conn;
	private DBMetaData dbMetaData;
	private final String PKG_NAME="getPackageName";

	private static String dbType;
	private int recNum=10;
	private String getProduct() throws EasyORMException{
		try {
			return conn.getMetaData().getDatabaseProductName();
		} catch (SQLException e) {
			throw new EasyORMException(e);
		}	
	}
	private String sqlQuery;
	private String sorting ="";
	private SortingType sortingType;
	public enum SortingType { ASC, DESC };
	
	public void setSortingType(SortingType sortType){
		sortingType=sortType;
		sorting= sortType==SortingType.DESC ? "DESC" : "";
	}
	public SortingType getSortingType(){
		return sortingType;
	}
	public DBSelect(DBTransaction dbTrx, String jdbcDriver, String jdbcURL, String user, String password) throws  EasyORMException{
		conn=ConnectionPool.getInstance(jdbcDriver, jdbcURL, user, password).getAvailableConnection();		
		dbTrx.setTransactionConnection(conn);

	}
	public DBSelect(String jdbcDriver, String jdbcURL, String user, String password) throws  EasyORMException{
		conn=ConnectionPool.getInstance(jdbcDriver, jdbcURL, user, password).getAvailableConnection();
		dbType=getProduct();
	}
	public <T>DBSelect(String jdbcDriver, String jdbcURL, String user, String password, Class<T> target) throws  EasyORMException{
		conn=ConnectionPool.getInstance(jdbcDriver, jdbcURL, user, password).getAvailableConnection();
		//dbMetaData=new DBMetaData(conn,target);
		dbType=getProduct();	
	}
	/**
	 * The method uses database metainformation to retrieve records for a particular table
	 * @param target (POJO corresponding to a table)
	 * @return
	 * @throws EasyORMException
	 */
	public <T>List<String> getTableColumnNames(Class<T>target) throws EasyORMException{
		return dbMetaData.getColumnNames(target);
	}
	/**
	 * 
	 * @param tableName
	 * @param schemaName
	 * @return
	 * @throws EasyORMException
	 */
	public <T>List<String> getTableColumnNames(String tableName,String schemaName) throws EasyORMException{
		return dbMetaData.getColumnNames(tableName, schemaName);
	}
	public <T>List<String> getTableColumnTypes(Class<T>target) throws  EasyORMException{
		return dbMetaData.getColumnTypes(target);
	}
	/**
	 * This method retrieves column types for a particular table
	 * @param tableName
	 * @param schemaName
	 * @return
	 * @throws EasyORMException
	 */
	public <T>List<String> getTableColumnTypes(String tableName,String schemaName) throws  EasyORMException{
		return dbMetaData.getColumnTypes(tableName,schemaName);
	}
	/**
	 * This method returns nullable columns, which can then be used for validation purposes
	 * @param target
	 * @return
	 * @throws EasyORMException
	 */
	public <T>List<Integer> getTableNullableColumns(Class<T>target) throws  EasyORMException{
		return dbMetaData.getNullableColumns(target);
	}
	/**
	 * This method returns nullable columns, which can then be used for validation purposes
	 * @param tableName
	 * @param schemaName
	 * @return
	 * @throws EasyORMException
	 */
	/**
	 * This method returns nullable columns, which can then be used for validation purposes
	 * @param tableName
	 * @param schemaName
	 * @return
	 * @throws EasyORMException
	 */
	public <T>List<Integer> getTableNullableColumns(String tableName,String schemaName) throws  EasyORMException{
		return dbMetaData.getNullableColumns(tableName,schemaName);
	}
	/**
	 * The method return column names as well as types for a particular table
	 * @param target
	 * @return
	 * @throws EasyORMException
	 */
	public <T>HashMap<String,String> getTableColumnInfo(Class<T>target) throws  EasyORMException{
		return dbMetaData.getTableColumnsInfo(target);
	}
	/**
	 * The method returns a list of table names belonging to a particular schema 
	 * @param schemaName
	 * @return
	 * @throws EasyORMException
	 */
	public List<String> getTableNames(String schemaName) throws EasyORMException{
		return dbMetaData.getTableNames(schemaName);
	}
	/**
	 * Normally, you will use this constuctor only if you don't use a connection pool
	 * Otherwise, call either a no-args constructor or a constructor that accepts a DBTransaction instance
	 * @param conn
	 * @throws SQLException
	 */
	public DBSelect(Connection conn) throws SQLException{
		this.conn=conn;
		dbType=conn.getMetaData().getDatabaseProductName();
	}
   
	public DBSelect( ) throws EasyORMException{
		conn=ConnectionPool.getInstance().getAvailableConnection();
		dbMetaData=new DBMetaData(conn);
		dbType=getProduct();	
	}
	/**
	 * this constructor should be used when you want to retrieve records as part of a database transaction. 
	 * This is useful, for example when you fetch records for update.
	 * @param dbTrx
	 * @throws EasyORMException
	 */
	public DBSelect(DBTransaction dbTrx ) throws EasyORMException{//
		conn=dbTrx.getTransactionConnection();
		dbMetaData=new DBMetaData(conn);
		dbType=getProduct();	
	}
	/**
	 * This method returns the connection used by this instance of DBSelect so that it can be reused
	 */
	public void returnActiveConnectionToPool(){
		
		ConnectionPool.getInstance().returnConnection(conn);
	}
	public void enableMetaData() throws EasyORMException{
		dbMetaData=new DBMetaData(conn);
	}
	private String generateGenericPagingClause(int countRecord, int startRecord){
		String specificSql="";
		if("PostgreSQL".equals(dbType)||"H2".equals(dbType)){
			specificSql=" LIMIT "+ countRecord+" OFFSET "+startRecord;
		}else if("MySQL".equals(dbType)||"SQLite".equals(dbType)){
			specificSql=" LIMIT "+ startRecord+","+countRecord;
		}
		return specificSql;
	}	

	public void setRecordNumber(int recNum){
		this.recNum=recNum;
	}
	public int getRecordNumber(){
		return recNum;
	}
	public String getSqlQuery(){
		return sqlQuery;
	}
	/**
	 * This method returns a single Object from the database (normally when you expect a single value to be returned). 
	 * @param query - the sql query (any parameters should be embedded within the string)
	 * @param throwIfMultiple - if set to true , an EasyORMException.MULTIPLE_RECORD exception will be thrown if multiple records are found
	 * @return
	 * @throws EasyORMException
	 */
	public Object getScalarValueForCustomQuery(String query, boolean throwIfMultiple) throws EasyORMException{
		Object obj=null;
		ResultSet rs=null;
		Statement stmt=null;
		try{
			sqlQuery = query;
			stmt=conn.createStatement();
			rs = stmt.executeQuery(sqlQuery);	
			if(rs.next()){
				obj=rs.getObject(1);
				if(throwIfMultiple&&rs.next()) 
					throw new EasyORMException(EasyORMException.MULTIPLE_RECORDS);
			}
		}catch(Exception e){
			throw new EasyORMException(e);
		}finally{
			closeResources(rs,stmt);
		}
		return obj;
	}
	private String[] childObjectNames=null;
	
	/**
	 * You can specify any child objects that you want returned by calls to getRecordForCustomQuery, getRecordsForParamQuery etc
	 * @param childNames - specify child object names (these are objects annotated with @AttributeInfo)
	 */
	public void setChildObjects(String[] childNames){
		childObjectNames=childNames;//"AddressDB"
	}
	public String[] getChildObjectsSet(){
		return childObjectNames;
	}
	 private  String dbSchema;
	 private static String globalDbSchema;
    
	 /**
	  * retrieves a database schema 
	  * @return
	  */
     public  String getDbSchema(){
          // return (dbSchema!=null&&!dbSchema.isEmpty()) ? dbSchema : globalDbSchema;
           return dbSchema;
     }
     /**
      * Usually , you'll want to set this to a particular schema, which differs from the default schema
      * @param schemaName
      */
     public  void setDbSchema(String schemaName){
         dbSchema=schemaName;
     }
     /*public static String getGlobalDbSchema(){
         return globalDbSchema;
  	}
      
     public static void setGlobalDbSchema(String schemaName){
           globalDbSchema=schemaName;
     }*/
     /**
      * This method returns all records belonging to a particular table or view (which have a corresponfing POJO)
      * @param target - target POJO class
      * @param startRecord - used for paging
      * @param countRecord - used for paging
      * @param orderByColumn - the name of the column used for sorting
      * @return
      * @throws EasyORMException
      */
	public <T> List<T> getRecordsForSingleTable( Class<? extends DBObject> target, int startRecord, int countRecord,String orderByColumn ) throws EasyORMException {
		T obj=null;
		ResultSet rs=null;
		PreparedStatement stmt=null;
		AnnotationUtil.checkTableAnnotation(target);
		List<T> objList = new ArrayList<T>();
		countRecord = (countRecord<=0)?recNum:countRecord;
		startRecord = (startRecord<0)?0:startRecord;
		String qualifiedName = dbSchema!=null?dbSchema+"."+target.getAnnotation(TableInfo.class).tableName():target.getAnnotation(TableInfo.class).tableName();
		String query=null;
		if(orderByColumn!=null&&!orderByColumn.isEmpty())
			query=modifyQuery(qualifiedName, startRecord, countRecord, orderByColumn);
		else
			query="SELECT * FROM "+qualifiedName;
		try{
			sqlQuery = query;
			stmt=conn.prepareStatement(query);
			rs = stmt.executeQuery();

			while(rs.next()){
				obj = (T) target.getConstructor(ResultSet.class).newInstance(rs);
				((DBObject)obj).setConnection(conn);
				objList.add(obj);
			}
		}catch(Exception e){
			throw new EasyORMException(e);
		} finally{
			closeResources(rs,stmt);
		}
		return objList;
	}
	/**
	 * This method accepts an sql query 
	 * e.g. select * from some_tbl where id = 45  
	 * @param target - The results of this query are mapped to a POJO target instance
	 * @param startRecord - set to 0 if you don't want paging
	 * @param countRecord - set to 0 if you don't want paging
	 * @param orderByColumn - must be set if you set startRecord and countRecord (because paging requires sorting)
	 * @return a list of T objects where T is any class extending DBObject
	 * @throws EasyORMException
	 */
	public <T> List<T> getRecordsForCustomQuery( String query, Class<? extends DBObject> target, int startRecord, int countRecord,String orderByColumn ) throws EasyORMException {
		T obj=null;
		ResultSet rs=null;
		PreparedStatement stmt=null;
		List<T> objList = new ArrayList<T>();
		countRecord = (countRecord<=0)?recNum:countRecord;
		startRecord = (startRecord<0)?0:startRecord;
		try{
			sqlQuery = (orderByColumn!=null)?modifyCustomQuery(query, startRecord, countRecord, orderByColumn):query;
			stmt = conn.prepareStatement(sqlQuery);
			//stmt=(orderByColumn!=null)?conn.prepareStatement(this.modifyCustomQuery(query, startRecord, countRecord, orderByColumn)):conn.prepareStatement(query);
			rs = stmt.executeQuery();

			while(rs.next()){
				obj=(T) target.getConstructor(ResultSet.class).newInstance(rs);
				((DBObject)obj).setConnection(conn);
				if(childObjectNames!=null&&childObjectNames.length>0)
					populateChildObjects(target,obj);
				objList.add(obj);
			}
		}catch(Exception e){
			throw new EasyORMException(e);
		}finally{
			closeResources(rs,stmt);
		}
		return objList;
	}
	private <T>void populateChildObjects(Class<T> parent,Object obj) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException{
		Method[] methods=parent.getDeclaredMethods();
		Method methodToCall = parent.getMethod(PKG_NAME);
		String packageName=(String)methodToCall.invoke(obj);
		for(Method m : methods){
			if(m.isAnnotationPresent(AttributeInfo.class)){	
				String childClsName = m.getAnnotation(AttributeInfo.class).attributeType();
				if(!childObjExists(childClsName)) 
					continue;
				Class<? extends DBObject> childCls=(Class<? extends DBObject>) Class.forName(packageName+"."+childClsName);
				m.invoke(obj,childCls.getConstructor(Object.class).newInstance(obj));				
			}
		}	
	}
	private boolean childObjExists(String name){
		
		for(String s : childObjectNames){
			if(s.equals(name))
				return true;
		}
		return false;
	}
	/**
	 * This method accepts a custom query and supports named parameters 
	 * e.g. select * from employee where id = :empId (empId is a named param)
	 * @param query
	 * @param paramValues - you could set this by filling a HashMap e.g. paramValues.put("empId", 15);
	 * @param target - the result is mapped to a target POJO instance
	 * @param startRecord - set to 0 if you don't want paging
	 * @param countRecord-set to 0 if you don't want paging
	 * @param orderByColumn - must be set if you set startRecord and countRecord (because paging requires sorting)
	 * @return - a list of T objects where T is any class extending DBObject
	 * @throws EasyORMException
	 */
	public <T> List<T> getRecordsForParamQuery( String query, HashMap<String,Object>paramValues, Class<? extends DBObject> target, int startRecord, int countRecord,String orderByColumn ) throws EasyORMException {
		T obj=null;
		ResultSet rs=null;
		PreparedStatement stmt=null;
		List<T> objList = new ArrayList<T>();
		countRecord = (countRecord<=0)?recNum:countRecord;
		startRecord = (startRecord<0)?0:startRecord;
		List<String> queryParams=DBHelper.parseQueryForParams(query);
		if(queryParams.size()==0){
			return null;
		}
		query=DBHelper.replaceQueryParams(query);
		try{
			sqlQuery = (orderByColumn!=null)? modifyCustomQuery(query, startRecord, countRecord,orderByColumn) : query;
			stmt = conn.prepareStatement(sqlQuery);
			//stmt=(orderByColumn!=null)?conn.prepareStatement(modifyCustomQuery(query, startRecord, countRecord,orderByColumn)):conn.prepareStatement(query);
			for(int i=1; i<=queryParams.size();i++){
				stmt.setObject(i, paramValues.get(queryParams.get(i-1)));
			}
			rs = stmt.executeQuery();
			while(rs.next()){
				obj=(T) target.getConstructor(ResultSet.class).newInstance(rs);
				if(childObjectNames!=null&&childObjectNames.length>0)
					populateChildObjects(target,obj);
				((DBObject)obj).setConnection(conn);
				objList.add(obj);
			}
		}catch(NoSuchMethodException e){
			throw new EasyORMException(target.getName()+EasyORMException.CONSTRUCTOR_ARGS_MISSING_RESULTSET);
		}catch(Exception e){
			throw new EasyORMException(e);
		}finally{
			closeResources(rs,stmt);
		}
		return objList;
	}
	/**
	 * This method returns a list of values (OUT parameters) and can't be used with procedures that return a cursor (result set)
	 * @param procedureName
	 * @param inParamPositions (optional)
	 * @param inParamValues (optional)
	 * @param outParamPositions (at least one must be specified)
	 * @param outParamTypes (at least one must be specified)
	 * @return - a list of OUT arguments/values
	 * @throws EasyORMException
	 */
	public List<Object> getScalarValuesForStoredProcedure(String procedureName, List<Integer>inParamPositions,List<Object>inParamValues, List<Integer>outParamPositions,List<Integer>outParamTypes) throws EasyORMException{
	       
      
        boolean parmsIn = false;
        CallableStatement stmt=null;
        List<Object> outList = new ArrayList<Object>();
            
        try{
            String paramsIn = "";
            String paramsOut = "";
           

            if(outParamPositions!=null&&!outParamPositions.isEmpty())
                  paramsOut = generateParamsPlaceholder(outParamPositions.size());
            else //at least one OUT parameter must be specified
            	throw new EasyORMException(EasyORMException.OUT_PARAM_MISSING);
            
            if(inParamValues!=null&&!inParamValues.isEmpty()){
                  paramsIn = generateParamsPlaceholder(inParamValues.size());
                  parmsIn = true;
            }
           
              if(this.dbSchema!=null && !dbSchema.isEmpty())
                    procedureName = dbSchema+"."+procedureName;
              if(parmsIn) //IN params not mandatory
            	  sqlQuery = "{call " + procedureName +"("+ paramsIn +","+ paramsOut +")}";
              else 
            	  sqlQuery = "{call " + procedureName +"(" + paramsOut +")}";
             
            	                
              stmt=conn.prepareCall(sqlQuery);
             
              if(inParamPositions != null)
            	  for(int i=0;i<inParamPositions.size();i++)
            		  stmt.setObject(inParamPositions.get(i), inParamValues.get(i));
              
              for(int i=0;i < outParamPositions.size();i++)
            	  stmt.registerOutParameter(outParamPositions.get(i), outParamTypes.get(i));

              stmt.execute();               
        
            	  for(int i=0; i<outParamPositions.size(); i++){
            		  outList.add(stmt.getObject(outParamPositions.get(i)));
            	  }
              
        }catch(Exception e){
        	throw new EasyORMException(e);
        }finally{
        	closeResources(null,stmt);
        }
        return outList;
  }
	/**
	 * This method returns a single scalar value from a function/stored procedure that returns a single value. For stored
	 * procedures, the first argument must an OUT parameter.
	 * @param paramValues - IN parameters
	 * @return - a single, scalar value
	 * @throws EasyORMException
	 */
	public Object getScalarValueForStoredProcedure(String procedureName, List<Object>paramValues, int returnType) throws EasyORMException{
        
        Object obj=null;
        ResultSet rs=null;
        CallableStatement stmt=null;
        try{
        	String params = "";
        	if(paramValues!=null&&!paramValues.isEmpty())
        		params = this.generateParamsPlaceholder(paramValues.size());
        	  if(this.dbSchema!=null && !dbSchema.isEmpty())
        		  procedureName = dbSchema+"."+procedureName;
        	  sqlQuery = "{ ? = call " + procedureName +"("+ params + ")}";
              stmt=conn.prepareCall(sqlQuery);
              stmt.registerOutParameter(1, returnType);
              if(paramValues!=null&&!paramValues.isEmpty())
            	  for(int i=0;i < paramValues.size();i++)
            		  stmt.setObject(i+2, paramValues.get(i));
            
              stmt.execute();
              obj = stmt.getObject(1);

        }catch(Exception e){
              throw new EasyORMException(e);
        }finally{
              closeResources(rs,stmt);
        }
        return obj;
  }
	/**
	 * This method returns a result set and converts it to a POJO object specified in the second parameter
	 * @param procedureName
	 * @param target
	 * @param paramValues
	 * @param startRecord
	 * @param countRecord
	 * @param orderByColumn
	 * @return
	 * @throws EasyORMException
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getRecordsForStoredProcedure(String procedureName,Class<? extends DBObject> target,List<Object>paramValues, int returnType, int startRecord, int countRecord,String orderByColumn) throws EasyORMException{
        
        T obj=null;
        ResultSet rs=null;
        CallableStatement stmt=null;
        List<T> objList = new ArrayList<T>();
        countRecord = (countRecord<=0)?recNum:countRecord;
        startRecord = (startRecord<0)?0:startRecord;
        try{
        	String params = "";
        	if(paramValues!=null&&!paramValues.isEmpty())
        		params = this.generateParamsPlaceholder(paramValues.size());
        	 if(this.dbSchema!=null && !dbSchema.isEmpty())
        		 procedureName = dbSchema+"."+procedureName;
        	  sqlQuery = "{ ? = call " + procedureName +"("+ params + ")}";
              stmt=conn.prepareCall(sqlQuery);
              stmt.registerOutParameter(1, returnType);
              if(paramValues!=null&&!paramValues.isEmpty())
            	  for(int i=0;i<paramValues.size();i++)
            		  stmt.setObject(i+2, paramValues.get(i));
              stmt.execute(); 
              rs = (ResultSet)stmt.getObject(1);
              while(rs.next()){
                    obj=(T) target.getConstructor(ResultSet.class).newInstance(rs);
                    if(childObjectNames!=null&&childObjectNames.length>0)
                    	populateChildObjects(target,obj);
                    objList.add(obj);
              }
        }catch(Exception e){
             
              throw new EasyORMException(e);
        }finally{
              closeResources(rs,stmt);
        }
        return objList;
  }
	/**
	 * This method expects a stored procedure / function to return a cursor  
	 * your stored proc/function must look like either 
	 * create or replace some_proc(...) returns refcursor AS.... 	//(cursor returned in a return value)		
	 * create or replace some_proc(OUT cur_var refcursor, ...) AS  //(cursor returned in an OUT value at position 1)
	 * @param procedureName
	 * @param target - the data returned by the cursor are mapped to the target POJO 
	 * @param paramValues -IN parameters
	 * @param returnType - cursor type (e.g. oracle.sql.Types.OracleCursor )
	 * @param startRecord - used for paging (set to 0 if you dont want paging)
	 * @param countRecord - used for paging (set to 0 if you dont want paging)
	 * @param orderByColumn - used for sorting
	 * @return - a list of instances of DBObject subclasses
	 * @throws EasyORMException
	 */
public <T> List<T> getResultSetForStoredProcedure(String procedureName,Class<? extends DBObject> target,List<Object>paramValues, int returnType, int startRecord, int countRecord,String orderByColumn) throws EasyORMException{
        
        T obj=null;
        ResultSet rs=null;
        CallableStatement stmt=null;
        List<T> objList = new ArrayList<T>();
        countRecord = (countRecord<=0)?recNum:countRecord;
        startRecord = (startRecord<0)?0:startRecord;
        try{
        	String params = "";
        	if(paramValues!=null&&!paramValues.isEmpty())
        		params = this.generateParamsPlaceholder(paramValues.size());
        	 if(this.dbSchema!=null && !dbSchema.isEmpty())
        		 procedureName = dbSchema+"."+procedureName;
        	  sqlQuery = "{ call " + procedureName +"("+ params + ")}";
              stmt=conn.prepareCall(sqlQuery);
              stmt.registerOutParameter(1, returnType);
              if(paramValues!=null&&!paramValues.isEmpty())
            	  for(int i=0;i<paramValues.size();i++)
            		  stmt.setObject(i+1, paramValues.get(i));
              stmt.execute();
              rs = (ResultSet) stmt.getObject(1);               
              
              while(rs.next()){
                    obj=(T) target.getConstructor(ResultSet.class).newInstance(rs);
                    if(childObjectNames!=null&&childObjectNames.length>0)
                    	populateChildObjects(target,obj);
                    objList.add(obj);
              }
        }catch(Exception e){
             
              throw new EasyORMException(e);
        }finally{
              closeResources(rs,stmt);
        }
        return objList;
  }

/**
 * This method is the most flexible of all methods calling stored procedures/function because it can accept IN or OUT args 
 * at any positions , including at most 1 cursor variable (OUT)	 -- 1 cursor variable must be specified
 * your stored proc/function should look like either 
 * create or replace some_proc(...) returns refcursor AS.... 	//(cursor returned in a return value)		
 * create or replace some_proc(OUT cur_var refcursor, ...) AS  //(cursor returned in an OUT value at any position in the argument list)
 * @param procedureName
 * @param target - if the function/procedure returns a cursor, the data returned are mapped to the target POJO 
 * @param inParamPositions - positions in the argument list of the IN parameters
 * @param inParamValues - values in the argument list of the IN parameters
 * @param outParamPositions - positions in the argument list of the OUT parameters
 * @param outParamTypes - types in the argument list of the OUT (or IN OUT) parameters
 * @param cursorPos - position of the cursor argument (OUT arg) 
 * @param startRecord
 * @param countRecord
 * @param orderByColumn
 * @return
 * @throws EasyORMException
 */
public  List<Object> getRecordsForStoredProcedure(String procedureName,Class<? extends DBObject> target,List<Integer>inParamPositions,List<Object>inParamValues, List<Integer>outParamPositions,List<Integer>outParamTypes, int cursorPos, int startRecord, int countRecord,String orderByColumn) throws EasyORMException{
    
    Object obj=null;
    ResultSet rs=null;
    CallableStatement stmt=null;
    List<Object> objList = new ArrayList<Object>();
    countRecord = (countRecord<=0)?recNum:countRecord;
    startRecord = (startRecord<0)?0:startRecord;
    String paramsIn = "";
    String paramsOut;
    boolean parmsIn = false;
    
    if(outParamPositions!=null&&!outParamPositions.isEmpty())
        paramsOut = generateParamsPlaceholder(outParamPositions.size());
  else //at least one OUT parameter must be specified
  	throw new EasyORMException(EasyORMException.OUT_PARAM_MISSING);
  
  if(inParamValues!=null&&!inParamValues.isEmpty()){
        paramsIn = generateParamsPlaceholder(inParamValues.size());
        parmsIn = true;
  }
    try{

    	 if(this.dbSchema!=null && !dbSchema.isEmpty())
    		 procedureName = dbSchema+"."+procedureName;
    	 if(parmsIn)
    		 sqlQuery = "{ call " + procedureName +"("+ paramsIn + "," + paramsOut+ ")}";
    	 else
    		 sqlQuery = "{ call " + procedureName +"("+  paramsOut + ")}";
    	 
          stmt=conn.prepareCall(sqlQuery);
          
          for(int i=0;i<outParamPositions.size();i++)
          stmt.registerOutParameter(outParamPositions.get(i), outParamTypes.get(i));
          //populate IN args
          if(parmsIn)
        	  for(int i=0;i<inParamValues.size();i++)
        		  stmt.setObject(inParamPositions.get(i), inParamValues.get(i));
          stmt.execute();
         
          if(cursorPos > 0){
        	  rs = (ResultSet) stmt.getObject(cursorPos);                        
          while(rs.next()){
                obj= target.getConstructor(ResultSet.class).newInstance(rs);
                if(childObjectNames!=null&&childObjectNames.length>0)
                	populateChildObjects(target,obj);
                objList.add(obj);
          }
          }else{
        	  throw new EasyORMException(EasyORMException.OUT_CURSOR_MISSING);
          }
          
          for(int i=0; i<outParamPositions.size(); i++){
        	  int outPos = outParamPositions.get(i);
        	  if(outPos == cursorPos) 
        		  continue;
        	 objList.add(stmt.getObject(outPos));
    	  }         
    }catch(Exception e){
         
          throw new EasyORMException(e);
    }finally{
          closeResources(rs,stmt);
    }
    return objList;
}
	private void closeResources(ResultSet rs, Statement stmt) throws EasyORMException {
		if(rs!=null)
			try {
				rs.close();
			} catch (Exception e) {
				throw new EasyORMException(e);
			}
		if(stmt!=null)
			try {
				stmt.close();
			} catch (Exception e) {
				throw new EasyORMException(e);
			}
	}
	private String generateParamsPlaceholder(int len){
		String params = "";
		for(int i=0;i<len;i++){
			params += "?,";
		}
		return params.substring(0, params.length()-1);
	}

	private String modifyCustomQuery(String query,int startRecord,int countRecord,String orderBy) throws EasyORMException{
		String modQuery="";
		if("Microsoft SQL Server".equals(dbType)||"Oracle".equals(dbType)||"DB2".equals(dbType.substring(0, 3))){
			String[] from=query.split("(from)|(FROM)");
			if(from.length<2)
				throw new EasyORMException(EasyORMException.FROM_CLAUSE_MISSING);
			modQuery ="SELECT * FROM ("+from[0]+",ROW_NUMBER() over (order by "+orderBy+" "+sorting+") as rownum from"+from[1]+") a where a.rownum>="+startRecord+" and a.rownum<="+(startRecord+countRecord);
		}else{
			modQuery = query+ " ORDER BY "+orderBy + generateGenericPagingClause(countRecord, startRecord);
		}
		return modQuery;
	}
	private String modifyQuery(String fullName,int startRecord,int countRecord,String orderBy){

        String modQuery="";
        if("Oracle".equals(dbType)){

               modQuery ="SELECT * FROM (SELECT a.*, rownum rnum from (select * from "+fullName+" order by "+orderBy+" "+sorting+") a where rownum<="+(startRecord+countRecord)+") where rnum>"+startRecord;
        }
        else if("Microsoft SQL Server".equals(dbType)||"DB2".equals(dbType.substring(0, 3))){

               modQuery ="SELECT * FROM (SELECT *,ROW_NUMBER() over (order by "+orderBy+" "+sorting+") as rownum from "+fullName+") a where a.rownum>="+startRecord+" and a.rownum<="+(startRecord+countRecord);
        }else{//every other db though I'm sure this does not work for every dbms
               modQuery = "SELECT * FROM "+fullName+" ORDER BY "+orderBy+" "+sorting+generateGenericPagingClause(countRecord, startRecord);

        }
        return modQuery;

  }
}