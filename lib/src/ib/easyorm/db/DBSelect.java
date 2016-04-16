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
	public <T>List<String> getTableColumnNames(Class<T>target) throws EasyORMException{
		return dbMetaData.getColumnNames(target);
	}
	public <T>List<String> getTableColumnNames(String tableName,String schemaName) throws EasyORMException{
		return dbMetaData.getColumnNames(tableName, schemaName);
	}
	public <T>List<String> getTableColumnTypes(Class<T>target) throws  EasyORMException{
		return dbMetaData.getColumnTypes(target);
	}
	public <T>List<String> getTableColumnTypes(String tableName,String schemaName) throws  EasyORMException{
		return dbMetaData.getColumnTypes(tableName,schemaName);
	}
	
	public <T>List<Integer> getTableNullableColumns(Class<T>target) throws  EasyORMException{
		return dbMetaData.getNullableColumns(target);
	}
	public <T>List<Integer> getTableNullableColumns(String tableName,String schemaName) throws  EasyORMException{
		return dbMetaData.getNullableColumns(tableName,schemaName);
	}
	public <T>HashMap<String,String> getTableColumnInfo(Class<T>target) throws  EasyORMException{
		return dbMetaData.getTableColumnsInfo(target);
	}
	public List<String> getTableNames(String schemaName) throws EasyORMException{
		return dbMetaData.getTableNames(schemaName);
	}
	public DBSelect(Connection conn) throws SQLException{
		this.conn=conn;
		dbType=conn.getMetaData().getDatabaseProductName();
	}
	/*public <T>DBSelect(Connection conn, Class<T> target) throws  EasyORMException{
		this.conn=conn;
		dbType=getProduct();
	}*/
	/*public <T>DBSelect( Class<T> target) throws  EasyORMException, SQLException{
		conn=ConnectionPool.getInstance().getAvailableConnection();
		dbType=conn.getMetaData().getDatabaseProductName();	
	}*/
	public DBSelect( ) throws EasyORMException{
		conn=ConnectionPool.getInstance().getAvailableConnection();
		dbMetaData=new DBMetaData(conn);
		dbType=getProduct();	
	}
	public DBSelect(DBTransaction dbTrx ) throws EasyORMException{//
		conn=dbTrx.getTransactionConnection();
		dbMetaData=new DBMetaData(conn);
		dbType=getProduct();	
	}
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
	public Object getScalarValueForCustomQuery(String query, boolean throwIfMultiple) throws EasyORMException{
		Object obj=null;
		ResultSet rs=null;
		PreparedStatement stmt=null;
		try{
			sqlQuery = query;
			stmt=conn.prepareStatement(query);
			rs = stmt.executeQuery();	
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
	
	public void setChildObjects(String[] childNames){
		childObjectNames=childNames;//"AddressDB"
	}
	public String[] getChildObjectsSet(){
		return childObjectNames;
	}
	 private String dbSchema;

     public String getDbSchema(){
           return dbSchema;
     }

     public void setDbSchema(String schemaName){
           dbSchema=schemaName;
     }
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
	 * method returns a list of values (out parameters) and can't be used with procedures/functions that return a cursor (result set)
	 * @param procedureName
	 * @param inParamPositions
	 * @param inParamValues
	 * @param outParamPositions
	 * @param outParamTypes
	 * @return
	 * @throws EasyORMException
	 */
	public List<Object> getScalarValuesForStoredProcedure(String procedureName, List<Integer>inParamPositions,List<Object>inParamValues, List<Integer>outParamPositions,List<Integer>outParamTypes) throws EasyORMException{
	       
      
        boolean result = false;
        CallableStatement stmt=null;
        List<Object> outList = new ArrayList<Object>();
        try{
            String paramsIn = "";
            String paramsOut = "";
           
            if(inParamValues!=null&&!inParamValues.isEmpty())
                  paramsIn = generateParamsPlaceholder(inParamValues.size());
           
            if(outParamPositions!=null&&!outParamPositions.isEmpty())
                  paramsOut = generateParamsPlaceholder(outParamPositions.size());
              if(this.dbSchema!=null && !dbSchema.isEmpty())
                    procedureName = dbSchema+"."+procedureName;
              sqlQuery = "{call " + procedureName +"("+ paramsIn +","+ paramsOut +")}";
              
              stmt=conn.prepareCall(sqlQuery);
             
              for(int i=0;i<inParamPositions.size();i++)
            	  stmt.setObject(inParamPositions.get(i), inParamValues.get(i));
              //register output params
              for(int i=0;i < outParamPositions.size();i++)
            	  stmt.registerOutParameter(outParamPositions.get(i), outParamTypes.get(i));

              result = stmt.execute();   

              if(result){
            	  for(int i=0; i<outParamPositions.size(); i++){
            		  outList.add(stmt.getObject(outParamPositions.get(i)));
            	  }
              }

        }catch(Exception e){
        	throw new EasyORMException(e);
        }finally{
        	closeResources(null,stmt);
        }
        return outList;
  }
	/**
	 * method returns a single scalar value (though the single value can be an array)
	 * @param procedureName
	 * @param paramValues
	 * @return
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
             // rs = stmt.executeQuery();
              stmt.execute();
              obj = stmt.getObject(1);
              /*if(rs.next()){
                    obj=rs.getObject(1);
              }*/
        }catch(Exception e){
              throw new EasyORMException(e);
        }finally{
              closeResources(rs,stmt);
        }
        return obj;
  }
	/**
	 * method returns a result set and converts it to a pojo
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
	private void closeResources(ResultSet rs, PreparedStatement stmt) throws EasyORMException {
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