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

package ib.easyorm.generator;

import ib.easyorm.db.DBSelect;
import ib.easyorm.exception.EasyORMException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Types;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

 

public class PojoGenerator{

       private String pojoName;
       private String objName;
       private String schemaName;
       private String idName;
       private String pathPkg;
       private String pkgName;
       private StringBuilder strBuilder;       
       private Boolean nullable = Boolean.FALSE;
       private static PojoGenerator pojoGen;
       private int bufferSize=8000;
       private Set<String> typeNames;
       private List<Integer> nullColumns;            
       private static  String NEW_LINE = null;

       private PojoGenerator( String pojoName, String tableName, String schemaName, String idColumnName) throws EasyORMException{

             this.pojoName=pojoName;
             this.objName=tableName;
             this.schemaName=schemaName;
             this.idName=idColumnName;   
       }
       static {
    	   NEW_LINE = System.getProperty("line.separator");
       }
       public void setPathToPackage(String path){

             this.pathPkg=path;
       }
       public String getPathToPackage(){

             return pathPkg;
       }
       public String getPackage(){

             return pkgName;

       }
       public void setPackage(String pkg){

             this.pkgName=pkg;

       }
       public static PojoGenerator getInstance(String pojoName, String tableName, String schemaName,String idColumnName) throws EasyORMException{

             if(pojoGen==null)         {
                    pojoGen = new PojoGenerator(pojoName,tableName,schemaName,idColumnName);
             }

             return pojoGen;
       }
       public int getBufferSize(){

             return bufferSize;
       }

       public void setBufferSize(int size){

             bufferSize=size;
       }
       public void generatePojo() throws EasyORMException{

             if(objName!=null){       
             
                    typeNames = new HashSet<String>();

                    DBSelect dbSelect = new DBSelect();   
                    
                    if(nullable)	{
             		   nullColumns = dbSelect.getTableNullableColumns(objName, schemaName);
                    }

                    List<String> listTypes = dbSelect.getTableColumnTypes(objName,schemaName);
                     
                    convertTypes(listTypes);

                    generatePojo(dbSelect.getTableColumnNames(objName,schemaName),listTypes);
                    
                    dbSelect.returnActiveConnectionToPool();

             }                  
       }
       public void setNullable(boolean nullable){
    	   this.nullable = nullable;
       }
       private void convertTypes( List<String> listTypes){

             for(int i=0;i<listTypes.size();i++){

                    int type=Integer.parseInt(listTypes.get(i));

                    switch(type){

                    case Types.VARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.NVARCHAR:
                    case Types.NCHAR:                
                          listTypes.set(i, "String");
                          break;
                    case Types.TIMESTAMP:
                          listTypes.set(i, "Timestamp");  
                          typeNames.add("java.sql.Timestamp");
                          break;
                    case Types.DATE:
                          listTypes.set(i, "java.sql.Date");
                          typeNames.add("java.sql.Date");
                          break;
                    case Types.INTEGER:
                          listTypes.set(i, "Integer");
                          break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                          listTypes.set(i, "BigDecimal");
                          typeNames.add("java.math.BigDecimal");
                          break;
                    case Types.BIGINT:
                          listTypes.set(i, "Long");
                          break;
                    case Types.FLOAT:
                          listTypes.set(i, "Float");
                          break;
                    case Types.DOUBLE:
                          listTypes.set(i, "Double");
                          break;
                    case Types.SMALLINT:
                          listTypes.set(i, "Short");
                          break;
                    case Types.TINYINT:
                          listTypes.set(i, "Byte");
                         break;
                    case Types.BLOB:
                    	listTypes.set(i, "Blob");
                    	break;
                    case Types.CLOB:
                    	listTypes.set(i, "Clob");
                    	break;
                    default:

                          listTypes.set(i, "Object");               

                    }                  
             }                         
       }

       private enum ObjectType { PHYSICAL, VIRTUAL };
       
       private void generatePojo(List<String> attrNames,List<String> attrTypes) throws EasyORMException{
   
    	     strBuilder=new StringBuilder(bufferSize);

             toLowerCase(attrNames);

             generatePackage();

             generateImports();

             generateComment(objName!=null?ObjectType.PHYSICAL:ObjectType.VIRTUAL);

             generateAnnotations();

             generatePojoClass();

             generatateConstructors();

             generateStaticAttributes(attrNames);

             generateGetterMethods(attrNames, attrTypes);

             generateSetterMethods(attrNames, attrTypes);

            // generateOverriddenMethods();

             strBuilder.append(NEW_LINE+"}");

             saveToFile();

       }

       private void toLowerCase(List<String> attrNames){

             for(int i=0; i<attrNames.size();i++){

                    attrNames.set(i, attrNames.get(i).toLowerCase());

             }

       }	
	private void saveToFile() throws EasyORMException{

		try {

			PrintWriter pw=new PrintWriter(pathPkg+"\\"+pojoName+".java");

			pw.write(strBuilder.toString());

			pw.close();

		} catch (FileNotFoundException e) {

			throw new EasyORMException(e);

		}
	}
	private void generateComment(ObjectType oType){

		if(oType==ObjectType.PHYSICAL){

			strBuilder.append(NEW_LINE+"/*This is an auto-generated file. The class generator uses database meta information\n\r"

+ "to retrieve the names and types for your class' members. If a type cannot be deduced from\n\r the "

+ "meta information, the Object type is assumed. You should check the class and make sure it is correct*/"+NEW_LINE);

		}else{

			strBuilder.append(NEW_LINE+"/*This is an auto-generated file.*/"+NEW_LINE);

		}

	}
	private void generateGetterMethods(List<String> attrNames,List<String> attrTypes){

		for(int i=0;i<attrNames.size();i++){

			String name=attrNames.get(i);

			String type=attrTypes.get(i);

			strBuilder.append("public "+type+" get"+modifyName(name)+"(){"+NEW_LINE+" return ("+type+")getValue(COLUMN_"+name.toUpperCase()+");"+NEW_LINE+"}"+NEW_LINE);

		}
	}
	private void generateSetterMethods(List<String> attrNames,List<String> attrTypes){

		for(int i=0;i<attrNames.size();i++){

			String name=attrNames.get(i);
			String type=attrTypes.get(i);
			String modName=modifyName(name);
			
			if(nullable){
				int nullVal = nullColumns.get(i);
				if(nullVal==0)
					strBuilder.append("@Notnullable"+NEW_LINE);			
			}

			strBuilder.append("public void set"+modName+"("+type+" "+ modName+"){"+NEW_LINE+" setValue(COLUMN_"+name.toUpperCase()+","+modName+");"+NEW_LINE+"}"+NEW_LINE);

		}
	}

	private void generateStaticAttributes(List<String> attrNames){

		for(String attr:attrNames){
			strBuilder.append("private static String COLUMN_"+attr.toUpperCase()+"=\""+attr+"\";"+NEW_LINE+NEW_LINE);
		}
	}

	private void generateAnnotations(){
		strBuilder.append("@TableInfo(tableName=\""+objName+"\",idColumnName=\""+idName+"\")"+NEW_LINE);
	}
	private void generatePojoClass(){
		strBuilder.append("public class "+pojoName+" extends DBObject {"+NEW_LINE);
	}
	private void generateTransactionConstructor(){

		strBuilder.append("public "+pojoName+"(DBTransaction dbTrx) throws EasyORMException {"+NEW_LINE+" super(dbTrx);"+NEW_LINE+"}"+NEW_LINE);
	}

	private void generateResultSetConstructor(){

		strBuilder.append("public "+pojoName+"(ResultSet rs) throws EasyORMException {"+NEW_LINE+"	super(rs);"+NEW_LINE+"}"+NEW_LINE);

	}

	private void generateNoArgsConstructor(){

		strBuilder.append("public "+pojoName+"() throws EasyORMException {}"+NEW_LINE+NEW_LINE);

	}

	private void generateObjectConstructor(){

		strBuilder.append("public "+pojoName+"(Object enclosingCls) throws EasyORMException {"+NEW_LINE+"	super("+pojoName+".class,enclosingCls);"+NEW_LINE+"}"+NEW_LINE);
	}

	private void generatateConstructors(){

		generateResultSetConstructor();
		generateNoArgsConstructor();
		generateObjectConstructor();
		generateTransactionConstructor();
	}

	/*private void generateOverriddenMethods(){

		strBuilder.append("@Override\n\rpublic String getPackageName(){\n\rreturn getClass().getPackage().getName();\n\r}\n\r");

	}*/

	private String modifyName(String name){

		String modified="";
		String[] underscoreSplit=name.split("_");
		int len=underscoreSplit.length;

		for(int i=0;i<len;i++){
			modified+=underscoreSplit[i].substring(0, 1).toUpperCase()+underscoreSplit[i].substring(1, underscoreSplit[i].length() );
		}

		return modified;
	}
	private void generatePackage(){
		
	       strBuilder.append("package "+pkgName+";"+NEW_LINE);
	}

	private void generateImports(){

	       //strBuilder.append("import java.io.Serializable;\n\r");
	       strBuilder.append("import java.sql.ResultSet;"+NEW_LINE);
	       strBuilder.append("import ib.easyorm.annotation.TableInfo;"+NEW_LINE);
	       strBuilder.append("import ib.easyorm.annotation.Notnullable;"+NEW_LINE);
	       strBuilder.append("import ib.easyorm.db.DBObject;"+NEW_LINE);
	       strBuilder.append("import ib.easyorm.db.DBTransaction;"+NEW_LINE);
	       strBuilder.append("import ib.easyorm.exception.EasyORMException;"+NEW_LINE);

	       Iterator<String> iter = typeNames.iterator();
	       while(iter.hasNext()){
	             strBuilder.append("import "+iter.next()+";"+NEW_LINE);
	       }

	}
}