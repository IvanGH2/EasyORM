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

package ib.easyorm.exception;
public class EasyORMException extends Exception{
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public EasyORMException(String message){
		super(message);
	}
	public EasyORMException(String message,Throwable cause){
		super(message, cause);
	}
	public EasyORMException(Throwable cause){
		super(cause);
	}
	public static final String CONNECTION_NUM_EXCEEDED = "Number of available connections has been reached ";
	public static final String QUERY_PARAMS_MISSING = "Query is missing params - check the query";
	public static final String CONSTRUCTOR_ARGS_MISSING_RESULTSET = " Class is missing a public  Constructor(ResultSet) ";
	public static final String CONSTRUCTOR_ARGS_MISSING_OBJECT = " Class is missing an Object public  Constructor(Object)";
	public static final String TABLE_ANNOTATION_MISSING = " Class is missing a TableInfo annotation ";
	public static final String TABLE_ID_COLUMN_ANNOTATION_MISSING = " Class is missing a TableInfo annotation for the identity column";
	public static final String NO_UPDATE_PARAMETERS = "No update parameters have been provided";
	public static final String NO_INSERT_PARAMETERS = "No insertion parameters have been provided";
	public static final String NO_WHERE_CLAUSE_PARAMETERS = "No parameters have been provided in the where clause";
	public static final String MULTIPLE_RECORDS = "Query has returned multiple records where only one was expected";
	public static final String FROM_CLAUSE_MISSING = "No from clause has been found for the query ";
	public static final String OUT_PARAM_MISSING = "At least one OUT parameter must be specified";
	public static final String OUT_CURSOR_MISSING = "OUT cursor parameter must be specified or the cursor position is wrong";
	public static final String RESULTSET_NOT_RETURNED = "Result set not returned";
	public static final String CURSOR_NOT_RETURNED = "Result set not returned";
	public static final String RESULTSET_CURSOR_NOT_RETURNED = "Neither a Result set nor a Cursor returned";
} 