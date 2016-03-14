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

import ib.easyorm.exception.EasyORMException;

import java.sql.Connection;
import java.sql.SQLException;


public class DBTransaction{

	private Connection trxConn;
	private IsolationLevel isolationLevel=IsolationLevel.TRX_UNCOMMITTED_READ;
	DBTransaction(){}
	public DBTransaction(ConnectionPool connPool) throws  EasyORMException{
		this.trxConn=connPool.getAvailableConnection();
		setTransactionProperties();
	}
	public DBTransaction(ConnectionPool connPool,IsolationLevel isolationLevel) throws EasyORMException{
		this(connPool);
		this.isolationLevel=isolationLevel;
	}
	public IsolationLevel getIsolationLevel(){
		return isolationLevel;
	}
	public void setIsolationLevel(IsolationLevel isoLevel){
		isolationLevel=isoLevel;
	}
	Connection getTransactionConnection(){
		return trxConn;
	}
	void returnConnectionToPool() throws EasyORMException {
		try {
			trxConn.setAutoCommit(true);
		} catch (SQLException e) {
			throw new EasyORMException(e);
			
		}
		ConnectionPool.getInstance().returnConnection(trxConn);
		
	}
	private void setTransactionProperties() throws  EasyORMException{
		try{
			if(trxConn.getAutoCommit())
				trxConn.setAutoCommit(false);

			switch(isolationLevel){
			case TRX_UNCOMMITTED_READ:
				trxConn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
				break;
			case TRX_COMMITTED_READ:
				trxConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				break;
			case TRX_REAPEATABLE_READ:
				trxConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
				break;
			case TRX_SERIALIZABLE:
				trxConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				break;
			}
		}catch(SQLException e){
			throw new EasyORMException(e);
		}
	}
	void setTransactionConnection(Connection conn) {
		trxConn=conn;		
	}
	public void commit() throws EasyORMException {
		try{
			trxConn.commit();
			returnConnectionToPool();
		}catch(SQLException e){
			throw new EasyORMException(e);
		}
	}
	public void rollback() throws EasyORMException {
		try{
			trxConn.rollback();		
			returnConnectionToPool();
		}catch(SQLException e){
			throw new EasyORMException(e);
		}
	}
	public static enum IsolationLevel { TRX_SERIALIZABLE, TRX_REAPEATABLE_READ, TRX_COMMITTED_READ, TRX_UNCOMMITTED_READ };

}