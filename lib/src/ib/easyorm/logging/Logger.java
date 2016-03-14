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

package ib.easyorm.logging;

import ib.easyorm.exception.EasyORMException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

public class Logger{
	private  String which;
	private  File logFile;
	private static Set<LogMsgType> blockMsgType;
	
	private static Writer writer;
	private static StringBuilder msgBuilder;
	private static int sizeToWrite=200;
	private Timestamp sysTime;
	
	public <T>Logger(Class<T> cls){
		
		if(blockMsgType==null)
			blockMsgType = new HashSet<LogMsgType>();
		blockMsgType.add(LogMsgType.MSG_LOG_INFO);
		which = cls.getName();
		sysTime = new Timestamp(System.currentTimeMillis());
	}
	public static void setSizeToFlush(int size){
		sizeToWrite=size;
	}
	public static int getSizeToFlush(){
		return sizeToWrite;
	}
	public  String getLogFileName(){
		return logFile.getAbsolutePath();
	}

	public void setLogFileName(String file) throws EasyORMException{
		
		
		logFile = new File(file);
		logFile.setWritable(true);
		
		try {
			logFile.createNewFile();		
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile, true)));
			msgBuilder=new StringBuilder(sizeToWrite+100);	
		
		} catch (FileNotFoundException e) {
			throw new EasyORMException(e);
		} catch (IOException e) {
			throw new EasyORMException(e);
		}
		
	}
	public void flush() throws EasyORMException{
		if(writer!=null){
			try {
				writer.write(msgBuilder.toString());
				writer.close();
			} catch (IOException e) {
				throw new EasyORMException(e);
			}
		}
	}
	private synchronized void writeToFile(String what, LogMsgType msgType) throws EasyORMException{		
		String logType = LogMsgType.convertToString(msgType);
		try {	
			msgBuilder.append(logType+" - "+which+" :"+what+" at "+sysTime.toString()+ "\r\n");
			if(msgBuilder.length()>=sizeToWrite){
			writer.write(msgBuilder.toString());			
			writer.flush();
			msgBuilder=new StringBuilder(300);
			msgBuilder.setLength(0);
			
			}
			}  catch (IOException e) {
				throw new EasyORMException(e);
			}finally{
				
			}
		
	}
	public void debug(String msg) throws EasyORMException{

		if(this.isLogMsgTypePresent(LogMsgType.MSG_LOG_DEBUG))
			writeToFile(msg,LogMsgType.MSG_LOG_DEBUG);
			//System.out.println(which+": DEBUG-" + msg); 
	}
	public void info(String msg) throws EasyORMException{

		if(this.isLogMsgTypePresent(LogMsgType.MSG_LOG_INFO))
			writeToFile(msg,LogMsgType.MSG_LOG_INFO);
	}
	public void error(String msg) throws EasyORMException{

		if(this.isLogMsgTypePresent(LogMsgType.MSG_LOG_ERROR))
			writeToFile(msg,(LogMsgType.MSG_LOG_ERROR));
	}

	public boolean isLogMsgTypePresent(LogMsgType msgType){
		if(!blockMsgType.contains(msgType))
			return false;
		return true;
	}
	public void enableLogMsgType(LogMsgType msgType){
		blockMsgType.add(msgType);
	}
	public  void disbleLogMsgType(LogMsgType msgType){
		blockMsgType.remove(msgType);
	}
	public  void disbleAllLogMsgTypes(){
		blockMsgType.clear();
	}
	public  void enableAllLogMsgTypes(){
	
		blockMsgType.clear();	
		blockMsgType.add(LogMsgType.MSG_LOG_DEBUG);
		blockMsgType.add(LogMsgType.MSG_LOG_INFO);
		blockMsgType.add(LogMsgType.MSG_LOG_ERROR);//log	
	}
	public static enum LogMsgType {
		
		MSG_LOG_DEBUG,
		MSG_LOG_INFO,
		MSG_LOG_ERROR;
		
		private static String convertToString(LogMsgType msgType){
			
			switch(msgType){
			case MSG_LOG_DEBUG:
				return "DEBUG";
			case MSG_LOG_INFO:
				return "INFO";
			case MSG_LOG_ERROR:
				return "ERROR";
			}
			return "";
		}
		
	}
}