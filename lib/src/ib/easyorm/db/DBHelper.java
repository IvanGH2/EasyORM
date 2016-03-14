package ib.easyorm.db;

import ib.easyorm.exception.EasyORMException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBHelper {
	static List<String> parseQueryForParams(String query) throws EasyORMException{

		List<String> paramsList = new ArrayList<String>();
		Pattern p=Pattern.compile("(:\\w+)");
		Matcher m=p.matcher(query);
		while(m.find()){
			paramsList.add(m.group().substring(1));
		}
		if(paramsList.size()==0) 
			throw new EasyORMException(EasyORMException.QUERY_PARAMS_MISSING);
		return paramsList;
	}
	static String replaceQueryParams(String query){
		String newQuery=query;
		Pattern p=Pattern.compile("(:\\w+)");
		Matcher m=p.matcher(query);
		newQuery=m.replaceAll("?");
		return newQuery;
	}
}
