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

package ib.easyorm.annotation.util;

import ib.easyorm.annotation.TableInfo;
import ib.easyorm.exception.EasyORMException;


public class AnnotationUtil{

	 public static  <T>void checkTableAnnotation(Class<T> target) throws EasyORMException{
		if(!target.isAnnotationPresent(TableInfo.class)){
			throw new EasyORMException(EasyORMException.TABLE_ANNOTATION_MISSING);
		}
	}
	 public static  <T>void checkIdColumnAnnotation(Class<T> target) throws EasyORMException{
		if(!target.isAnnotationPresent(TableInfo.class)){
			throw new EasyORMException(EasyORMException.TABLE_ANNOTATION_MISSING);
		}
	}
	public static  <T>void checkAnnotations(Class<T> target) throws EasyORMException{
		AnnotationUtil.checkTableAnnotation(target);
		AnnotationUtil.checkIdColumnAnnotation(target);
	}
}