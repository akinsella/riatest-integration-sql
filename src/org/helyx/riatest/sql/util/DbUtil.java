package org.helyx.riatest.sql.util;
/**
 * Copyright (C) 2011 Alexis Kinsella - http://www.helyx.org - <Helyx.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.helyx.riatest.sql.util.LogUtil.printException;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class DbUtil {
	
	private static final String COLUMN_SEPARATOR = "|";
	private static final String ROW_SEPARATOR = ",";
	
	private DbUtil() {
		super();
	}
	
	public static String convertResultSetToString(ResultSet rs) throws SQLException {
		StringBuffer sb = new StringBuffer();
		boolean hasNext = rs.next();
		while (hasNext) {
			sb.append(convertCurrentRowToString(rs));
			hasNext = rs.next();
			if (hasNext) {
				sb.append(ROW_SEPARATOR);
			}
		}

		return sb.toString();
	}
	
	private static String convertCurrentRowToString(ResultSet rs) throws SQLException {
		StringBuffer sb = new StringBuffer();
	    ResultSetMetaData rsmd = rs.getMetaData();
	    int numColumns = rsmd.getColumnCount();
	    // Get the column names; column indices start from 1
	    for (int i = 1 ; i <= numColumns ; i++) {
	        String columnName = rsmd.getColumnName(i);
	        String columnValue = rs.getString(columnName);
			sb.append(columnValue);
			if (i < numColumns) {
				sb.append(COLUMN_SEPARATOR);
			}
		}
	    
		return sb.toString();
	}

	public static void deregisterDriver(Driver driverClass) {
		try {
			if (driverClass != null) {
				DriverManager.deregisterDriver(driverClass);
			}
		}
		catch(Exception e) {
			printException(e);
		}
	}
	
	public static void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			}
			catch(Exception e) { 
				printException(e);
			}
		}
	}
	
	public static void close(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			}
			catch(Exception e) { 
				printException(e);
			}
		}
	}
	
	public static void close(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			}
			catch(Exception e) { 
				printException(e);
			}
		}
	}

}
