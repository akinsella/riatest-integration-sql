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
package org.helyx.riatest.sql;

import static org.helyx.riatest.sql.util.DbUtil.close;
import static org.helyx.riatest.sql.util.DbUtil.convertResultSetToString;
import static org.helyx.riatest.sql.util.DbUtil.deregisterDriver;
import static org.helyx.riatest.sql.util.LogUtil.printException;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.helyx.riatest.sql.QueryType;
import org.helyx.riatest.sql.SqlQueryExecutor;


public class SqlQueryExecutor {

	private String url;
	private String driver;
	
	private Driver driverClass;
	
	private Connection connection;
	
	public SqlQueryExecutor(String url, String driver) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.url = url;
		this.driver = driver;
	}
	
	public void init() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {	
		driverClass = (Driver)Class.forName(driver).newInstance();
		DriverManager.registerDriver(driverClass);
	}
	
	public void dispose() {	
		close(connection);
		deregisterDriver(driverClass);
	}
	
	private Connection getConnection() throws SQLException {
		if (connection == null) {
			connection = DriverManager.getConnection(url);
		}

		return connection;
	}

	public String executeQuery(String query) throws Exception {
		
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = getConnection().createStatement();
			resultSet = statement.executeQuery(query);
			String result = convertResultSetToString(resultSet);
			
			return result;
		}
		finally {
			close(resultSet);
			close(statement);
		}
	}

	public int executeUpdate(String query) throws Exception {
		
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			Connection connection = getConnection();
			connection.setAutoCommit(true);
			statement = connection.createStatement();
			int result = statement.executeUpdate(query);
			
			return result;
		}
		finally {
			close(resultSet);
			close(statement);
		}
	}
	
	/**
	 * @param args
	 * @throws FileNotFoundException
	 * @throws Exception
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 4) {
				throw new IllegalArgumentException(
						"The absolute file path to the properties file containing the configuration details was not specified.");
			}

			final String url = args[0];
			final String driver = args[1];
			final QueryType queryType = QueryType.valueOf(args[2]);
			final String query = args[3];

			SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor(url, driver);
			
			try {
				sqlQueryExecutor.init();
				switch(queryType) {
					case SELECT:
						System.out.print(sqlQueryExecutor.executeQuery(query));
						break;
					case INSERT:
					case UPDATE:
					case DELETE:
						System.out.print(sqlQueryExecutor.executeUpdate(query));
						break;
					default:
						throw new IllegalAccessException("Query Type '" + queryType.name() + "' is not currently supported");
				}
			}
			finally {
				sqlQueryExecutor.dispose();
			}
			
		} catch (Exception e) {
			printException(e);
			System.exit(1);
		}
	}


}
