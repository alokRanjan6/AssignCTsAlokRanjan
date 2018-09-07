package OracleEngine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.ResourceBundle;
/**
* The OracleConnection creates database connection
* 
*
* @author  Alok Ranjan
* @version 1.0
* @since   2018-09-07 
*/
public class OracleConnection 
{
	Connection connection;	
	public OracleConnection() {
		connection = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} 
		catch(ClassNotFoundException ex) {
			ex.printStackTrace();
			return;
		}
		try {
			connection = DriverManager.getConnection("jdbc:oracle:thin:@"+getProperty("DB_IP")+":"+getProperty("DB_PORT")+":"+getProperty("DB_SID"), getProperty("DB_USER"), getProperty("DB_PASSWORD"));
			
		} 
		catch(SQLException ex) {
			ex.printStackTrace();
			return;
		}
		if(connection != null) 	{
		} 
		else {
			//log.info("Failed to make connection!");
		}
	}
	/**
	   * Returns Connection
	   *  @return Connection 
	   */
	public Connection getConnection(){
		return connection;
	}
	void CloseConnection(){
		try {
			if(connection != null) {
				connection.close();
				connection = null;
			}
		} 
		catch (SQLException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
    		//log.error(ex);
		}
	}
	/**
	   * This method reads Database credentials from property files
	   * @param queryID, Property file keys
	   * @return String The Value in Property file
	   */
	public static String getProperty(String queryID) {
		ResourceBundle resourceBundle = ResourceBundle.getBundle("connection");
		String queryString = resourceBundle.getString(queryID);
		return queryString;
	}

}


