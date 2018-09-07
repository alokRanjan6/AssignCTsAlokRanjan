package com.alok.JsonParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory; 
import org.codehaus.jackson.JsonGenerationException; 
import org.codehaus.jackson.JsonParser; 
import org.codehaus.jackson.JsonToken; 
import org.codehaus.jackson.map.JsonMappingException;

import com.fasterxml.jackson.core.JsonParseException;

import OracleEngine.OracleConnection;

/**
* The JsonParserClass parses the given jsons of a 
* file, calculates duration event and inserts 
* data into database
* 
*
* @author  Alok Ranjan
* @version 1.0
* @since   2014-09-07 
*/
public class JsonParserClass {

	 /**
	   * This method take json file as input reads it line by line 
	   * calls method that makes calculation for event duration
	   * and finally calls method to insert data in Database
	   * @param args[0] Path to json file
	   */
	public static void main(String[] args) throws JsonParseException, com.fasterxml.jackson.databind.JsonMappingException, IOException {
		
		if(args.length > 1) {
			System.out.println("Please try again after entering only path..");
			return;
		}
		if(args.length == 0) {
			System.out.println("Please try again after entering path..");
			return;
		}
		Map<String,ArrayList<String>> jasonValues = new HashMap<>(); //Will contain id as the key and rest of the values of json in ArrayList
		
		try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) { //read the Json one at a time, line by line
			String line;
			while ((line = br.readLine()) != null) {
				parseJson(line, jasonValues);// passes map for putting key=id, value=json data
			}
		}
		putInDataBase(jasonValues);//insert require data in Database
		System.out.println();
}

	 /**
	   * This method parses each line/json line by line
	   * calls a method actualLogic, where duration of event calculation is done
	   * @param line, each line of json file that corresponds to a json
	   * @param jasonValues, is the hashMap for putting key=id, value=json data in ArrayList
	   */
	private static void parseJson(String line, Map<String, ArrayList<String>> jasonValues) {
		String Id = "";
		ArrayList<String> tempData = new ArrayList<>();
		try { 
			JsonFactory jsonfactory = new JsonFactory(); 
			JsonParser parser = jsonfactory.createJsonParser(line); // starting parsing of JSON String 
			while (parser.nextToken() != JsonToken.END_OBJECT) { 
				String token = parser.getCurrentName(); 
				if ("id".equals(token)) { 
					parser.nextToken(); //next token contains value 
					String id = parser.getText(); //getting text field
					Id = id;
					System.out.println("id : " + id); 
				} if ("state".equals(token)) { 
					parser.nextToken(); 
					String state = parser.getText(); 
					System.out.println("state : " + state); 
					tempData.add(state);
				} if ("type".equals(token)) {
					parser.nextToken(); 
					String type = parser.getText(); 
					System.out.println("type : " + type); 
					tempData.add(type);
				} if ("host".equals(token)) { 

					parser.nextToken();  
					String host = parser.getText();
					System.out.println("host : " + host); 
					tempData.add(host);
				} 
				if ("timestamp".equals(token)) { 

					parser.nextToken();
					String timestamp = parser.getText(); 
					System.out.println("timestamp : " + timestamp); 
					tempData.add(timestamp);
				} 

			} 
			
			parser.close(); 
		} catch (JsonGenerationException jge) { 
			jge.printStackTrace(); 
		} catch (JsonMappingException jme) { 
			jme.printStackTrace(); 
		} catch (IOException ioex) { 
			ioex.printStackTrace();
		} 
		actuallogic(Id, tempData, jasonValues);
		System.out.println();
	
		
	}
	 /**
	   * This Method calculates duration of event
	   * puts data in Map that will have data in format (key=id, value=json data + duration)
	   * @param Id, id of the Json
	   * @param tempData, json data in Arraylist
	   * @param jasonValues, Map that will have data in format (key=id, value=json data + duration) at the end of this method
	   */
	private static void actuallogic(String Id, ArrayList<String> tempData, Map<String, ArrayList<String>> jasonValues) {
		int duration =0;
		//else, line 148 will get executed at first time
		if(jasonValues.containsKey(Id)) { //If map already has the event 
			if(jasonValues.get(Id).get(0).equals("STARTED")) { //if already present is STARTED then this means FINISHED has come 
				//substract FINISHED timestamp- STARTED timestamp
				duration = (int) (Long.parseLong(tempData.get(tempData.size()-1)) - Long.parseLong(jasonValues.get(Id).get(jasonValues.get(Id).size()-1))) ;
			}
			else { //This means FINISHED has come, and STARTED is in exixting map
				//substract FINISHED timestamp- STRTED timestamp
				duration = (int) (Long.parseLong(jasonValues.get(Id).get(jasonValues.get(Id).size()-1)) - Long.parseLong(tempData.get(tempData.size()-1)));
			}
			
			
			tempData.add(String.valueOf(duration)); //Arraylist populated with additional field, i.e duration
			
			jasonValues.put(Id, tempData); //populate map
		}
		else {
			jasonValues.put(Id, tempData);
		}
		
	}
	
	/**
	   * This Method Enters Data in Database, EventId, Event Duration, Type and Host if present, alert
	   * Reads data in Map that has  data in format (key=id, value=json data + duration)
	   * @param jasonValues, Map that now has data in format (key=id, value=json data + duration)
	   */
	
	private static void putInDataBase(Map<String, ArrayList<String>> jasonValues) {
		Connection connection = null;
		PreparedStatement ps = null;
		String query = "Insert into SYSTEMLOG values (?,?,?,?,?)";//EventId, Event Duration, Type , Host , alert
		
		try {
			connection = new OracleConnection().getConnection();
			ps = connection.prepareStatement(query);
			
			Iterator<String> iterator = jasonValues.keySet().iterator();
			int i =0;
		    while (iterator.hasNext()) {
		    	String key = (String) iterator.next();
		    	int duration = Integer.parseInt(jasonValues.get(key).get(jasonValues.get(key).size()-1));
				ps.setString(1, key);
				ps.setInt(2, duration);
				if(jasonValues.get(key).size() ==5) { //if json has Type and host
					ps.setString(3, jasonValues.get(key).get(1));
					ps.setString(4, jasonValues.get(key).get(2));
				} else {
					ps.setString(3, null);
					ps.setString(4, null);
				}
				if(duration > 4) { //populating the alert flag
					ps.setString(5, "True");
				} else {
					ps.setString(5, "False");
				}
				
				ps.addBatch();
				
				if(i%1000 == 0) 
					ps.executeBatch();
				
				i++;
			}
			ps.executeBatch(); //execute rest
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				connection.close();
			}catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
