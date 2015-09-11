package draxis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class InsertData {
	
	public static void main(String[] args) {
		
		try {
			
			String mssql_url = null;
			String mssql_database = null;
			String mssql_table = null;
			String mssql_username = null;
			String mssql_password = null;
			
			File fXmlFile = new File("confing.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			
			doc.getDocumentElement().normalize();
			
//			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			
			NodeList nList = doc.getElementsByTagName("MSSQL");
			
			
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				
//				System.out.println("\nCurrent Element:" + nNode.getNodeName());
						
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;
					
					mssql_url = eElement.getElementsByTagName("url").item(0).getTextContent();
					mssql_database = eElement.getElementsByTagName("database").item(0).getTextContent();
					mssql_table = eElement.getElementsByTagName("table").item(0).getTextContent();
					mssql_username = eElement.getElementsByTagName("username").item(0).getTextContent();
					mssql_password = eElement.getElementsByTagName("password").item(0).getTextContent();

				}
			}			
			
			List<String> csvListVariables = new ArrayList<String>();
			NodeList nList1 = doc.getElementsByTagName("variable");
			for (int temp = 0; temp < nList1.getLength(); temp++) {					
				Element eElement = (Element) nList1.item(temp);
				if (eElement.getParentNode().getNodeName().equals("SourceCSVfields")) {
					csvListVariables.add(eElement.getTextContent());
				}
			}
			
			List<String> destinationTableFields = new ArrayList<String>();
			NodeList nList2 = doc.getElementsByTagName("field");
			for (int temp = 0; temp < nList1.getLength(); temp++) {					
				Element eElement = (Element) nList2.item(temp);
				if (eElement.getParentNode().getNodeName().equals("DestinationTableFields")) {
					destinationTableFields.add(eElement.getTextContent());
				}
			}
			
			insertData(mssql_url,mssql_database,mssql_table,mssql_username,mssql_password, csvListVariables, destinationTableFields, args[0]);
			
		} catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void insertData(String mssql_url, String mssql_database,
			String mssql_table, String mssql_username, String mssql_password,
			List<String> csvListVariables, List<String> destinationTableFields, String csvdata) {
		// TODO Auto-generated method stub
		
		List<List<String>> datalist = readCSV(csvListVariables, csvdata);
		
		try {
			try {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} 
			
			String url = "jdbc:sqlserver://" + mssql_url + ";databaseName=" + mssql_database; 
	        Connection conn = DriverManager.getConnection(url,mssql_username,mssql_password); 
	        Statement st = conn.createStatement();
	        
//	        String pattern = "yyyy-MM-dd HH:mm";
//	        SimpleDateFormat format = new SimpleDateFormat(pattern);
	        
	        for (int i = 0; i < datalist.size(); i++) {
	        	
	        	String select_sql = "SELECT * FROM " + mssql_table + " WHERE ";
	        	
	        	for (int j = 0; j < datalist.get(0).size() - 1; j++){
	        	
	        		boolean isWhitespace = datalist.get(i).get(j).contains(" ");
	        					        		
	        		if(j == datalist.get(0).size() - 2)
	        			if(isWhitespace)
	        				select_sql = select_sql + destinationTableFields.get(j) + "='" + datalist.get(i).get(j) + "' ";
	        			else
	        				select_sql = select_sql + destinationTableFields.get(j) + "=" + datalist.get(i).get(j) + " ";
	        		else
	        			if(isWhitespace)
	        				select_sql = select_sql + destinationTableFields.get(j) + "='" + datalist.get(i).get(j) + "' AND ";
	        			else
	        				select_sql = select_sql + destinationTableFields.get(j) + "=" + datalist.get(i).get(j) + " AND ";
	        	}
	        		        	
	        	ResultSet Results = st.executeQuery(select_sql);
	        	
	        	int add = 1;
	        	
	        	String recordexists = "Record: ";
	        	for (int j = 0; j < datalist.get(0).size(); j++){
	        		if(j == datalist.get(0).size() - 1)
	        			recordexists = recordexists + destinationTableFields.get(j) + "=" + datalist.get(i).get(j) + " exists";
	        		else
	        			recordexists = recordexists + destinationTableFields.get(j) + "=" + datalist.get(i).get(j) + " ";
	        	}
	        		        	
	        	while(Results.next()){
	        		add = 0;
	        		System.out.println(recordexists);
	        	}
	        	
	        	String add_sql = "INSERT INTO " + mssql_table + " (";	        	
	        	if(add == 1){
	        		for (int j = 0; j < datalist.get(0).size(); j++){
	        			if(j == datalist.get(0).size() - 1)
	        				add_sql = add_sql + "[" + destinationTableFields.get(j) + "]) VALUES (";
	        			else
	        				add_sql = add_sql + "[" + destinationTableFields.get(j) + "],";
	        		}
	        		for (int j = 0; j < datalist.get(0).size(); j++){
	    	        	
		        		boolean isWhitespace = datalist.get(i).get(j).contains(" ");
		        					        		
		        		if(j == datalist.get(0).size() - 1)
		        			if(isWhitespace)
		        				add_sql = add_sql + "'" + datalist.get(i).get(j) + "') ";
		        			else
		        				add_sql = add_sql + datalist.get(i).get(j) + ") ";
		        		else
		        			if(isWhitespace)
		        				add_sql = add_sql + "'" + datalist.get(i).get(j) + "',";
		        			else
		        				add_sql = add_sql + datalist.get(i).get(j) + ",";
		        	}
	        		
	        		System.out.println(add_sql);	        		
		        
		        	st.executeUpdate(add_sql);
		        	
		        	System.out.println("Success!");
	        	}  
	        	
	        }
	        	        
	        conn.close(); 
	        
	        System.out.println("No errors! - Success!");
	        
		} catch (SQLException e) {  
	        System.err.println(e.getMessage()); 
	    } 		
	}
	
	private static List<List<String>> readCSV(List<String> csvListVariables,
			String csvdata) {

		List<List<String>> csvDataList = new ArrayList<List<String>>(500);
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(new File(csvdata)));
			String line;
		    
			while ((line = br.readLine()) != null) {				
				String[] entries = line.split(",");
				csvDataList.add(Arrays.asList(entries));
				
			}
		    
		    br.close();
		    
		} catch(IOException ioe){
			System.err.println(ioe.getMessage()); 
		}
						
		return csvDataList;
	}
	
}
