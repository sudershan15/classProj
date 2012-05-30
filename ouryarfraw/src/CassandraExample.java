

import java.util.*;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class CassandraExample {

	//The string serializer translates the byte[] to and from String using utf-8 encoding
    private static StringSerializer stringSerializer = StringSerializer.get();
	
    public static void insertData(String line) {
    	try
    	{
     		//Create a cluster object from your existing Cassandra cluster
            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
            
            //Create a keyspace object from the existing keyspace we created using CLI
            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
            
            //Create a mutator object for this keyspace using utf-8 encoding
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            if(line.startsWith("Date"))
            {
            	int length=line.length();
				String myline=line.substring(5, length);
            //Use the mutator object to insert a column and value pair to an existing key
            mutator.insert("0", "Articles", HFactory.createStringColumn("Date", myline));
            }
            else if(line.startsWith("Title"))
            {
            	int length=line.length();
				String myline=line.substring(7, length);
            mutator.insert("0", "Articles", HFactory.createStringColumn("Title", myline));
            }
            else if(line.startsWith("URL"))
            {
            	int length=line.length();
				String myline=line.substring(4, length);
            mutator.insert("0", "Articles", HFactory.createStringColumn("Url", myline));
            }
            else if(line.startsWith("Summary"))
            {
            	int length=line.length();
				String myline=line.substring(8, length);
            mutator.insert("0", "Articles", HFactory.createStringColumn("Summary", myline));
            }
            else if(line.startsWith("Text"))
            {
            	int length=line.length();
				String myline=line.substring(5, length);
            	 mutator.insert("0", "Articles", HFactory.createStringColumn("Text", myline));
            }
            else if(line.startsWith("DocId"))
            {
            	int length=line.length();
				String myline=line.substring(6, length);
            	 mutator.insert("0", "Articles", HFactory.createStringColumn("DocId", myline));
            
            }
            System.out.println("Data Inserted");
            System.out.println();
    	} 
    	catch (Exception ex) 
    	{
    		System.out.println("Error encountered while inserting data!!");
    		ex.printStackTrace() ;
    	}
    }
    
    @SuppressWarnings("unchecked")
	public static void retrieveData() 
    {
    	try {
    		//Create a cluster object from your existing Cassandra cluster
            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
            
            //Create a keyspace object from the existing keyspace we created using CLI
            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
    		isq.addGteExpression("Date","2012-03-01 ");
    		isq.addLteExpression("Date", "2012-03-20" );
    		isq.addEqualsExpression("Symbol", "GOOG");
    		//isq.addEqualsExpression("Date",	 "4/10/2010");
    		//isq.setStartKey("4/13/2010");
    		 
    	 
    		//isq.addEqualsExpression("Date", "4/13/2010");
    		//isq.addEqualsExpression("Date", "4/10/2010");
    		isq.setColumnNames("Symbol","Date","Open","Low","High","Close");
    		isq.setColumnFamily("mystockdata");
    		//isq.setStartKey("");
            //isq.setRange("", "", true, 4);
        /*QueryResult<OrderedRows<String, String, String>> result = isq.execute();
       	isq.setStartKey("");
      
        String Address =result.get().iterator().next().getColumnSlice().getColumnByName("Address").getValue();
        String City =result.get().iterator().next().getColumnSlice().getColumnByName("City").getValue();
        String Phone =result.get().iterator().next().getColumnSlice().getColumnByName("Phone").getValue();
       // String State =result.get().iterator().next().getColumnSlice().getColumnByName("State").getValue();
        String myresult=row+","+Address+","+City+","+Phone;
       // System.out.println(myresult);
        System.out.println("\nInserted data is as follows:\n" +myresult);
		System.out.println();
	}   catch (Exception ex) {
		System.out.println("Error encountered while retrieving data!!");
		ex.printStackTrace() ;
	}*/
    		
    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
           	isq.setStartKey("");
            OrderedRows<String, String, String> rows = result.get();
           
           	Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
           
            {
            while (rowsIterator.hasNext()) 
            {
                Row<String, String, String> row = rowsIterator.next();
                String row1=row.getKey();
                String Symbol=row.getColumnSlice().getColumnByName("Symbol").getValue();
                String Date=row.getColumnSlice().getColumnByName("Date").getValue();
                String Open=row.getColumnSlice().getColumnByName("Open").getValue();
                String Low=row.getColumnSlice().getColumnByName("Low").getValue();
                String High=row.getColumnSlice().getColumnByName("High").getValue();
                String Close=row.getColumnSlice().getColumnByName("Close").getValue();
                //String Volume=row.getColumnSlice().getColumnByName("Volume").getValue();
                String last_key=Symbol+","+Date+","+Open+","+Low+","+High+","+Close+",";
           		System.out.println("\nInserted data is as follows:\n" + last_key);
            }            
        
            }   
    	}  
    	catch (Exception ex) 
    	{
    		System.out.println("Error encountered while retrieving data!!");
    		ex.printStackTrace() ;
    	}
    } 
	 
    
    public static void updateData() 
    {
    	try {

    		//Create a cluster object from your existing Cassandra cluster
            Cluster cluster = HFactory.getOrCreateCluster("Test Sample", "localhost:9160");
            
            //Create a keyspace object from the existing keyspace we created using CLI
            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
            
          //Create a mutator object for this keyspace using utf-8 encoding
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            
            //Use the mutator object to update a column and value pair to an existing key
           // mutator.insert("sample", "authCollection", HFactory.createStringColumn("username", "administrator"));
            
            //Check if data is updated
            MultigetSliceQuery<String, String, String> multigetSliceQuery = HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
            multigetSliceQuery.setColumnFamily("mystockdata");
            multigetSliceQuery.setKeys("Date");
        
            //The 3rd parameter returns the columns in reverse order if true
            //The 4th parameter in setRange determines the maximum number of columns returned per key
            multigetSliceQuery.setRange(null, null, false, 5);
            QueryResult<Rows<String, String, String>> result = multigetSliceQuery.execute();
            String myresult=result.get().getByKey("Date").getColumnSlice().getColumnByName("Date").getValue();
            System.out.println(myresult);
    		
    	} catch (Exception ex) {
    		System.out.println("Error encountered while updating data!!");
    		ex.printStackTrace() ;
    	}
    }

    public static void deleteData() {
    	try {
    
    		//Create a cluster object from your existing Cassandra cluster
    	       Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
    	       
    	       //Create a keyspace object from the existing keyspace we created using CLI
    	       Keyspace keyspace = HFactory.createKeyspace("AuthDB", cluster);
    	       
    	     //Create a mutator object for this keyspace using utf-8 encoding
    	       Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
    	       
    	       //Use the mutator object to delete row
    	       mutator.delete("sample", "authCollection",null, stringSerializer);
    	       
    	       System.out.println("Data Deleted!!");
    	       
    	       //try to retrieve data after deleting
    	       SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
    	       sliceQuery.setColumnFamily("authCollection").setKey("sample");
    	       sliceQuery.setRange("", "", false, 4);
    	       
    	       QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute(); 
    	       System.out.println("\nTrying to Retrieve data after deleting the key 'sample':\n" + result.get());
    	       
    	       //close connection
    	       cluster.getConnectionManager().shutdown();
    
		} catch (Exception ex) {
			System.out.println("Error encountered while deleting data!!");
			ex.printStackTrace() ;
		}
	}
    
    
	public static void main(String[] args) 
	{
		
		//insertData() ;
		// retrieveData();
		//updateData() ;
		//deleteData() ;
        
	}
	
	
}
