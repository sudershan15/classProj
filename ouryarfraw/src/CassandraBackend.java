import java.util.*;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

 
 
public class CassandraBackend  implements Comparator 
{
	private static StringSerializer stringSerializer = StringSerializer.get();
	private Object weight;
	
/***********************************************insert data**************************************/
	 public void insertData(String rowkey,String date,String Symbol,String prediction,String change) 
	 {
	    	try
	    	{
	     		//Create a cluster object from your existing Cassandra cluster
	            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	            
	            //Create a mutator object for this keyspace using utf-8 encoding
	            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
	    		
	            //Use the mutator object to insert a column and value pair to an existing key
	            mutator.insert(rowkey, "stockchange", HFactory.createStringColumn("Date",date));
	            mutator.insert(rowkey, "stockchange", HFactory.createStringColumn("Symbol", Symbol));
	            mutator.insert(rowkey, "stockchange", HFactory.createStringColumn("Change", change));
	            mutator.insert(rowkey, "stockchange", HFactory.createStringColumn("result", prediction));
	            
	            System.out.println("Data Inserted");
	            System.out.println();
	    	} 
	    	catch (Exception ex) 
	    	{
	    		System.out.println("Error encountered while inserting data!!");
	    		ex.printStackTrace() ;
	    	}
	    }
	 /*********************************insert weights and counts*********************************/
	 public void insertweightscounts(String rowkey,String date,String weight,String count) 
	 {
	    	try
	    	{
	     		//Create a cluster object from your existing Cassandra cluster
	            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	            
	            //Create a mutator object for this keyspace using utf-8 encoding
	            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
	    		
	            //Use the mutator object to insert a column and value pair to an existing key
	            mutator.insert(rowkey, "weightscounts", HFactory.createStringColumn("Date",date));
	            mutator.insert(rowkey, "weightscounts", HFactory.createStringColumn("weight", weight));
	            mutator.insert(rowkey, "weightscounts", HFactory.createStringColumn("count", count));
	           
	            
	            System.out.println("Data Inserted");
	            System.out.println();
	    	} 
	    	catch (Exception ex) 
	    	{
	    		System.out.println("Error encountered while inserting data!!");
	    		ex.printStackTrace() ;
	    	}
	    }
	 /**********************************insert keyword****************************************/
	 public void insertKeywords(String date,String keyword,String count,String symbol) 
	 {
		 String rowkey=date+keyword;
		 
	    	try
	    	{
	     		//Create a cluster object from your existing Cassandra cluster
	            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	            
	            //Create a mutator object for this keyspace using utf-8 encoding
	            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
	    		
	            //Use the mutator object to insert a column and value pair to an existing key
	            mutator.insert(rowkey, "mykeywords", HFactory.createStringColumn("symbol",symbol));
	            mutator.insert(rowkey, "mykeywords", HFactory.createStringColumn("theDate",date));
	            mutator.insert(rowkey, "mykeywords", HFactory.createStringColumn("thewords", keyword));
	            mutator.insert(rowkey, "mykeywords", HFactory.createStringColumn("itsCount", count));
	            System.out.println("Data Inserted");
	            System.out.println();
	    	} 
	    	catch (Exception ex) 
	    	{
	    		System.out.println("Error encountered while inserting data!!");
	    		ex.printStackTrace() ;
	    	}
	    }
	
/*********retrives historical data based on a particular date range	*******/
	
	@SuppressWarnings({ "rawtypes", "unused" })
	public List<String> retrieveData(String startdate,String enddate,String symbol)  
    {
    	ArrayList  mydata=new ArrayList();
    	ArrayList last_key=new ArrayList();
    	try {
    		
    		//Create a cluster object from your existing Cassandra cluster
            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
            
            //Create a keyspace object from the existing keyspace we created using CLI
            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
    		isq.addGteExpression("Date",startdate);
    		isq.addLteExpression("Date", enddate );
    		isq.addEqualsExpression("Symbol", symbol);
    		isq.setColumnNames("Symbol","Date","Open","Low","High","Close");
    		isq.setColumnFamily("mystockdata");
    		ArrayList[] main_list = new ArrayList[] {new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList()};
    		//String rowkey="";
    		String Date="";
    		String Low="";
    		String Close="";
    		String Open="";
    		String High="";
    		String Symbol="";
    		String Volume="";
    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
           	isq.setStartKey("");
            OrderedRows<String, String, String> rows = result.get();
          
            int i = 0;
           	Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
           
            {
            	ArrayList<String> dt=new ArrayList<String>();
            	ArrayList<String> dt1=new ArrayList<String>();
            	while (rowsIterator.hasNext()) 
            {
            		
            	Row<String, String, String> row = rowsIterator.next();
                String row1=row.getKey().toString();   
                Symbol=row.getColumnSlice().getColumnByName("Symbol").getValue();
                Date=row.getColumnSlice().getColumnByName("Date").getValue();
                dt.add(Date);
                dt1.add(Date);
          		Open=row.getColumnSlice().getColumnByName("Open").getValue();
                Low=row.getColumnSlice().getColumnByName("Low").getValue();
            	High=row.getColumnSlice().getColumnByName("High").getValue();
            	Close=row.getColumnSlice().getColumnByName("Close").getValue();
            	//Volume=row.getColumnSlice().getColumnByName("Volume").getValue();
            	     	main_list[i].add(Symbol);
            	     	main_list[i].add(Date);
            	     	main_list[i].add(Open);
            	     	main_list[i].add(Low);
            	     	main_list[i].add(High);
            	     	main_list[i].add(Close);
            	     	//main_list[i].add(Volume);
            	     	main_list[i].add(row1);
            	     	
            	     	last_key.add(main_list[i]);
            	     	
            	     	i++;
                    
            		
            	}
            	
            }
          
    	}
    	
           catch (Exception ex) 
    	{
    		System.out.println("Error encountered while retrieving data!!");
    		ex.printStackTrace() ;
    	}
		return last_key;
    } 
	 public static class ArrayList2DComarpator implements Comparator {  
         public int compare(Object obj1, Object obj2) {  
             if (! (obj1 instanceof ArrayList) || ! (obj2 instanceof ArrayList)) {  
                 throw new ClassCastException(  
                             "compared objects must be instances of ArrayList");  
             }  
             String date1 = (String) ((ArrayList) obj1).get(1);  
             String date2 = (String) ((ArrayList) obj2).get(1);  
             return date1.compareTo(date2);  
         }  
     }
	 /**********************retrieving stock change data ****************************************/
	 public List<String> retrievechangeData(String startdate,String enddate,String symbol)  
	    {
	    	ArrayList  mydata=new ArrayList();
	    	ArrayList last_key=new ArrayList();
	    	try {
	    		
	    		//Create a cluster object from your existing Cassandra cluster
	            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
	    		isq.addGteExpression("Date",startdate);
	    		isq.addLteExpression("Date", enddate );
	    		isq.addEqualsExpression("Symbol", symbol);
	    		isq.setColumnNames("Change","Date","Symbol");
	    		isq.setColumnFamily("stockchange");
	    		ArrayList[] main_list = new ArrayList[] {new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList()};
	    		//String rowkey="";
	    		String Date="";
	    		String Change="";
	    		String Symbol="";
	    		 
	    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
	           	isq.setStartKey("");
	            OrderedRows<String, String, String> rows = result.get();
	          
	            int i = 0;
	           	Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
	           
	            {
	            	ArrayList<String> dt=new ArrayList<String>();
	            	ArrayList<String> dt1=new ArrayList<String>();
	            	while (rowsIterator.hasNext()) 
	            {
	            		
	            	Row<String, String, String> row = rowsIterator.next();
	                String row1=row.getKey().toString();   
	                Symbol=row.getColumnSlice().getColumnByName("Symbol").getValue();
	                Date=row.getColumnSlice().getColumnByName("Date").getValue();
	                dt.add(Date);
	                dt1.add(Date);
	          		Change=row.getColumnSlice().getColumnByName("Change").getValue();
	                
	            	//Volume=row.getColumnSlice().getColumnByName("Volume").getValue();
	            	     	main_list[i].add(Symbol);
	            	     	main_list[i].add(Date);
	            	     	main_list[i].add(Change);
	            	     	 
	            	     	//main_list[i].add(Volume);
	            	     	main_list[i].add(row1);
	            	     	
	            	     	last_key.add(main_list[i]);
	            	     	
	            	     	i++;
	                    
	            		
	            	}
	            	
	            }
	          
	    	}
	    	
	           catch (Exception ex) 
	    	{
	    		System.out.println("Error encountered while retrieving data!!");
	    		ex.printStackTrace() ;
	    	}
			return last_key;
	    } 
	 /***************************************extract keywords weight************************************/
	 public Double retrievewordsweight(String word)  
	    {
		 	String myword=word;
		 	Double weight = null;
		 	String row1;
	       		//Create a cluster object from your existing Cassandra cluster
		   		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
	    		isq.setColumnFamily("listkeywords");
	    		isq.addEqualsExpression("KeyWord", myword);
	    		isq.setColumnNames("Weight","Weight");
	    		//ArrayList[] main_list = new ArrayList[] {new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList()};
	    		//String rowkey="";
	    		String kword="";
	    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
	           	isq.setStartKey("");
	            OrderedRows<String, String, String> rows = result.get();
	            int i = 0;
	           	Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
	           	while (rowsIterator.hasNext()) 
	            {
	           		Row<String, String, String> row = rowsIterator.next();
	                row1=row.getKey().toString();   
	              // kword=row.getColumnSlice().getColumnByName("KeyWord").getValue();
	                weight=Double.parseDouble(row.getColumnSlice().getColumnByName("Weight").getValue());
	                //ws=weight;
	                
	            }
	           	
	           	return weight;
	    	/*   catch (Exception ex) 
	    	   {
	    		   System.out.println("Error encountered while retrieving data!!");
	    		   ex.printStackTrace() ;
	    	   }
		   		return ws;*/
		} 
		/******************************************************retrieving weights and counts************************/
	 public List<String> retrievewordsweightcount(String date)  
	    {
		 ArrayList<String> wc=new ArrayList<String>();
		 	String date1=date;
		 	 
		 	String row1;
	       		//Create a cluster object from your existing Cassandra cluster
		   		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
	    		isq.setColumnFamily("weightscounts");
	    		isq.addEqualsExpression("Date", date1);
	    		isq.setColumnNames("weight","count","Date");
	    		//ArrayList[] main_list = new ArrayList[] {new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList()};
	    		//String rowkey="";
	    	 
	    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
	           	isq.setStartKey("");
	            OrderedRows<String, String, String> rows = result.get();
	            int i = 0;
	           	Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
	           	while (rowsIterator.hasNext()) 
	            {
	           		Row<String, String, String> row = rowsIterator.next();
	                row1=row.getKey().toString();   
	               String Date=row.getColumnSlice().getColumnByName("Date").getValue();
	                String weight= row.getColumnSlice().getColumnByName("weight").getValue();
	                String count=row.getColumnSlice().getColumnByName("count").getValue();
	                String ws=weight+","+count+","+Date;
	                wc.add(ws);
	                
	            }
	           	
	           	return wc;
	    	 
		} 

		  
	 /**********************************retrieve the keywords *************************************/
	 public List<String> retrievewords()  
	    {
		 String startdate="2012-03-01";
		String enddate="2012-03-03";
		 ArrayList  mydata=new ArrayList();
	    	ArrayList last_key=new ArrayList();
	    	try {
	    		
	    		//Create a cluster object from your existing Cassandra cluster
	            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
	            
	            //Create a keyspace object from the existing keyspace we created using CLI
	            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
	    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
	    		isq.addGteExpression("theDate",startdate);
	    		isq.addLteExpression("theDate", enddate );
	    		 isq.addEqualsExpression("symbol", "IBM");
	    		isq.setColumnNames("theDate","itsCount","thewords");
	    		isq.setColumnFamily("mykeywords");
	    		ArrayList[] main_list = new ArrayList[] {new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList(),new ArrayList()};
	    		//String rowkey="";
	    		String Date="";
	    		String Words="";
	    		String Count="";
	    		
	    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
	           	isq.setStartKey("");
	            OrderedRows<String, String, String> rows = result.get();
	          
	            int i = 0;
	           	Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
	           
	            {
	            	ArrayList<String> dt=new ArrayList<String>();
	            	ArrayList<String> dt1=new ArrayList<String>();
	            	while (rowsIterator.hasNext()) 
	            {
	            		
	            	Row<String, String, String> row = rowsIterator.next();
	                String row1=row.getKey().toString();   
	               Count=row.getColumnSlice().getColumnByName("itsCount").getValue();
	                Date=row.getColumnSlice().getColumnByName("theDate").getValue();
	                dt.add(Date);
	                dt1.add(Date);
	          		Words=row.getColumnSlice().getColumnByName("thewords").getValue();
	                 
	            	//Volume=row.getColumnSlice().getColumnByName("Volume").getValue();
	            	     	main_list[i].add(Count);
	            	     	main_list[i].add(Date);
	            	     	main_list[i].add(Words);
	            	     	 
	            	     	//main_list[i].add(Volume);
	            	     	main_list[i].add(row1);
	            	     	
	            	     	last_key.add(main_list[i]);
	            	     	
	            	     	i++;
	                    
	            		
	            	}
	            	
	            }
	          
	    	}
	    	
	           catch (Exception ex) 
	    	{
	    		System.out.println("Error encountered while retrieving data!!");
	    		ex.printStackTrace() ;
	    	}
			return last_key;
	    } 
		
	
/***************retrive all historical data of a ticker *******************/
	
	public List<String> retrieveAllData(String symbol)
	{
		List<String> mystocks=new ArrayList<String>();
		try {
    		//Create a cluster object from your existing Cassandra cluster
            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
            
            //Create a keyspace object from the existing keyspace we created using CLI
            Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
    		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
    		 //sets the symbol here
    		isq.addEqualsExpression("Symbol", symbol);
    		isq.setColumnNames("Symbol","Date","Open","Low","High","Close","Volume");
    		isq.setColumnFamily("mystockdata");
    		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
           	isq.setStartKey("");
            OrderedRows<String, String, String> rows = result.get();
            Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
            while (rowsIterator.hasNext()) 
            {
                Row<String, String, String> row = rowsIterator.next();
                String rowkey=row.getKey();
                String Symbol=row.getColumnSlice().getColumnByName("Symbol").getValue();
                String Date=row.getColumnSlice().getColumnByName("Date").getValue();
                String Open=row.getColumnSlice().getColumnByName("Open").getValue();
                String Low=row.getColumnSlice().getColumnByName("Low").getValue();
                String High=row.getColumnSlice().getColumnByName("High").getValue();
                String Close=row.getColumnSlice().getColumnByName("Close").getValue();
               // String Volume=row.getColumnSlice().getColumnByName("Volume").getValue();
                String last_key=Symbol+","+Date+","+Open+","+Low+","+High+","+Close+","+rowkey;
           		//System.out.println("\nInserted data is as follows:\n" + last_key);
                mystocks.add(last_key);
            }            
        
              
    	}  
    	catch (Exception ex) 
    	{
    		System.out.println("Error encountered while retrieving data!!");
    		ex.printStackTrace() ;
    	}
		return mystocks;
		
	}
	
/**********************retrive real time data for a particular date*********************************/
	
	public List<String> retrieveRealTime(String date)
	{
		List<String> mystocks=new ArrayList<String>();
		try {
		//Create a cluster object from your existing Cassandra cluster
        Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
        
        //Create a keyspace object from the existing keyspace we created using CLI
        Keyspace keyspace = HFactory.createKeyspace("finalproject", cluster);
		IndexedSlicesQuery<String, String, String> isq = HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
		 //sets the symbol here
		isq.addEqualsExpression("Date", "2011-08-05 00:00:00-0700");
		isq.setColumnNames("Symbol","Date","Open","Low","High","Close","Volume");
		isq.setColumnFamily("mystockdata");
		QueryResult<OrderedRows<String, String, String>> result = isq.execute();
       	isq.setStartKey("");
        OrderedRows<String, String, String> rows = result.get();
        Iterator<Row<String, String, String>> rowsIterator =rows.iterator();
        while (rowsIterator.hasNext()) 
        {
            Row<String, String, String> row = rowsIterator.next();
          
            String Symbol=row.getColumnSlice().getColumnByName("Symbol").getValue();
            String Date=row.getColumnSlice().getColumnByName("Date").getValue();
            String Open=row.getColumnSlice().getColumnByName("Open").getValue();
            String Low=row.getColumnSlice().getColumnByName("Low").getValue();
            String High=row.getColumnSlice().getColumnByName("High").getValue();
            String Close=row.getColumnSlice().getColumnByName("Close").getValue();
            String Volume=row.getColumnSlice().getColumnByName("Volume").getValue();
            String last_key=Symbol+","+Date+","+Open+","+Low+","+High+","+Close+","+Volume;
       		System.out.println("\nInserted data is as follows:\n" + last_key);
            mystocks.add(last_key);
        }            
    
          
	}  
	catch (Exception ex) 
	{
		System.out.println("Error encountered while retrieving data!!");
		ex.printStackTrace() ;
	}
		return mystocks;
	
}
		
	 
	@SuppressWarnings("unchecked")
	public static void main(String args[])
	{
		CassandraBackend b=new CassandraBackend();
		 
		 
	/* List<String> newresult=new ArrayList<String>();
		 
		 ArrayList result=(ArrayList)  b.retrievewords();
			
			 
           Collections.sort(result, new ArrayList2DComarpator()); 
             for(int j = 0; j < result.size(); j++)
             {
     			ArrayList myRow   = (ArrayList) result.get(j);
         	 
         			String count=myRow.get(0).toString();
         			String words=myRow.get(1).toString() ;
         			String date=myRow.get(2).toString() ;
         			 
         		 
         			String mydata= count+","+date+","+words;
         			newresult.add(mydata);
         		
             }
			  Iterator i=newresult.listIterator();
			  while(i.hasNext())
			  {
				  System.out.println(i.next());
			  }*/
        
		List<String> newresult=new ArrayList<String>();
		 
		 ArrayList result=(ArrayList) b.retrieveData("2012-03-01","2012-04-01","IBM");
			
			 
           Collections.sort(result, new ArrayList2DComarpator()); 
             for(int j = 0; j < result.size(); j++)
             {
     			ArrayList myRow   = (ArrayList) result.get(j);
         	 
         			String symbol=myRow.get(0).toString();
         			String date=myRow.get(1).toString() ;
         			String open=myRow.get(2).toString() ;
         			String low=myRow.get(3).toString();
         			String high=myRow.get(4).toString();
         			String close=myRow.get(5).toString();
         			String rowkey=symbol+date;
         			String mydata=symbol+","+date+","+open+","+low+","+high+","+close+","+rowkey;
         			newresult.add(mydata);
         		
             }
			  Iterator i=newresult.listIterator();
			  while(i.hasNext())
			  {
				  System.out.println(i.next());
			  }/*
			 //Double wresult= b.retrievewordsweight("Kurds");
			 //System.out.println(wresult);
			 // ArrayList<String> countresult=(ArrayList<String>) b.retrievewordsweightcount("2012-03-03");
			  //Iterator i=countresult.listIterator();
			  while(i.hasNext())
			  {
				  System.out.println(i.next());
			  }
			  b.insertweightscounts("10292", "20120303"," 0.3", "20");
			  */
	}


	@Override
	public int compare(Object o1, Object o2) {
		// TODO Auto-generated method stub
		return 0;
	}
}

