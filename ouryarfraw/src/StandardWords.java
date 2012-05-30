import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

 


public class StandardWords 
{
	 public static class ArrayList2DComarpator implements Comparator
	 {  
         public int compare(Object obj1, Object obj2) 
         {  
             if (! (obj1 instanceof ArrayList) || ! (obj2 instanceof ArrayList)) 
             {  
                 throw new ClassCastException(  
                             "compared objects must be instances of ArrayList");  
             }  
             String date1 = (String) ((ArrayList) obj1).get(1);  
             String date2 = (String) ((ArrayList) obj2).get(1);  
             return date1.compareTo(date2);  
         } 
	 }
		 public void standardwords()
		 {
			 CassandraBackend c=new CassandraBackend();
			 List<String> newresult=new ArrayList<String>();
			 
			 ArrayList result=(ArrayList)  c.retrievewords();
				
				 
	          Collections.sort(result, new ArrayList2DComarpator()); 
	            for(int j = 0; j < result.size(); j++)
	            {
	    			ArrayList myRow   = (ArrayList) result.get(j);
	        	 
	        			String count=myRow.get(0).toString();
	        			String words=myRow.get(1).toString() ;
	        			String date=myRow.get(2).toString() ;
	        			String mydata= count+","+date+","+words;
	        			 extractwords(mydata);
	        			
	        			//newresult.add(mydata);
	        		
	            }
				   
	             
			
			 
		 }
		 public void extractwords(String words)
		
		 {
			 String [] data=words.split(",");
			 String word=data[0];
			 String date=data[1];
			 String count=data[2];
			 
			 
		 }
		 public void checkwords(String word,String count)
		 {
			 int count1=Integer.valueOf(count);
			 System.out.println("count1");
			 int mycount=0;
			 String mykeyword="";
			 String keyword=word;
			 System.out.println(keyword);
			 
			 String mykeyword1=keyword;
			 System.out.println(mykeyword1);
			 if(mykeyword.equalsIgnoreCase(keyword))
			 {
				  System.out.println("here");
			 }
		 }
		 public static void main(String args[])
			
		 {
			 StandardWords s1=new StandardWords();
			 s1.standardwords();
			 
	}

}
