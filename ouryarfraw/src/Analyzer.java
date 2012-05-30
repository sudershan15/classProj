import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

 

public class Analyzer
{ 
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void myanalysis()
	{
				String prediction=null;
				CassandraBackend b=new CassandraBackend();
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
     		    String key=symbol+date;
     			String mydata=symbol+","+date+","+open+","+low+","+high+","+close+","+key;
     			newresult.add(mydata);
     		
         }
		  
				Iterator i=newresult.listIterator();
				while(i.hasNext())
				{
					String data=i.next().toString();
					String[] mydata=data.split(",");
					String date=mydata[1];
					String symbol=mydata[0];
					double open=Double.valueOf(mydata[2]);
					double close=Double.valueOf(mydata[5]);
					String keyvalue=mydata[6];
					 
					double change=((close-open)/open)*100;
					String percentchange=String.valueOf(change);
					System.out.println(change);
					if(change>0)
					{
						prediction="rise";
						
					}
					else
					{
						prediction="fall";
						
					}
					System.out.println(data);
					System.out.println(prediction);
					b.insertData(keyvalue, date, symbol, prediction, percentchange);
		}
		
		 
		
	}
	
	public static void main(String args[])
	{
		Analyzer a=new Analyzer();
		 a.myanalysis();
		 
	}

}

