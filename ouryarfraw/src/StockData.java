import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
public class StockData 
{
	public static void main(String... args)
	{
		
    	try {
    		File file = new File("C:/Users/Harini/Desktop/news.txt");
        	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        	String line = null;
        
			while( (line = br.readLine())!= null )
			{
				if(line.startsWith("Date"))
				{
					//String [] tokens = line.split(":");
					// String myline=tokens[1];
					int length=line.length();
					String myline=line.substring(5, length);
					 System.out.println(myline);
				}
				else if(line.startsWith("URL"))
				{
					int length=line.length();
					String myline=line.substring(4, length);
					 System.out.println(myline);
				}
				else if(line.startsWith("Summary"))
				{
					int length=line.length();
					String myline=line.substring(8, length);
					 System.out.println(myline);
				}
				else if(line.startsWith("PubDate"))
				{
					
					int length=line.length();
					String myline=line.substring(8, length);
					 System.out.println(myline);
				}
				else if(line.startsWith("Text"))
				{
					int length=line.length();
					String myline=line.substring(5, length);
					 System.out.println(myline);
				}
			      CassandraExample n=new CassandraExample();
			     n.insertData(line);
			   
				
				 
				 
			 
}
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}

}
