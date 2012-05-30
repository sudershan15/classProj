import java.util.Iterator;
import java.util.List;

 


public class ClientClass 
{
	public void gethistoricaldata(String symbol)
		{
			CassandraBackend c=new CassandraBackend();
			List<String> resultset=c.retrieveAllData(symbol);
			Iterator i=resultset.listIterator();
			while(i.hasNext())
			{
				System.out.println(i.next());
			}
		}
	public void getprediction(String symbol)
		{
			CassandraBackend c=new CassandraBackend();
		
		
		}
	public void getsnapshot(String startdate,String enddate,String symbol)
		{
			CassandraBackend c=new CassandraBackend();
			List<String> resultset=c.retrieveData(startdate, enddate, symbol);
			Iterator i=resultset.listIterator();
			while(i.hasNext())
				{
						System.out.println(i.next());
					}
		
	}
	public void gettrends(String startdate,String enddate,String symbol)
	{

		CassandraBackend c=new CassandraBackend();
		List<String> resultset=c.retrieveData(startdate, enddate, symbol);
		Iterator i=resultset.listIterator();
		while(i.hasNext())
			{
					System.out.println(i.next());
				}
		
	}
	
	
	public static void main(String args[])
	{
		ClientClass c=new ClientClass();
		//c.gethistoricaldata("IBM");
		c.getsnapshot("2012-03-01", "2012-04-01", "IBM");
		//c.gettrends("2012-03-01", "2012-04-01", "IBM");
	}
	
}
