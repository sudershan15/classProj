import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
public class PredictionAlgorithm
{
	Double totalweight=0.0;
	 
	int k = 0;
	public String[] removeDuplicates(String [] duplicates)
	   {
		
		String [] fileArr = null;
		
		ArrayList <String> fileNameArr = new ArrayList <String> ();
		
		fileNameArr.add(duplicates[0]);
	
	       for ( int i = 0; i < duplicates.length; i++ )
	       {
	           for ( int j = 0; j < fileNameArr.size(); j++ )
	           {
	        	   
	               if ( fileNameArr.contains(duplicates[i]) ) 
	               {
	            	   break;

	               }
	               else {
	            	   fileNameArr.add(duplicates[i]);
	            	//   System.out.println("unique file: " + duplicates[i]);
	            	   //break;
	               }
	           }
	       }

	       /*
	       for (String thisFile : fileNameArr) {
	    	   System.out.println(thisFile.toString());
	       }
	       */
	       fileArr = (String [])fileNameArr.toArray(new String[0]);
	       return fileArr;
	   }

		public String[] Process(File aFile) {
			
			String [] fileArr = null;
			ArrayList <String> fileNameArr = new ArrayList <String> ();
			
		    if(aFile.isFile())
		      System.out.println("[FILE] " + aFile.getName());
		    else if (aFile.isDirectory()) {
		      System.out.println("[DIR] " + aFile.getName());
		      File[] listOfFiles = aFile.listFiles();
		      
		      for (File thisFile: listOfFiles) {
		    	  String thisFileName = thisFile.getName();//.split("_")[0];
		    	  
		    	  //System.out.println("filename: " + thisFileName);
		    	  fileNameArr.add(thisFileName);
		      }
		      
		      fileArr = (String [])fileNameArr.toArray(new String[0]);
		     System.out.println("number of files in " + aFile + ": " + fileArr.length);
		    }
			return fileArr;
		  }
	 
	public void datareader(String mykeyword,String keyfilename,String symbol)
	{
		String w=mykeyword.toLowerCase();
		String date="";
		
		int wordFound =0;
		try {
			String filen=keyfilename;
			String textfilename=filen.replace(".key", ".txt");
			File file = new File(textfilename);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(textfilename)));
			String line = null;
			String myarticle="";
			while( (line = br.readLine())!= null){
     			if(line.startsWith("Date"))
				{
     			 
					date=line.substring(5, line.length());
					 
				}
				myarticle=myarticle.concat(line.toLowerCase());
				wordFound = StringUtils.countMatches(myarticle, w);
				 
     		}
		 
			  
	 
			 
		 
			 
			PredictionAlgorithm d=new PredictionAlgorithm();
			d.predictor(w,wordFound,date);//passing the date from here
			 
     	}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block nooo.run it as it is.If u comment it you wont get dates!
			e.printStackTrace();
		}
		//System.out.println(date);
	}
	public void keywordextractor(String symbol)
	{
		try 
		{
 		//File file = new File("C:/Users/Harini/Desktop/news.txt");
     	//BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
     	//File file1 = new File("testdocs/en/stock/05-07-2012_Final.key");
 		
			PredictionAlgorithm d=new PredictionAlgorithm();//I cannot process anymore code :'lol...u hav bcum compiler
			//i might become a jvm too :P :P
			String rootDir = "testdocs/en/reuters1";
 		File afile=new File(rootDir);
     	
     	String [] allFiles = d.Process(afile);
     	String [] fileRootArr = d.removeDuplicates( allFiles);
     	
     	for (String thisRoot: fileRootArr) {
     	
     		for (String thisFileName : allFiles) {
     			thisFileName = rootDir+"/"+thisFileName;
	        		if (thisFileName.contains(thisRoot)) {
	        			//System.out.println("this File Name: " + thisFileName);
	        			if (thisFileName.contains(".key")) 
	        				{
				        		BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(thisFileName)));
				        		String line = null;
				        
				        		while( (line = br1.readLine())!= null)
							{
								String mykeyword=line;
								//System.out.println(mykeyword);
								//DataReader d=new DataReader();
							 
								d.datareader(mykeyword,thisFileName,symbol);
							}
	        			}
	        		}
     			}
			
     		}
		}	
		catch (IOException e) 
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	public void predictor(String name,int count,String date)//passing the date here
	{
		String keypair=name+date;
		String thisdate=date;//actually if u can pass the whole date containin array here that
		//System.out.println(thisdate);
		String name1=name;
		Double totweight=0.0;
		String totcount=String.valueOf(count);
		CassandraBackend b=new CassandraBackend();
		Double weight=b.retrievewordsweight(name1);
		String myweight=String.valueOf(weight);
		String mycount=String.valueOf(count);
		
		 insertvalues(keypair,date,myweight,mycount);
	}
public void insertvalues(String keypair,String myweight,String mycount,String date)
	{
		 		CassandraBackend b=new CassandraBackend();
		 		//b.insertweightscounts(keypair, date, myweight,mycount);
		 		getprediction(date);
		 		
	}
	 public void getprediction(String date)
	 
	 {  String weight=null;
		 String count=null;
		 String result=null;
		 Double totalweightage=0.0;
		 CassandraBackend b=new CassandraBackend();
		 ArrayList<String> predictdata=(ArrayList<String>) b.retrievewordsweightcount(date);
		 Iterator me=predictdata.listIterator();
		 while(me.hasNext())
		 {
			   result=me.next().toString();
		 }
		 
			 String[] splits=result.split(",");
			  for(int k=0;k<splits.length;k++)
			  {
				    weight=splits[1];
				    count=splits[2];
				  //Double weightage=Double.valueOf(weight);
				 // totalweightage=totalweightage+weightage;
				  
			  }
			 
			  predictvalue(weight,count);
		// System.out.println(weightage+"    "+count);
	 }
	 public void predictvalue(String weight,String count)
	 {
		 Double thisweight=Double.valueOf(weight);
		 
		 //System.out.println(weight);
		 totalweight=totalweight+thisweight;
		 Double finalweight=totalweight+thisweight;
		 System.out.println(finalweight);
	 }
	
	
	public static void main(String... args)
	{
		PredictionAlgorithm d=new PredictionAlgorithm();
		d.keywordextractor("IBM");
		
}
}

		


 
