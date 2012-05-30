import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;


public class DataReader 
{
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
		//String w="Oracle";
		 String date="";
		try {
			String filen=keyfilename;
		 
	 
			String textfilename=filen.replace(".key", ".txt");
			//System.out.println(textfilename);
		
			int wordFound =0;
        	File file = new File(textfilename);
        	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(textfilename)));
        	String line = null;
         String myarticle="";
        	//int count=0;
        	//int linecount=0;
        	while( (line = br.readLine())!= null)
        	{
        		
        	if(line.startsWith("Date"))
        	{
        	   date=line.substring(5, line.length());
        	}
        	
        		
        		  myarticle=myarticle.concat(line.toLowerCase());
        		 //String newline=null;
        		 //String resultline
        		//boolean result=myarticle.contains(w);
        		//System.out.println(result);
        		//if(result=true)
        		/*	
        		{ 
        			//System.out.println("This article: " +myarticle);
        			count=count+1;
        			
        		}
        		int newcount=count;
    			int countresult=count+newcount;*/
    			//System.out.println("This word appears ="+countresult+"times");
        		
        		 //String[] textarray=myarticle.split(" ");
        		/* for(int i=0;i<textarray.length;i++)
        		 {
        			 if(textarray[i].toString().toLowerCase().contains(w.toLowerCase()))
        			 {
        				 count=count+1;
        			 }
        		 }*/
        		  wordFound = StringUtils.countMatches(myarticle, w);
        		 
        		
        		 
        		
        	}
        	 DataReader d=new DataReader();
        	
        	// System.out.println(w + " =  " + wordFound + " times"); 
        	d.insertkeywords(w, date, wordFound,symbol);
        	
        	//}
        	
		}
	
			
			
			 
		
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
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
    		
			DataReader d=new DataReader();
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
		}	 catch (IOException e) 
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	public void insertkeywords(String keyword,String date,int count,String symbol)
	{
		String insertkeyword=keyword;
		String itscount=String.valueOf(count);
		String itsdate=date;
		 symbol="IBM";
		System.out.println(insertkeyword+"-----"+itscount+"--------"+date);
		CassandraBackend c=new CassandraBackend();
		c.insertKeywords(itsdate, insertkeyword, itscount,symbol);
	}
	
	public static void main(String... args)
	{
		DataReader d=new DataReader();
		d.keywordextractor("IBM");
}
}

	
