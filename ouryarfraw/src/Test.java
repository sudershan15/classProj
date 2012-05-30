
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.httpclient.HttpURL;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.lang.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.examples.HtmlToPlainText;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import yarfraw.core.datamodel.ChannelFeed;
import yarfraw.core.datamodel.Content;
import yarfraw.core.datamodel.ItemEntry;
import yarfraw.core.datamodel.Link;
import yarfraw.core.datamodel.YarfrawException;
import yarfraw.generated.rss10.elements.Items;
import yarfraw.io.FeedReader;


public class Test{

  public static void main(String[] args) throws Exception {
	  
	  // can pass different rss url as a string here
	  String url = "http://finance.yahoo.com/news/category-stocks/rss";
//	  processFeed(url);
//	  processFeed("http://articlefeeds.nasdaq.com/nasdaq/symbols?symbol=IBM");
	  
	 // processFeed("http://www.nasdaq.com/aspxcontent/NasdaqRSS.aspx?data=quotes&symbol=IBM");
 }

  public static void processFeed(String url) {
	  
	  FeedReader r = null;
		try {
			r = new FeedReader(new HttpURL(url));
		} catch (URIException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (YarfrawException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		  ChannelFeed c=null;
		try {
			c = r.readChannel();
		} catch (YarfrawException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
		
	
        
        /*
		String text = doc.body().text();
		 System.out.println("----");
		 System.out.println(text);
		  */
		
  
		
  System.out.println("Fan added: " + c.getTitleText());
  List<ItemEntry> items = c.getItems();
	// write index
	for(ItemEntry item : items) {
		String itemUrl= item.getLinks().get(0).getHref();//.get(0).getHref();
		String itemTitle = item.getTitleText();
		String pubDateStr = item.getPubDate();
		System.out.println("Publish date is : " + pubDateStr);
		
		
		String pubDate = "";
		try{
			SimpleDateFormat sdfSource = new SimpleDateFormat("EEE, d MMM yy HH:mm:ss z");
			Date strDate = sdfSource.parse(pubDateStr);
		
			SimpleDateFormat sdfDestination = new SimpleDateFormat("MM-dd-yyyy");
			pubDate = sdfDestination.format(strDate);
			

			System.out.println("Converted date is : " + pubDate);

			
		} catch (ParseException e) {
			System.out.println("Parse Exception: " + e);
		}
		
		
		
	    System.out.println("Title: " + itemTitle);
		System.out.println("URL: " + itemUrl);
		System.out.println("Summary: " + item.getDescriptionOrSummaryText());
		System.out.println("Pub Date: " + pubDateStr);
		
		String[] textTitleArr = itemTitle.split(" ");
		String textTitle = textTitleArr[0];
	
		FileWriter outFile = null;
		try {
			outFile = new FileWriter("testdocs/en/stock/"+pubDate+"_"+textTitle+".txt");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		PrintWriter out = new PrintWriter(outFile);
		
		Document doc = null;
		try {
			doc = Jsoup.parse(Jsoup.connect(itemUrl).get().toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		//HtmlToPlainText formatter = new HtmlToPlainText();
        //String plainText = doc.body().ownText();//text();//formatter.getPlainText(doc);
        
		Elements e = doc.select("body");//.outerHtml();
	//String bodyHtml = e.html();
		
		String plainText = e.text();//doc.select("p").attr("first");//body().text();
		
		out.print(itemTitle);
		out.print(plainText);
		out.close();
		System.out.println(plainText);
	    
	    System.out.println("----");
		
	
	
		
	}
	
	//HtmlWriter hwriter = new HtmlWriter(c, "Yahoo", ".", "", "2012-05-05");
	
  }
}
