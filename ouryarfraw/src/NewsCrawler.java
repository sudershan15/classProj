

import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Example program to list links from a URL.
 */
public class NewsCrawler {
	
	
	public static void main(String[] args) throws Exception 
	{
		String rootDir = "testdocs/en/";
		
		processReuters(rootDir);
		
	}
	
	public static void processReuters (String rootDir) {
		String urlRoot = "http://www.reuters.com/resources/archive/us/";// "http://www.reuters.com/news/archive/globalMarketsNews?date=05012012";
		int startDate = 20120401;
		int endDate = 20120430;
		String urlTail = ".html";
		
		int seed = 9; // how many articles to retrieve from each day's archive
		
		// create a directory for reuters news
		
		String subDir = "reuters";
		
		for (int i = startDate; i<= endDate; i++) {
			String thisUrl = urlRoot+i+urlTail;
			
			try {
				// get an array of urls
				ListLinks u = new ListLinks ();
				String [] urlArr = u.ListLinks(thisUrl, "abs:href");
				int count = 0;
				String allArticles = "";
				
				
				allArticles += "Date:" + i + "\n";
				allArticles += "Text:";
				
				for (String url : urlArr) {
					System.out.println("--->URL: " + url);
					if (url.indexOf("http://www.reuters.com/article") != -1) {
						//System.out.println(url);
						// crawl each url and write text to file
						String prefix = Integer.toString(i);
						 //writeTextToFile(count, rootDir+subDir, url, prefix, "body p");

						 allArticles += getArticleText(rootDir+subDir, url, "body p") + "\n";
						count ++;
						 
					}
					
					if (count > seed) {
						break;
					}
				}
				
				allArticles += "DocID:" + i + "\n";
				
				writeAllTextToFile(allArticles, rootDir+subDir+"/"+i+".txt");
				System.out.println("articles found: " + count);
				System.out.println("All articles: " + allArticles);
				
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static String getArticleText (String dir, String url, String htmlTag) {

		String articleText = "";
		String[] textArr = url.split("/");
		String textTag = textArr[textArr.length-1];
		System.out.println("url: "+url);
		
		Document doc = null;
		try {
			doc = Jsoup.parse(Jsoup.connect(url).get().toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		//HtmlToPlainText formatter = new HtmlToPlainText();
        //String plainText = doc.body().ownText();//text();//formatter.getPlainText(doc);
        
		//Elements e = doc.select("body");//.outerHtml();
		String articleTitle = doc.select("title").text().toString();
		
	//	System.out.println("title: " + articleTitle);
	//	System.out.println("Date:" + prefix);
	//	System.out.println("Title:" + articleTitle);
	//	System.out.println("URL:" + url);
		
		articleText += articleTitle + "\n";
		
		
		Elements e = doc.select(htmlTag);
	//String bodyHtml = e.html();
		
		String plainText = e.text();//doc.select("p").attr("first");//body().text();
		
		//out.print(itemTitle);
		articleText += plainText + "\n";
	//	articleText += "DocID:" + prefix ;
		
		return articleText;
		//System.out.println("Text: " + plainText);
	}
	
	public static void writeAllTextToFile(String text, String fileName) {
		FileWriter outFile = null;
		try {
			outFile = new FileWriter(fileName);//dir+"/"+prefix+"_"+DocID+".txt");
			
			//outFile = new FileWriter("test.txt");
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		PrintWriter out = new PrintWriter(outFile);
		
		out.println(text);
		out.close();
	}
	
	public static void writeTextToFile (int DocID, String dir, String url, String prefix, String htmlTag) {
		FileWriter outFile = null;
		
		String[] textArr = url.split("/");
		String textTag = textArr[textArr.length-1];
		System.out.println("url: "+url);
		try {
			outFile = new FileWriter(dir+"/"+prefix+"_"+DocID+".txt");
			
			//outFile = new FileWriter("test.txt");
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		PrintWriter out = new PrintWriter(outFile);
		
		Document doc = null;
		try {
			doc = Jsoup.parse(Jsoup.connect(url).get().toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		//HtmlToPlainText formatter = new HtmlToPlainText();
        //String plainText = doc.body().ownText();//text();//formatter.getPlainText(doc);
        
		//Elements e = doc.select("body");//.outerHtml();
		String articleTitle = doc.select("title").text().toString();
		
	//	System.out.println("title: " + articleTitle);
	//	System.out.println("Date:" + prefix);
	//	System.out.println("Title:" + articleTitle);
	//	System.out.println("URL:" + url);
		
		
		out.println("Date:" + prefix);
		out.println("Title:" + articleTitle);
		out.println("URL:" + url);
		
		
		Elements e = doc.select(htmlTag);
	//String bodyHtml = e.html();
		
		String plainText = e.text();//doc.select("p").attr("first");//body().text();
		
		//out.print(itemTitle);
		out.println("Text:" + plainText);
		out.println("DocID:" + prefix+"_"+DocID);
		out.close();
		//System.out.println("Text: " + plainText);
	}
	
}
