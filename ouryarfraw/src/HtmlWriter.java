

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.regex.*;




import yarfraw.core.datamodel.ChannelFeed;
import yarfraw.core.datamodel.Content;
import yarfraw.core.datamodel.ItemEntry;

/**
 * HtmlWriter is a class to convert a feed, already parsed by Yarfraw,
 * into an HTML file containing an index and the body of each post.
 * During the process, it will download every image referenced in the feed.
 * 
 * @author      Jose Cotrino
 * @version     %I%, %G%
 * @since       1.0
 */
public class HtmlWriter {

	String path = "";
	String filename = "";
	String htmlUrl = "";
	String title = "";
	Pattern imagePattern = Pattern.compile("src\\s*=\\s*\"([^\" ]+)\"", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
	Pattern extensionPattern = Pattern.compile("\\.(\\w\\w\\w\\w*)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
	Pattern protocolPattern = Pattern.compile("^\\w+://", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    /** 
     * The constructor is doing all the action. It generates
     * an HTML file containing:
     * <ul>
     * <li>The index of posts</li>
     * <li>The content of every post</li>
     * </ul>
     *
     * @param channel	Feed already parsed by Yarfraw
     * @param title		Escaped name of the feed
     * @param path		Path where the HTML file should be generated
     * @param htmlUrl	URL to the blog, in case some images have relative paths
     * @return          <code>HttpWriter</code>
     * @see				FileFeed
     * @since           1.0
     */
	public HtmlWriter( ChannelFeed channel, String title, String path, String htmlUrl, String today ) {
		this.path = path;
		this.htmlUrl = htmlUrl;
		this.title = title;
		BufferedWriter out = null;
		String html = "";
		try{
			filename = path+"/"+title+".html";
			System.out.println("Writing to "+filename+"...");
			OutputStreamWriter fstream = new OutputStreamWriter(
					new FileOutputStream(filename),"UTF8");
			out = new BufferedWriter(fstream);
		} catch (Exception e) {
			System.err.println(e);
		}

		html = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" "+
			"\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">"+
			"<html><head>"+
			"<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"+
			"<title>"+channel.getTitleText()+"</title></head><body>\n"+
			"<p>Last synchronization: "+today+"<br/></p>\n";
		List<ItemEntry> items = channel.getItems();
		// write index
		for(ItemEntry item : items) {
			String id = item.getUid().getIdValue();
			String chapter = item.getTitleText();
			String date = item.getUpdatedDate(); 
			if( date == null ) {
				date = item.getPubDate(); 
			}
			html += "<a href='#"+id+"'>"+chapter+"</a>"+"<br/>\n";
			if( date != null ) {
				html += "<small>("+date+")</small><br/>\n";
			}
		}
			
		try {
			// write content
			for(ItemEntry item : items) {
				Content content = item.getContent();
				String id = item.getUid().getIdValue();
				String chapter = item.getTitleText();
				
				String summary = getContent(item.getLinks().get(0).getHref());
				
				
				
				String chapterText = "<a name='"+id+"'/><h1>"+chapter+"</h1><br/>\n";
				if( content == null ) {
					if( item.getElementByLocalName("encoded") != null ) {
						chapterText += item.getElementByLocalName("encoded").getTextContent()+"<br/>\n";
					} else {
						chapterText += item.getDescriptionOrSummaryText()+"<br/>\n";
					}
				} else {
					List<String> list = content.getContentText();
					for(String text : list) {
						chapterText += text+"<br/>\n";
					}
				}
				html += chapterText;
				
				html += "<h3>Comments</h3><br/>\n";
				html += summary+"<br/>\n";
			}
		} catch (Exception e) {
			System.err.println(e);
		}
		html += "</body></html>";
			
		try {
			html = getImages(html);
			html = escapeHTML(html);
			out.write(html);
			out.close();
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	
	public String getContent(String url) {
		 URL Url = null;
		try {
			Url = new URL("http://www.oracle.com/");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	        BufferedReader in = null;
			try {
				in = new BufferedReader(
				new InputStreamReader(Url.openStream()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	        String inputLine = "";
	        try {
				while ((inputLine = in.readLine()) != null) {
				    System.out.println(inputLine);
				    inputLine += inputLine;
				}
				
			//	String textOnly = Jsoup.parse(inputLine);
			  //  return textOnly;

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	        return inputLine;
	}
	
	public String getFile() {
		return filename;
	}
	
    /** 
     * Download and store locally all images contained in the feed.
     * This is doing by parsing the HTML content with regular expressions.
     *
     * @param html		Current HTML content
     * @return          <code>String</code> with the modified HTML content
     * @see				HttpGet
     * @since           1.0
     */
	private String getImages(String html) {
		int runningID = 0;
		try {
			StringBuffer buffer = new StringBuffer();
			Matcher imagesInHtml = imagePattern.matcher(html);
			System.out.print("Downloading images");
			while (imagesInHtml.find()) {
				String url = imagesInHtml.group(1);
				String ext = "";
				Matcher extensionMatcher = extensionPattern.matcher(url);
				if( extensionMatcher.find() ) {
					ext = extensionMatcher.group(1);
				}
				String cachedFile = path+"/"+title+runningID+".";
				if( ext.equals("") ) {
					cachedFile += "jpg";
				} else {
					cachedFile += ext;
				}
				// check if the URL contains the protocol (e.g. http://)
				Matcher protocolInUrl = protocolPattern.matcher(url);
				// otherwise, add the URL of the feed as prefix
				if( !protocolInUrl.find() ) {
					url = htmlUrl+"/"+url;
				}
				// download the image
				if( HttpGet.cache(cachedFile,url) ) {
					imagesInHtml.appendReplacement(buffer,"src=\""+cachedFile+"\"");
				} else {
					imagesInHtml.appendReplacement(buffer,"src=\""+url+"\"");
				}
				runningID++;
				System.out.print(".");
			}
			imagesInHtml.appendTail(buffer);
			html = buffer.toString();
			System.out.println("");
		} catch (Exception e ) {
			System.err.println(e);
		}
		return html;
	}
	
	/**
	 * Escape any non-ASCII characters into HTML entities.
	 * @param s		String to be escaped
	 * @return		Escaped string
	 */
	public static final String escapeHTML(String s){
		StringBuffer sb = new StringBuffer();
		int n = s.length();
		for (int i = 0; i < n; i++) {
			char c = s.charAt(i);
			if( c > 127 ) {
				sb.append("&#"+(int)c+";");
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}

}
