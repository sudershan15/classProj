

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * HttpGet is a wget: download any file via HTTP and store locally.
 * 
 * @author      Jose Cotrino
 * @version     %I%, %G%
 * @since       1.0
 */
public class HttpGet {

    /** 
     * This is a static method which, given a URL, will download & store
     * its content locally under the given file name.
     *
     * @param filename	File where to store the downloaded content
     * @param url		HTTP link to the resource
     * @return          <code>true</code> if download successful,
     * 					<code>false</code> otherwise
     * @see				java.net.HttpURLConnection
     * @since           1.0
     */
	public static boolean cache(String filename,String url) {
		boolean ready = false;
		try  {  
			URL u = new URL(url) ; 
			HttpURLConnection huc = (HttpURLConnection)u.openConnection(); 
			huc.setRequestMethod("GET"); 
			huc.connect(); 
			InputStream is = huc.getInputStream(); 
			if( huc.getResponseCode() == HttpURLConnection.HTTP_OK ) {
				byte[] buffer = new byte[4096];
				File output = new File(filename);
				FileOutputStream outputStream = new FileOutputStream(output); 
				boolean downloading = true;
				while(downloading)  {
					int bytes = is.read(buffer);
					if (bytes <= 0) {
						downloading = false;
					} else {
						outputStream.write(buffer,0,bytes);
					}
				}
				outputStream.close();
			}
			is.close();
			huc.disconnect(); 
			ready = true;
		} catch( IOException e ) { 
			System.err.println("\nCould not retrieve "+url+"!\n"+e.getMessage());
		}
		return ready;
	}
}
