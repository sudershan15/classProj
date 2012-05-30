
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import yarfraw.core.datamodel.FeedFormat;
import yarfraw.core.datamodel.ChannelFeed;
import yarfraw.io.FeedReader;
import yarfraw.utils.FeedFormatDetector;

/**
 * FileFeed is a class to parse a RSS/Atom XML file
 * using the Yarfraw library.
 * 
 * @author      Jose Cotrino
 * @version     %I%, %G%
 * @since       1.0
 */
public class FileFeed {

	ChannelFeed channel = null;
	
    /** 
     * The constructor is doing all the action. It will:
     * <ul>
     * <li>Detect the feed format of the file</li>
     * <li>Parse the feed using Yarfraw</li>
     * </ul>
     *
     * @param feed		Path to the RSS/Atom XML file 
     * @return          <code>FileFeed</code>
     * @see				yarfraw.io.FeedReader
     * @since           1.0
     */
	public FileFeed(String feed) {
		FeedReader reader = null;
		try {
			System.out.println("Reading "+feed+"...");
			File file = new File(feed);
			InputStream input = (InputStream)new FileInputStream(file);
			FeedFormat format = FeedFormatDetector.getFormat(input);
			System.out.println("Format: "+format);
			reader = new FeedReader(file, format);
			channel = reader.readChannel();
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	
	public ChannelFeed getChannel() {
		return channel;
	}
}
