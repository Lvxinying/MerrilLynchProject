package MarketPriceBackFeedNotification;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public abstract class XOIReader<V> extends AbstractXMLReader<V> {
    
    public InputStream getInputStream(String url) throws MalformedURLException, IOException {
       String XOICookie = "XoiLogin=B7C3C13B-9699-47A0-98D7-56ACA46EFCD4";
       HttpURLConnection hc = (HttpURLConnection) (new URL(url)
              .openConnection());

       hc.setInstanceFollowRedirects(false);
       hc.setRequestProperty("Cookie",XOICookie);
       hc.setRequestProperty("Accept-Encoding", "Gzip");
       int status = hc.getResponseCode();

       if (status == 200) {
           InputStream is = hc.getInputStream();
           String encoding = hc.getHeaderField("Content-Encoding");

           if (encoding != null && encoding.equalsIgnoreCase("Gzip")) {
              is = new GZIPInputStream(is);
           }
           return is;

       } else {
           String message = hc.getResponseMessage();
           throw new RuntimeException("statusCode " + status + ": " + message);
       }

    }

}

