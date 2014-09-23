package MarketPriceBackFeedNotification;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;




public abstract class AbstractXMLReader<V> implements IDataReader<V> {
    
    protected Document getXMLDocument(String url) throws ParserConfigurationException, SAXException, IOException{
           
       InputStream is = getInputStream(url);

       DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
              .newInstance();
       DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
       Document doc = docBuilder.parse(is);

       return doc;
    };
    
    protected abstract InputStream getInputStream(String url) throws MalformedURLException, IOException;
    
    protected abstract V parseXML() throws Exception;

    /* (non-Javadoc)
    * @see com.morningstar.calculation.fundtna.dao.IDataReader#getData()
    */
    public V getData() throws Exception {      
       return parseXML();
    }
}

