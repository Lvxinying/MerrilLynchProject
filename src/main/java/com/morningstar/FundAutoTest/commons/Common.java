package com.morningstar.FundAutoTest.commons;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.XPath;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.morningstar.FundAutoTest.DateUtil;
import com.morningstar.FundAutoTest.commons.testbase.Check;
import com.morningstar.FundAutoTest.commons.testbase.TestConfigData;

public class Common {
	private static final Logger logger = LoggerFactory.getLogger(Common.class);

	public String cn2Stardard(String input) {
		String result = null;
		try {
			byte[] tempVal2 = input.trim().getBytes();
			result = new String(tempVal2, "GB2312");
			// result = new String(tempVal2, "ISO-8859-1");
		} catch (Exception e) {
			logger.error("Error in Convert " + input + " to standard Words!");
		}
		return result;
	}

	// get the previous's test case Name;
	public String getCaseName() {
		StackTraceElement stack[] = (new Throwable()).getStackTrace();
		int i = stack[1].toString().indexOf("(");
		String tempStr1 = stack[1].toString().substring(0, i);
		return tempStr1.trim();
	}

	// caseName is the test case Name
	// caseResult is the test case result status: true or false
	// print result in result log file
	public void printCaseResult(String caseName, boolean caseResult) {
		String caseValue;
		if (caseResult)
			caseValue = "pass";
		else
			caseValue = "failure";
		logger.info(caseName + " : " + caseValue);
	}

	// Write Chinese words into a specified file
	public void logCNStr(String words, String fileName) {
		try {
			OutputStreamWriter osw = new OutputStreamWriter(
					new FileOutputStream("log//" + fileName), "utf-8");
			osw.write(words);
			osw.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	public List<TestConfigData> loadConfig(String configFile) {
		SAXReader saxReader = new SAXReader();
		List<TestConfigData> testList = new ArrayList<TestConfigData>();

		InputStream in;
		Document doc = null;
		try {
			in = new FileInputStream(configFile);
			doc = saxReader.read(in);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Element testConfig = (Element) doc.selectSingleNode("TestConfig");
		for (Iterator<Element> it = testConfig.elementIterator(); it.hasNext();) {
			Element test = (Element) it.next();
			TestConfigData testData = new TestConfigData();
			testData.Source = test.selectSingleNode("Source").getText();
			testData.Type = test.selectSingleNode("Type").getText();
			testData.Name = test.getName();
			testData.DestNodeSpecifiedDefinitions = test.selectSingleNode(
					"DestNodeSpecifiedDefinitions").getText();
			if (test.selectSingleNode("SourceNodeSpecifiedDefinitions") == null ) testData.SourceNodeSpecifiedDefinitions = null;
			else testData.SourceNodeSpecifiedDefinitions = test.selectSingleNode("SourceNodeSpecifiedDefinitions").getText();
			
			List<Node> checks = test.selectNodes("Check");
			for (Node check : checks) {
				Check c = new Check();
/*				Combine check sourcevalue with test's value
				String targetValue = check.selectSingleNode("SourcePath").getText();
				if ( (testData.SourceNodeSpecifiedDefinitions == null) || (testData.SourceNodeSpecifiedDefinitions.equals("")))	c.SourcePath = targetValue;
				else	
				{
					if (targetValue.equals("SPECIAL_ITEM")) c.SourcePath = targetValue;
					else c.SourcePath = testData.SourceNodeSpecifiedDefinitions + "/" + check.selectSingleNode("SourcePath").getText();	
				}
*/								
				c.SourcePath = check.selectSingleNode("SourcePath").getText();
				c.DestinationPath = check.selectSingleNode("DestinationPath")
						.getText();
				if (check.selectSingleNode("RoundSource") != null)
					c.RoundSource = Boolean.parseBoolean(check
							.selectSingleNode("RoundSource").getText());
				if (check.selectSingleNode("Type") != null)
					c.Type = check.selectSingleNode("Type").getText();
				testData.CheckList.add(c);
			}
			testList.add(testData);
		}
		return testList;
	}

	public String getPeriodDefinition(Document doc, String nodeWithDefinitions,
			String periodIndex) {
		try{
			String a = nodeWithDefinitions.substring(0,
					nodeWithDefinitions.indexOf("@") - 1)
					+ "/@" + periodIndex;
			XPath x = doc.createXPath(a);
			Node node = x.selectSingleNode(doc);
			if ("".equals(node.getText()) || "0".equals(node.getText()))
				return "";
			else {
				SimpleDateFormat sdt = new SimpleDateFormat("yyyy/MM/dd");
				SimpleDateFormat sdt1 = new SimpleDateFormat("yyyy-MM-dd");
				try {
					Date date = sdt.parse(node.getText());
					return sdt1.format(date);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					logger.warn(nodeWithDefinitions + "\tperiodIndex " + periodIndex + "\t"+ e.getMessage());
				}
			}
		}catch(Exception e)
		{
			logger.warn("nodeWithDefinitions: " + nodeWithDefinitions + "\t" + "periodIndex: " + periodIndex + "\t" + e.getMessage());
		}		
		return "";
	}

	public Document getXmlFromURL(String url) {
		try {
			Document doc = null;
			if (url.trim().contains(" ")) url.trim().replaceAll(" ", "%20");
			byte[] data = getDataFromAPI(url);
			String xmlData = new String(data, "UTF-8");

			// need to remove this xmlns otherwise search is not working
			// (selectnodes etc) not sure how to fix it
			xmlData = xmlData.replace(
					" xmlns=\"http://equityapi.morningstar.com/\"", "");

			if (xmlData.indexOf("xmlns=\"MSN_Money_Stock\"") != -1) {
				xmlData = xmlData.replace("xmlns=\"MSN_Money_Stock\"", "");
			}
			doc = DocumentHelper.parseText(xmlData);
			return doc;
		} catch (Exception ex) {
			logger.warn("GetXmlFromURL: " + url + "\t" + ex.toString());
			return null;
		}
	}

	public byte[] getDataFromAPI(String apiUrl) {
		int tryCount = 0;
		while (true) {
			try {
				URL url = new URL(apiUrl);
				URLConnection uRLConnection = url.openConnection();
				int contentLength = uRLConnection.getContentLength();
				// Object o = uRLConnection.getContent();
				InputStream inputStream = uRLConnection.getInputStream();
				int aa;
				//
				ByteArrayOutputStream bao = new ByteArrayOutputStream(
						contentLength);
				byte[] b = new byte[1024];
				while ((aa = inputStream.read(b)) != -1) {
					bao.write(b, 0, aa);
				}

				return bao.toByteArray();
			} catch (Exception e) {
				if (tryCount == 5)
					break;
				try {
					Thread.sleep(1000 * 1);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				tryCount++;
				e.printStackTrace();
			}
		}
		return null;
	}

	public Document getCustomAPIDoc(String url) {
		try {
			if (url.trim().contains(" ")) url.trim().replaceAll(" ", "%20");
			Document doc = getXmlFromURL(url);

			return doc;
		} catch (Exception ex) {
			logger.warn("GetCustomAPIDoc: " + url + "\t" + ex.toString());
			ex.printStackTrace();
			return null;
		}
	}
	
/*	public String getExchangeIdNum(String exchangeName)
	{
		String sql="select distinct(CountryId) from GEAPI.StockExchangeSecurity where ExchangeId = '%s'";
		String ExchangeIDNum = "";
		String countryId = DBCommons.getData(String.format(sql, exchangeName), Database.geapi61);
		if (countryId.equalsIgnoreCase("USA"))
			ExchangeIDNum = "126";
		if (countryId.equalsIgnoreCase("BRA"))
			ExchangeIDNum = "56";
		if (countryId.equalsIgnoreCase("FIN"))
			ExchangeIDNum = "176";
		return ExchangeIDNum;
	}
*/	

	//Find the data location in the XML Node 
	public int getDataPostionInXML(List<Element> dataElements, String data)
	{
		int result = -1;
		for (int i = 0; i < dataElements.size(); i++)
		{
			if (dataElements.get(i).getTextTrim().equals(data)) 	return i;
		}
		return result;
	}
	
	//Find the item's first location in XML File firstly
	//Then use the location to find the item's value
	public String getValueByKeyInXML(List<Element> dataElements, String keyName, String keyValue, String item)
	{
		String result = null;
		for (int i = 0; i < dataElements.size(); i++)
		{			
			Node node =	dataElements.get(i).selectSingleNode(keyName);
			if ( node != null )		
			{
				if (node.getText().trim().equals(keyValue))
				{
					Node expectNode = dataElements.get(i).selectSingleNode(item);
					if ( expectNode != null )
					{
						return expectNode.getText().trim();
					}else 
					{
						logger.warn("This Item " + item + " doesn't exist in XML");
						return null;
					}
				}				
			}
		}		
		return result;
	}
	
	//Find the date location in the XML Node, if date is EndDate, isEnd will be true; if date is beginDate, isEnd will be false 
	public int getDatePostionInXML(List<Element> dataElements, String date, boolean isEndDate) throws Exception
	{
		if (date.equals(DateUtil.getCurrentDate(ResourceManager.getTimeZone(), "yyyy-MM-dd"))) 	return 0;
		int result = -1;
		if (isEndDate) date = DateUtil.getPreWorkingDay(date, "yyyy-MM-dd");
		else date = DateUtil.getLateWorkingDay(date, "yyyy-MM-dd");
		for (int i = 0; i < dataElements.size(); i++)
		{
			if (dataElements.get(i).getTextTrim().equals(date)) 	return i;
		}
		return result;
	}

	// Compare XML List Data with Map List Data
	public boolean compareXMLMapList(List<Element> destinationDataElements, List<Map<Integer, String>> mapList, List<Check> ck, String messageTitle, String keyName, int keyId)
	{
		boolean result = true;
		try{
			for (int i=0; i< destinationDataElements.size(); i++)
			{
				Element element = destinationDataElements.get(i);
				Map<Integer, String> map = mapList.get(i);
				for (int j=0; j<ck.size(); j++) 
				{
					Check check = ck.get(j);
					Node destinationNode = element.selectSingleNode(check.DestinationPath);
					String sqlValue = map.get(j+1);
					if (destinationNode != null)
					{
						if (destinationNode.getText().trim().endsWith(sqlValue)) 
						{
							logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\t" + check.DestinationPath + " Actual Value isn't equal with Expected Value: " + destinationNode.getText().trim() + "," + sqlValue);
							result = false;
						}
					}else if (sqlValue != null )
					{
						if (!sqlValue.trim().equals("")) 
						{
							logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\t" + check.DestinationPath + " Actual Value isn't equal with Expected Value: null, " + sqlValue);
							result = false;
						}
					}
				}
			}
		}catch (Exception ex) {			
			logger.error(messageTitle + "\t" + ex.toString());
			result = false;
			ex.printStackTrace();
		}
		return result;
	}
	
	// Compare XML List Data with No Order Map List Data
	public boolean compareXMLNoOrderMapList(List<Element> destinationDataElements, List<Map<Integer, String>> mapList, List<Check> ck, String messageTitle, String keyName, int keyId)
	{
		boolean result = true;
		try{
			for (int i=0; i<mapList.size(); i++)
			{					
				Map<Integer, String> map = mapList.get(i);
				for (int j=0; j<destinationDataElements.size(); j++)
				{					
					Element element = destinationDataElements.get(j);
					if (element.selectSingleNode(keyName).getText().trim().equals(map.get(keyId)))
					{
						for (int k=0; k<ck.size(); k++) 
						{
							Check check = ck.get(k);
							Node destinationNode = element.selectSingleNode(check.DestinationPath);
							String sqlValue = map.get(k+1);
							if (destinationNode != null)
							{
								if (!destinationNode.getText().trim().equals(sqlValue)) 
								{
									logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\t" + check.DestinationPath + " Actual Value isn't equal with Expected Value: " + destinationNode.getText().trim() + "," + sqlValue);
									result = false;
								}
							}else if (sqlValue != null )
							{
								if (!sqlValue.trim().equals("")) 
								{
									logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\t" + check.DestinationPath + " Actual Value isn't equal with Expected Value: null, " + sqlValue);
									result = false;
								}
							}
						}
						break;
					}
					if (j == (destinationDataElements.size() -1 ))
					{
						logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\tCan't find it in Destination Page");
						result = false;						
					}
				}				
			}
		}catch (Exception ex) {			
			logger.error(messageTitle + "\t" + ex.toString());
			result = false;
			ex.printStackTrace();
		}
		return result;
	}
	
	// Compare XML Data with Map Data
	public boolean compareXMLMap(Element destinationDataElement, Map<Integer, String> map, List<Check> ck, String messageTitle, String keyName, int keyId)
	{
		boolean result = true;
		try{				
			for (int j=0; j<ck.size(); j++) 
			{
				Check check = ck.get(j);
				Node destinationNode = destinationDataElement.selectSingleNode(check.DestinationPath);
				String sqlValue = map.get(j+1);
				if (destinationNode != null)
				{
					if (!destinationNode.getText().trim().equals(sqlValue)) 
					{
						logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\t" + check.DestinationPath + " Actual Value isn't equal with Expected Value: " + destinationNode.getText().trim() + "," + sqlValue);
						result = false;
					}
				}else if (sqlValue != null )
				{
					if (!sqlValue.trim().equals("")) 
					{
						logger.error(messageTitle + "\t" + keyName + ":" + map.get(keyId) + "\t" + "CUSIP " + map.get(4) + "\t" + check.DestinationPath + " Actual Value isn't equal with Expected Value: null, " + sqlValue);
						result = false;
					}					
				}
			}
		}catch (Exception ex) {			
			logger.error(messageTitle + "\t" + "CUSIP " + map.get(4) + "\t" + ex.toString());
			result = false;
			ex.printStackTrace();
		}
		return result;
	}
	
	// Compare XML Data with Map Data
	public boolean compareXMLList(List<Element> destinationDataElements, List<Element> sourceDataElements, String messageTitle, Check check, TestConfigData test)
	{
		boolean result = true;
		try{
			if (sourceDataElements == null || destinationDataElements == null)
			{
				if (sourceDataElements != null && sourceDataElements.size() > 0)	
				{
					result = false;
					logger.error(messageTitle + "Destination Element is null, but source Element has data and its data size: " + sourceDataElements.size()); 
				}
				if (destinationDataElements != null && destinationDataElements.size() > 0)	
				{
					result = false;
					logger.error(messageTitle + "Source Element is null, but Destination Element has data and its data size: " + destinationDataElements.size()); 
				}
			}else
			{
				if (sourceDataElements.size() == destinationDataElements.size())
				{
					for (int i=0; i<sourceDataElements.size(); i++) 
					{
						Node destinationNode = destinationDataElements.get(i);
						Node sourceNode = sourceDataElements.get(i);
						result = nodeValueCompare(destinationNode, sourceNode, messageTitle, check, test);
					}
				}else
				{
					result = false;
					logger.error(messageTitle + "Destination Element:" + test.DestNodeSpecifiedDefinitions + check.DestinationPath + "\tSource Element:" + test.SourceNodeSpecifiedDefinitions + check.SourcePath + "\tDestination Data Size is different with Source Data Size, Destination Size,Source Size: " + destinationDataElements.size() + "," + sourceDataElements.size());				
				}
			}
						
		}catch (Exception ex) {
			logger.error(messageTitle + "Destination Element:" + test.DestNodeSpecifiedDefinitions + check.DestinationPath + "\tSource Element:" + test.SourceNodeSpecifiedDefinitions + check.SourcePath + "\t" + ex.toString());
			result = false;
			ex.printStackTrace();
		}
		return result;
	}
	
	//Find the ID by a item in XML
	public int getElementPositionByItem(List<Element> destinationDataElements, String item, Map<Integer, String> map, int mapId)
	{
		int result = -1;
		String ExpectedValue = map.get(mapId);
		for(int i=0; i<destinationDataElements.size(); i++)
		{
			Node node = destinationDataElements.get(i).selectSingleNode(item);
			if (node != null)
			{
				if (ExpectedValue.equals(node.getText().trim())) return i;
			}
		}
		return result;
	}
	
	//Compare 2 XML Documents, Check Destination Value with Source Value
	//Take Null same with Blank Value
	public boolean nodeValueCompare(Node destinationNode, Node sourceNode, String messageTitle, Check check, TestConfigData test)
	{
		boolean result = true;
		boolean checkAttribute = false;
		Element sourceElement = (Element) sourceNode;
		String sourceAttribute = "";
		if (check.SourcePath.trim().indexOf(")") == (check.SourcePath.trim().length() -1)) 
		{
			checkAttribute = true;
			sourceAttribute = check.SourcePath.substring(check.SourcePath.indexOf("(")+1, check.SourcePath.indexOf(")"));			
		}
		if (sourceNode != null &&  destinationNode != null)
		{	
			if (check.Type == null)
			{	
				double sourceValue;
				double destinationValue = destinationNode.getText() == "" ? 0 : Double.parseDouble(destinationNode.getText());
				if (checkAttribute) sourceValue = sourceElement.attributeValue(sourceAttribute) == "" ? 0 : Double.parseDouble(sourceElement.attributeValue(sourceAttribute));
				else sourceValue = sourceNode.getText() == "" ? 0 : Double.parseDouble(sourceNode.getText());									
				if (destinationValue != sourceValue)
				{
					if (checkAttribute) logger.error(messageTitle + check.DestinationPath + "\t" + check.SourcePath + "\tdata deosn't match for " + test.DestNodeSpecifiedDefinitions + "/" + check.DestinationPath + ": "
							+ destinationNode.getText() + "," + sourceElement.attributeValue(sourceAttribute));	
					else	logger.error(messageTitle + check.DestinationPath + "\t" + check.SourcePath + "\tdata deosn't match for " + test.DestNodeSpecifiedDefinitions + "/" + check.DestinationPath + ": "
							+ destinationNode.getText() + "," + sourceNode.getText());								
					result = false;
				}											
			}else
			{
				String destinationValue = destinationNode.getText();
				String sourceValue;
				if (checkAttribute) sourceValue = sourceElement.attributeValue(sourceAttribute);
				else	sourceValue = sourceNode.getText();						
				
				if (!destinationValue.equals(sourceValue)) 
				{
					logger.error(messageTitle + check.DestinationPath + "\t" + check.SourcePath + "\tdata deosn't match for " + test.DestNodeSpecifiedDefinitions + ": "
							+ destinationNode.getText() + "," + sourceValue);		
					result = false;
				}														
			}									
		}else if (sourceNode == null &&  destinationNode == null) ;
			  else if (sourceNode == null) 
				  {
				  	  if (!destinationNode.getText().trim().equals(""))
				  	  {
				  		  result = false;
						  logger.error(messageTitle + test.Name + "\t" + check.DestinationPath + "\t source Value can't be same with destination value. \t Source Node Value is null, but Destination Node Value is " + destinationNode.getText());
				  	  }				  	  
				  }
			  	   else 
			  	  {
			  		  String sourceValue;
					  if (checkAttribute) sourceValue = sourceElement.attributeValue(sourceAttribute).trim();
					  else	sourceValue = sourceNode.getText().trim();	

			  		  if (!sourceValue.equals(""))
			  		  {
			  			  result = false;
			  			  logger.error(messageTitle + test.Name + "\t" + check.DestinationPath + "\t source Value can't be same with destination value. \t Source Node value is " + sourceValue + "\t Destination Node Value is " + destinationNode);
			  		  }			  		  
			  	  }
		return result;
	}
	
	
	// Compare Node Value with a fixed data
	public boolean nodeFixedCompare(Node destinationNode, String sourceValue, String messageTitle, Check check, TestConfigData test)
	{
		boolean result = true;
		String destinationValue = destinationNode.getText().trim();
		if (!destinationValue.equals(sourceValue.trim())) 
		{
			logger.error(messageTitle + check.DestinationPath + "\t" + check.SourcePath + "\tdata deosn't match for " + test.DestNodeSpecifiedDefinitions + "\tDestination,Source : "
					+ destinationValue + "," + sourceValue);		
			result = false;
		}
		return result;
	}

	/**
	* Remove a Node/Element by before Date or after Date
	* @param XPath
	* The Element's absolute path in XML File
	* @param elementDate
	* Element's sub attribute -- date name
	* @param datePattern
	* Element's sub attribute -- date value's format
	* @param targetDate
	* format is MM/dd/yyyy
	* @param isMoveBefore
	* true means move data before the targetDate
	* false means move data after the targetDate
	*/
	public Document removeXMLNodeByDate(Document doc, String XPath, String elementDate, String targetDate, String datePattern, boolean isMoveBefore)
	{
		String pattern = "MM/dd/yyyy";
		List<Element> sourceElements = doc.selectNodes(XPath);
		for (int i = 0; i < sourceElements.size(); i++)
		{
			String earliestDate = sourceElements.get(i).selectSingleNode(elementDate).getText().trim();
			earliestDate = DateUtil.translateDate(earliestDate, datePattern, pattern);
			if (isMoveBefore)
			{
				if (DateUtil.compareDate(targetDate, earliestDate, pattern))	sourceElements.get(i).detach();
			}else
			{
				if (DateUtil.compareDate(earliestDate, targetDate, pattern))	sourceElements.get(i).detach();
			}
		}
		return doc;
	}
	
	// Make a List to a String with , split
	public String getStringFromListItem(List<Map<Integer, String>> list, int item)
	{
		String result = "";
		for(int i=0; i<list.size(); i++)
		{
			result = result + "'" + list.get(i).get(item) + "'";
			if (i < (list.size() -1)) result = result + ", ";
		}
		return result;
	}
	
	// Update a Map List Value with different Method, 
	// PREVIOUSADD: PREVIOUS Adding, BACKADD: BACK Adding, FULLCHANGE: FULL Change
	public void mapListUpdate(List<Map<Integer, String>> list, int item, String methodType, String updateValue)
	{
		for(int i=0; i<list.size(); i++)
		{
			if (methodType.equals("PREVIOUSADD")) 	list.get(i).put(item, updateValue + list.get(i).get(item));
			if (methodType.equals("BACKADD")) 	list.get(i).put(item, list.get(i).get(item) + updateValue);
			if (methodType.equals("FULLCHANGE")) 	list.get(i).put(item, updateValue + list.get(i).get(item));
		}
	}
	
	// Find the list by a key which has the specified value, 
	public List<Map<Integer, String>> find1List(List<Map<Integer, String>> list, int key, String keyValue)
	{
		for(int i=0; i<list.size(); i++)
		{
			 if (list.get(i).get(key).equals(keyValue)) return list.subList(i, i+1);
		}
		return null;
	}
	
	public String combineXPath(String parentPath, String subPath)
	{
		String result;
		if ((parentPath == null) || (parentPath.equals("")))	result = subPath;
		else result = parentPath + "/" + subPath;
		return result;
	}
	
	//getXMLNode for some node whose path has some kind of attribute
	public Node getXMLNode(Document document, String parentPath, String subPath)
	{
		Node node = null;
		try{
			if (subPath.contains("(") && subPath.contains(")"))
			{
				String attribute = subPath.substring(subPath.indexOf("(") + 1, subPath.indexOf(")"));
				if (parentPath == null || parentPath.equals(""))
				{
					node = document.selectSingleNode(subPath.substring(0, subPath.indexOf("(")));
					return node;
				}
				List<Element> elements = document.selectNodes(parentPath);
				for (Element element : elements)
				{
					if ( subPath.indexOf("(") == 0 )
					{						
						//if the SourcePath is FIRST_NODE, then return the first node
						//if the Element Value is what the sourcepath contains, then return the Element
						if (attribute.equals("FIRST_NODE") || (element.attributeValue(attribute.substring(0, attribute.indexOf("="))).trim().equals(attribute.substring(attribute.indexOf("=")+1))))
						{						
							node = element.selectSingleNode(subPath.substring(subPath.indexOf("/")+1));
							return node;
						}
					}
					else
					{
						node = element.selectSingleNode(subPath.substring(0, subPath.indexOf("(")));
					}
				}			
			}
			else node = document.selectSingleNode(combineXPath(parentPath, subPath));
		}catch(Exception e)
		{
			logger.warn("Get Node Failed \t" + parentPath + subPath + e.getMessage());
		}
				
		return node;
	}
	
	//Check API Document's basic information
	public boolean checkXMLBasicInfo(Document customAPIDoc, String messageTitle, String destinationUrl, boolean result)
	{
		boolean caseResult = true;	
		if (customAPIDoc == null)
		{
			if (result) 
			{
				logger.error(messageTitle + "Destination Document Load Failed, " + destinationUrl);
				caseResult = false;
			}
			return caseResult;
		}
		else if (customAPIDoc.getRootElement().selectSingleNode("MessageInfo/MessageCode").getText().trim().equals("50002"))
		{
			if (result)
			{
				logger.warn(messageTitle + "Load Destination URL, MessageCode is " + customAPIDoc.getRootElement().selectSingleNode("MessageInfo/MessageCode").getText());
			}
			else return caseResult;
		}
		else if (!customAPIDoc.getRootElement().selectSingleNode("MessageInfo/MessageCode").getText().trim().equals("200"))
		{
			if (result) 
			{
				caseResult = false;
				logger.error(messageTitle + "Load Destination URL failed, MessageCode isn't 200, it is " + customAPIDoc.getRootElement().selectSingleNode("MessageInfo/MessageCode").getText());
			}
			return caseResult;
		}
		
		return caseResult;
	}
	
	//List<Map<Integer, String>> change to List<List<String>>
	public List<List<String>> listMap2List(List<Map<Integer, String>> list)
	{
		List<List<String>> result = new ArrayList<List<String>>();		
		for (int i = 0; i < list.size(); i++)
		{
			List<String> temp =  new ArrayList<String>();
			for (int j = 0; j < list.get(i).size(); j++)
			{
				 temp.add(list.get(i).get(j));
			}
			result.add(temp);
		}
		return result;
	}
	
	//List<List<String>> change to List<Map<Integer, String>>
	public List<Map<Integer, String>> listList2Map(List<List<String>> list)
	{
		List<Map<Integer, String>> result = new ArrayList<Map<Integer, String>>();		
		for (int i = 0; i < list.size(); i++)
		{
			Map<Integer, String> temp = new HashMap<Integer, String>();
			for (int j = 0; j < list.get(i).size(); j++)
			{
				 temp.put(j+1, list.get(i).get(j));
			}
			result.add(temp);
		}
		return result;
	}
}
