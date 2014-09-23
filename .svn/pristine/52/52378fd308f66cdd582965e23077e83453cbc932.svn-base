package com.morningstar.FundAutoTest.source;

import java.text.MessageFormat;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.morningstar.FundAutoTest.commons.Common;
import com.morningstar.FundAutoTest.commons.ResourceManager;

public class IDService {
	
	private static String basicXPath = "root/m/r/dv";
	private static Common common = new Common();
	private static final Logger logger = LoggerFactory.getLogger(IDService.class);
	
	
	public static String getExchangeIdNum(String ShareClassId)
	{
		String url = ResourceManager.getIDServiceURL();
		String exactURL = MessageFormat.format(url, ShareClassId);
		String ExchangeIDNum = "";
		try{
			Document sourceDoc = common.getXmlFromURL(exactURL);
			List<Element> elements = sourceDoc.selectNodes(basicXPath);
			for (Element element : elements)
			{
				if (element.attributeValue("d") !=null && element.attributeValue("d").trim().equals("AC030"))  
				{
					ExchangeIDNum = element.attributeValue("v").trim();
					String[] temp = ExchangeIDNum.split(" ");
					if (temp.length > 1)
					{
						for (int i = 0; i < temp.length; i++)
						{
							if (temp[i].equals("126")) 	ExchangeIDNum = "126";
						}
						if (!ExchangeIDNum.equals("126")) 	ExchangeIDNum = temp[0];
					}
				}
			}
		}catch(Exception e)
		{
			logger.warn("ShareClassId: " + ShareClassId + "\t" + e.getMessage());
		}		
		return ExchangeIDNum;
	}
}
