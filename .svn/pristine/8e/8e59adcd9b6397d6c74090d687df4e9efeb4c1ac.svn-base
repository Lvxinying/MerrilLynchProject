package com.morningstar.FundAutoTest.source;

import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;

import com.morningstar.FundAutoTest.XmlHelper;
import com.morningstar.FundAutoTest.commons.Common;
import com.morningstar.FundAutoTest.commons.ResourceManager;

public class CodeMapping {
	private static Document codeSourceDoc = null; 
	private static String path = "Categories/Category";
	
	public static String getItemValue(String keyName, String keyValue, String item)
	{
		String result = "";
		
		Common common = new Common();
		//codeSourceDoc = XmlHelper.readStream(PriceXOI.getInputStream(ResourceManager.getCodeMappingURL()));
		//codeSourceDoc = common.getCustomAPIDoc(ResourceManager.getCodeMappingURL());
		codeSourceDoc = XmlHelper.readStream(ResourceManager.getCodeMappingURL());
		List<Element> dataElements = codeSourceDoc.selectNodes(path);
		result = common.getValueByKeyInXML(dataElements, keyName, keyValue, item);
		return result;
	}
}
