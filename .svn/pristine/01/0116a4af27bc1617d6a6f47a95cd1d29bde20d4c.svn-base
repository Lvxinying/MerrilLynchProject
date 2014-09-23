package com.morningstar.FundAutoTest;

import java.io.File;   
import java.io.FileWriter;   
import java.io.IOException;   
import java.io.Writer;   
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;   
import org.dom4j.DocumentException;   
import org.dom4j.DocumentHelper;   
import org.dom4j.Element;   
import org.dom4j.io.SAXReader;   
import org.dom4j.io.XMLWriter;



public class XmlHelperNew {
//	Stefan.hou
//	创建XML文件(仅有一级子节点)
	public static void createXml(String fileName) {   
		Document document = DocumentHelper.createDocument();   
		Element rootNode=document.addElement("RootNode");   
		Element node1=rootNode.addElement("Node1");   
		Element name= node1.addElement("name");   
		name.setText("stefan.hou");   
		Element sex=node1.addElement("sex");   
		sex.setText("m");   
		Element age=node1.addElement("age");   
		age.setText("29");   
		try {   
		Writer fileWriter=new FileWriter(fileName);   
		XMLWriter xmlWriter=new XMLWriter(fileWriter);   
		xmlWriter.write(document);   
		xmlWriter.close();   
		} catch (IOException e) {   

		System.out.println(e.getMessage());   
		}   
	}   

//解析xml文件，2级节点
		public static List<String> parserXml(String fileName) {   
			File inputXml = new File(fileName);   
			SAXReader saxReader = new SAXReader();
			List<String> list = new ArrayList<String>();
			try {   
				Document document = saxReader.read(inputXml);   
				Element rootNode = document.getRootElement();
				for(Iterator<?> i = rootNode.elementIterator(); i.hasNext();){   
					Element node1 = (Element) i.next();   
					for(Iterator<?> j = node1.elementIterator(); j.hasNext();){   
						Element node2=(Element) j.next();   
						list.add(node2.getStringValue()); 
					}   
				}   
			} catch (DocumentException e) {   
				System.out.println(e.getMessage());   
			} 
			return list;
		}
/*
		public static void main(String[] args){
			List<String> text = parserXml("./config/DBFactory.xml");
			for(int i =0;i<text.size();i++){
				System.out.println(text.get(i));	
			}
		}
*/
		
}


