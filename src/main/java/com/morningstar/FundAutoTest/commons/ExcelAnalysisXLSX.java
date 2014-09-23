package com.morningstar.FundAutoTest.commons;

//import java.io.FileOutputStream;
import java.io.IOException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

//import org.apache.poi.hssf.usermodel.HSSFCell;
//import org.apache.poi.hssf.usermodel.HSSFRichTextString;
//import org.apache.poi.hssf.usermodel.HSSFRow;
//import org.apache.poi.hssf.usermodel.HSSFSheet;
//import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;



/**
 * ���������excel xlsx(07������0)
 * @author dallas16
 *
 */
public class ExcelAnalysisXLSX extends DefaultHandler {
    /**
     * excel���������
     */
     private StylesTable stylesTable;
     /**
      * ���������
      */
     private ReadOnlySharedStringsTable sharedStringsTable;
     /**
      * ���������������������������������������������������������������
      *
      */
     enum xssfDataType {
         BOOL,
         ERROR,
         FORMULA,
         INLINESTR,
         SSTINDEX,
         NUMBER,
     }
     /**
      * ������������������cell���������������
      */
     private xssfDataType nextDataType = xssfDataType.NUMBER;
    /**
     * ���������cell���������?
     */
    private String value = "";
    
    private short formatIndex;
    private String formatString;
    private final DataFormatter formatter = new DataFormatter();
    
    /**
     * ������������������������������������
     */
    private List<String> rowlist = new ArrayList<String>();
    /**
     * excel������heet��������
     */
    private String sheetName;
    /**
     * excel ���������������
     */
    private String path;
    /**
     * ������������������������������������
     */
    private List<List<String>> datas = new ArrayList<List<String>>();
    /**
     * ������������������������������������������
     */
    private int  thisColumn;
    private int lastColumnNumber;
    
    /**
     * ���������������������������������������������������������������������efaultHandler������haracters������������������������efaultHandler���������������������������������������������������������������������
     *     ������������������������������������������������������������������������
     * @param ch
     * @param start
     * @param length
     */
    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        value = value+new String(ch,start,length);
    }

    /**
     * ������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������
     *                             ������������������������������������������alue������������������rowlist������������������������
     *                             ������������������������������������������rowlist���������������heetVo������
     * @param uri
     * @param localName
     * @param qName
     */
    @Override
    public void endElement(String uri, String localName, String qName)
            throws SAXException {
         if ("v".equals(qName)) {
            endDeal();
         } else if ("row".equals(qName)) {
             if (lastColumnNumber == -1) {
                 lastColumnNumber = 0;
             }
             this.dealData();
             lastColumnNumber = -1;
         }

    }

    /**
     * ���������������������������������������������������������
     * @param uri
     * @param localName
     * @param qName
     */
    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) throws SAXException {
        if ("inlineStr".equals(qName) || "v".equals(qName)) {
            value = "";
        }
        else if ("c".equals(qName)) {
            String r = attributes.getValue("r");
            int firstDigit = -1;
            int length = r.length();
            for (int c = 0; c < length; ++c) {
                if (Character.isDigit(r.charAt(c))) {
                    firstDigit = c;
                    break;
                }
            }
            thisColumn = nameToColumn(r.substring(0, firstDigit));
            dealDataType(attributes); 
        }

    }

    /**
     * ������������������sheet������������������
     * @param styles
     * @param strings
     * @param sheetInputStream
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    public void processSheet(
            StylesTable styles,
            ReadOnlySharedStringsTable strings,
            InputStream sheetInputStream)
            throws IOException, ParserConfigurationException, SAXException {

        InputSource sheetSource = new InputSource(sheetInputStream);
        SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        SAXParser saxParser = saxFactory.newSAXParser();
        XMLReader sheetParser = saxParser.getXMLReader();
        this.stylesTable = styles ;
        this.sharedStringsTable = strings;
        sheetParser.setContentHandler(this);
        sheetParser.parse(sheetSource);
    }

    /**
     * 
     * @param excelUtilBean.getPath() ������������������������������xcel���������������
     * @param excelUtilBean.getSheetName() ��������������������������������������������������������������
     * @param excelUtilBean.getSheetNumber() ������������������������������������������������������������������������ 
     * @throws IOException
     * @throws OpenXML4JException
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    @SuppressWarnings("unused")
    public void process()
            throws IOException, OpenXML4JException, ParserConfigurationException, SAXException {
        OPCPackage pkg = OPCPackage.open(path);
        ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
        XSSFReader xssfReader = new XSSFReader(pkg);
        StylesTable styles = xssfReader.getStylesTable();
        XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) xssfReader.getSheetsData();
        int index = 0;
        boolean flag = false;
        while (iter.hasNext()) { // ���������������������������������������������������������������������������������������������sheet
            InputStream stream = iter.next();
             String sheetName = iter.getSheetName();
             if(sheetName.equals(this.sheetName)){
                 processSheet(styles, strings, stream);
                 flag = true;
                 break;
             }   
              stream.close();
              ++index;
        }
        if(!flag ){
            String errorInfo = "The "+sheetName+" is not exists";
            throw new RuntimeException(errorInfo);
        }
        pkg = null;
        strings = null;
        xssfReader = null;
        styles = null;
    }
    
    /**
     * ���������������������������������������������������������������������������������������rowlist������
     */
    public void endDeal(){
         String thisStr = null;
         thisStr = dealData(value,thisStr); //������������������������������������������������������������
         if (lastColumnNumber == -1) {
             lastColumnNumber = 0;
         }
         for (int i = lastColumnNumber+1; i < thisColumn; ++i){
             rowlist.add("  ");
         }
             rowlist.add(thisStr==null?"":thisStr);
         if (thisColumn > -1){
             lastColumnNumber = thisColumn;
         }
    }
    
    /**
     * ���������������������������sheetVo������������������������������������������������������������������������
     */
    public void dealData(){
        if(rowlist != null && rowlist.size() != 0 ){
            datas.add(rowlist);
            rowlist = null;
            
        }
        if(rowlist == null){
            rowlist = new ArrayList<String>();
        }
    }
    
    /**
     * ���������������������������������������������������
     * @param name
     * @return
     */
    private int nameToColumn(String name) {
        int column = -1;
        int length = name.length();
        for (int i = 0; i < length ; ++i) {
            int c = name.charAt(i);
            column = (column + 1) * 26 + c - 'A';
        }
        return column;
    }
    
    /**
     * ��������������������������������������������������������������
     * @param value       ���������������������������������������������������������������������������
     * @param thisStr  ���������������������������������
     * @return
     */
    public String dealData(String value,String thisStr){
         switch (nextDataType) {//���������������������������������������������������������������������������������������������������������������������
         case BOOL:
             char first = value.charAt(0);
             thisStr = first == '0' ? "FALSE" : "TRUE";
             break;
         case ERROR:
             thisStr = "\"ERROR:" + value.toString() + '"';
             break;
         case FORMULA:
             thisStr = '"' + value.toString() + '"';
             break;
         case INLINESTR:
             XSSFRichTextString rtsi = new XSSFRichTextString(value.toString());
             
             thisStr =  rtsi.toString() ;
             rtsi = null;
             break;
         case SSTINDEX:
             String sstIndex = value.toString();
             try {
                 int idx = Integer.parseInt(sstIndex);
                 XSSFRichTextString rtss = new XSSFRichTextString(sharedStringsTable.getEntryAt(idx));
                 thisStr = rtss.toString();
                 rtss = null;
             }
             catch (NumberFormatException ex) {
                 thisStr  = value.toString();
             }
             break;
         case NUMBER:
             String n = value.toString();
             if (this.formatString != null)
                 thisStr = formatter.formatRawCellContents(Double.parseDouble(n), this.formatIndex, this.formatString);
             else
                 thisStr = n;
             break;
         default:
             thisStr = " ";
             
             break;
     }
        return thisStr;
    }
    

    /**
     * ���������������������������
     * @param attributes
     */
    public void dealDataType(Attributes attributes){
        this.nextDataType = xssfDataType.NUMBER;
        this.formatIndex = -1;
        this.formatString = null;
        String cellType = attributes.getValue("t");
        String cellStyleStr = attributes.getValue("s");

        if ("b".equals(cellType))
            nextDataType = xssfDataType.BOOL;
        else if ("e".equals(cellType))
            nextDataType = xssfDataType.ERROR;
        else if ("inlineStr".equals(cellType))
            nextDataType = xssfDataType.INLINESTR;
        else if ("s".equals(cellType))
            nextDataType = xssfDataType.SSTINDEX;
        else if ("str".equals(cellType))
            nextDataType = xssfDataType.FORMULA;
        else if (cellStyleStr != null) {
            int styleIndex = Integer.parseInt(cellStyleStr);
            XSSFCellStyle style = stylesTable.getStyleAt(styleIndex);
            this.formatIndex = style.getDataFormat();
            this.formatString = style.getDataFormatString();
            if (this.formatString == null)
                this.formatString = BuiltinFormats.getBuiltinFormat(this.formatIndex);
        }
    }
    /**
     * ���������������������������rowlist������
     * @param str    ���������������������������
     * @param num    ���������
     */
    public void addRowlist(String str , int num){

        if(rowlist.size() >= num){
            rowlist.add(num, str==null?"":str);
        } else {
            int size = rowlist.size();
            int newNum = num+1;
            for(int i = size; i < newNum; i++){
                rowlist.add("");
            }
            rowlist.add(num, str==null?"":str);
        }
    }

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<List<String>> getDatas() {
        return datas;
    }

    public void setDatas(List<List<String>> datas) {
        this.datas = datas;
    }

//    public static void main(String[] args) throws Exception {
//        
//        ExcelAnalysisXLSX excel = new ExcelAnalysisXLSX();
//        excel.setPath("F:/b.xlsx");
//        excel.setSheetName("Sheet1");
//        excel.process();
//        List<List<String>> datas = excel.getDatas();
//        for(List<String> data : datas){
//            System.out.println(data);
//        }
//    }
}
