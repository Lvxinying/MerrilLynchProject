package com.morningstar.FundAutoTest.commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;

import org.apache.poi.ss.usermodel.DateUtil;

import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook; 
  
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFSheet;  
import org.apache.poi.hssf.util.CellReference;

import org.simpleframework.xml.transform.InvalidFormatException;

/**
 * 新Excel工具库
 * Excel文件数据的读取，更新，公式设置，公式计算结果读取
 * @author Stefan.Hou
 * 2013-10-16
 */
public class ExcelHelper {
	 /** 
     * Excel 2003 
     */  
    private final static String XLS = "xls";  
    /** 
     * Excel 2007 
     */  
    private final static String XLSX = "xlsx";    
    /** 
     * 由Excel文件的Sheet导出至List 
     *  
     * @param file 
     * @param sheetNum 
     * @return 
     */  
    public static List<String> exportListFromExcel(File file, int sheetNum ,int rowNum,int colNum) throws IOException {  
        return exportListFromExcel(new FileInputStream(file),FilenameUtils.getExtension(file.getName()), sheetNum,rowNum,colNum);  
    }  
  
    /** 
     * 由Excel流的Sheet导出至List 
     *  
     * @param is 
     * @param extensionName 
     * @param sheetNum 
     * @return 
     * @throws IOException 
     */  
    private static List<String> exportListFromExcel(InputStream is,String extensionName,int sheetNum,int rowNum,int colNum) throws IOException {    
        Workbook workbook = null;    
        if (extensionName.toLowerCase().equals(XLS)) {  
            workbook = new HSSFWorkbook(is);  
        } else if (extensionName.toLowerCase().equals(XLSX)) {  
            workbook = new XSSFWorkbook(is);  
        }    
        return exportListFromExcel(workbook,sheetNum,rowNum,colNum);  
    }    
    /** 
     * 由指定的Sheet导出至List 
     *  
     * @param workbook 
     * @param sheetNum 
     * @return 
     * @throws IOException 
     */  
    private static List<String> exportListFromExcel(Workbook workbook,int sheetNum,int rowNum,int colNum) {    
        Sheet sheet = workbook.getSheetAt(sheetNum);   
// 解析公式结果  
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();    
        List<String> list = new ArrayList<String>();  
                
        Row row = sheet.getRow(rowNum);  
//        StringBuilder sb = new StringBuilder();  
	        Cell cell = row.getCell(new Integer(colNum));  
	        CellValue cellValue = evaluator.evaluate(cell);
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
	        boolean boolValue = true;
	        Date dateValue = null;
	        double doubleValue = 0;
	        if (cellValue == null){  
	            String nullValue = "";
	            list.add(nullValue);
	        }  
// 经过公式解析，最后只存在Boolean、Numeric和String三种数据类型，此外就是Error了  
// 其余数据类型，根据官方文档，完全可以忽略http://poi.apache.org/spreadsheet/eval.html  
        switch (cellValue.getCellType()) {  
        case Cell.CELL_TYPE_BOOLEAN:  
        	boolValue = cellValue.getBooleanValue();
        	String StrboolValue = new Boolean(boolValue).toString();
        	list.add(StrboolValue);
            break;  
        case Cell.CELL_TYPE_NUMERIC:  
// 这里的日期类型会被转换为数字类型，需要判别后区分处理  
            if (DateUtil.isCellDateFormatted(cell)) {  
            	dateValue = cell.getDateCellValue();
            	String StrdateValue = sdf.format(dateValue);
            	list.add(StrdateValue);
            } else {  
            	doubleValue = cellValue.getNumberValue();
            	String StrdoubleValue = new Double(doubleValue).toString();
            	list.add(StrdoubleValue);
            }  
            break;  
        case Cell.CELL_TYPE_STRING:  
            cellValue.getStringValue();  
            break;  
        case Cell.CELL_TYPE_FORMULA:  
            break;  
        case Cell.CELL_TYPE_BLANK:  
            break;  
        case Cell.CELL_TYPE_ERROR:  
            break;  
        default:  
            break;  
        }  
        return list;  
    }
//不建议使用此方法了，对于有公式存在的情况时存在问题    
    public static List<String[]> readExcel(String filePath) {
        List<String[]> dataList = new ArrayList<String[]>();
        boolean isExcel2003 = true;
        if (isExcel2007(filePath)) {
            isExcel2003 = false;
        }
        File file = new File(filePath);
        InputStream is = null;
        try {
            is = new FileInputStream(file);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ExcelUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        Workbook wb = null;
        try {
//是Excel2003则使用HSSWorkbook，不是则使用XSSFWorkbook        	
            wb = isExcel2003 ? new HSSFWorkbook(is) : new XSSFWorkbook(is);
        } catch (IOException ex) {
            Logger.getLogger(ExcelUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        
//定位Excel的sheet位置，默认定位到sheet1        
        Sheet sheet = wb.getSheetAt(0);
        int totalRows = sheet.getPhysicalNumberOfRows();
        int totalCells = 0;
//取Excel的cell数目        
        if (totalRows >= 1 && sheet.getRow(0) != null) {
            totalCells = sheet.getRow(0).getPhysicalNumberOfCells();
        }
        for (int r = 0; r < totalRows; r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
//一次读取每一个Excel单元格中的内容，若为NULL，则赋""            
            String[] rowList = new String[totalCells];
            for (int c = 0; c < totalCells; c++) {
                Cell cell = row.getCell(c);
                String cellValue = "";
                if (cell == null) {
                    rowList[c] = (cellValue);
                    continue;
                }
                cellValue = ConvertCellStr(cell, cellValue);
                rowList[c] = (cellValue);
//如果为公式型，则读取公式计算后的结果                
                if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
//先转换成字符串型
                	cell.setCellType(Cell.CELL_TYPE_STRING);
                	rowList[c] = (cellValue);
                }
            }
            dataList.add(rowList);
        }
        return dataList;
    }

//将Excel的单元格中的内容转换成字符串
    private static String ConvertCellStr(Cell cell, String cellStr) {
        switch (cell.getCellType()) {
            case Cell.CELL_TYPE_STRING:
                // 读取String
                cellStr = cell.getStringCellValue().toString();
                break;
            case Cell.CELL_TYPE_BOOLEAN:
                // 得到Boolean对象的方法
                cellStr = String.valueOf(cell.getBooleanCellValue());
                break;
            case Cell.CELL_TYPE_NUMERIC:
                // 先看是否是日期格式
                if (DateUtil.isCellDateFormatted(cell)) {
                    // 读取日期格式
                    cellStr = formatTime(cell.getDateCellValue().toString());
                } else {
                    // 读取数字
                    cellStr = String.valueOf(cell.getNumericCellValue());
                }
                break;
            case Cell.CELL_TYPE_FORMULA:
                // 读取公式
                cellStr = cell.getCellFormula().toString();
                break;
        }
        return cellStr;
    }

//通过判断拓展名为xlsx来判断是否使用的是Excel2007    
    private static boolean isExcel2007(String fileName) {
        return fileName.matches("^.+\\.(?i)(xlsx)$");
    }

//调整时间格式，支持中英式时间格式的适配    
    private static String formatTime(String s) {
        SimpleDateFormat sf = new SimpleDateFormat("EEE MMM dd hh:mm:ss z yyyy", Locale.ENGLISH);
        Date date = null;
        try {
            date = sf.parse(s);
        } catch (ParseException ex) {
            Logger.getLogger(ExcelUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String result = sdf.format(date);
        return result;
    }

//写Excel，支持拓展名为xls的老版本Excel文件   office2003 
    public static void writeExcelOldVersion(String filePath,String sheetName) throws Exception {  
        List<List<String>> dateList = new ExcelListSource().listSource();  
        HSSFWorkbook wb = new HSSFWorkbook();
//增加sheet，名称为参数中给定的值，支持中文
        HSSFSheet sheet = wb.createSheet(sheetName);
        // 表格样式  
        HSSFCellStyle style = wb.createCellStyle();  
        style.setAlignment(HSSFCellStyle.ALIGN_CENTER);// 指定单元格居中对齐  
        // // 边框  
        // style.setBorderBottom(HSSFCellStyle.BORDER_MEDIUM);  
        // style.setBorderTop(HSSFCellStyle.BORDER_MEDIUM);  
        // style.setBorderLeft(HSSFCellStyle.BORDER_MEDIUM);  
        // style.setBorderRight(HSSFCellStyle.BORDER_MEDIUM);  
        // //设置字体  
        // HSSFFont f = wb.createFont();  
        // f.setFontHeightInPoints((short)10);  
        // f.setBoldweight(HSSFFont.BOLDWEIGHT_NORMAL);  
        // style.setFont(f);  
        // //设置列宽  
        // sheet.setColumnWidth((short)0, (short)9600);  
        // sheet.setColumnWidth((short)1, (short)4000);  
        // sheet.setColumnWidth((short)2, (short)8000);  
        // sheet.setColumnWidth((short)3, (short)8000);  

// 在索引0的位置创建第一行   
        for (int i = 0; i < dateList.size(); i++) {  
            HSSFRow row = sheet.createRow(i);  
            List<String> list = dateList.get(i);  
            for (int j = 0; j < list.size(); j++) {  
                HSSFCell cell = row.createCell(j);  
                cell.setCellValue(list.get(j));  
                cell.setCellStyle(style);  
            }  
        }  
        // 导出文件  
        FileOutputStream fout = new FileOutputStream(filePath);  
        wb.write(fout);  
        fout.close();  
    } 
    
//写Excel，支持拓展名为xlsx的新版本Excel文件      office2007
    public static void writeExcelNewVersion(String filePath,String sheetName) throws Exception {  
        List<List<String>> dateList = new ExcelListSource().listSource();
        XSSFWorkbook wb = new XSSFWorkbook();
//增加sheet，名称为参数中给定的值，支持中文    
        XSSFSheet sheet = wb.createSheet(sheetName);  
        // 表格样式  
        XSSFCellStyle style = wb.createCellStyle(); 
        style.setAlignment(XSSFCellStyle.ALIGN_CENTER);// 指定单元格居中对齐  
        // 边框  
        // style.setBorderBottom(HSSFCellStyle.BORDER_MEDIUM);  
        // style.setBorderTop(HSSFCellStyle.BORDER_MEDIUM);  
        // style.setBorderLeft(HSSFCellStyle.BORDER_MEDIUM);  
        // style.setBorderRight(HSSFCellStyle.BORDER_MEDIUM);  
        //设置字体  
        // HSSFFont f = wb.createFont();  
        // f.setFontHeightInPoints((short)10);  
        // f.setBoldweight(HSSFFont.BOLDWEIGHT_NORMAL);  
        // style.setFont(f);  
        //设置列宽  
        // sheet.setColumnWidth((short)0, (short)9600);  
        // sheet.setColumnWidth((short)1, (short)4000);  
        // sheet.setColumnWidth((short)2, (short)8000);  
        // sheet.setColumnWidth((short)3, (short)8000);  
// 在索引0的位置创建第一行   
        for (int i = 0; i < dateList.size(); i++) {  
            XSSFRow row = sheet.createRow(i);  
            List<String> list = dateList.get(i);  
            for (int j = 0; j < list.size(); j++) {  
                XSSFCell cell = row.createCell(j);  
                cell.setCellValue(list.get(j));  
                cell.setCellStyle(style);  
            }  
        }  
        // 导出文件  
        FileOutputStream fout = new FileOutputStream(filePath);
        wb.write(fout);  
        fout.close();  
    }

//写Excel，支持拓展名为xlsx的新版本Excel文件，指定在一个Cell中进行值设置      office2007
//参数说明：filePath-->生成的Excel的路径
//sheetName-->sheet名设置  setValue-->往单元格中填入的值
//rowNum-->行号，从0开始
//calNum-->列号，从0开始
//注意：仅仅是覆盖，会把原内容删除    
    public static void createExcelNewVersion(String filePath,String sheetName,double setValue,int rowNum,int colNum) throws Exception {  
        XSSFWorkbook wb = new XSSFWorkbook();        
//增加sheet，名称为参数中给定的值，支持中文    
        XSSFSheet sheet = wb.createSheet(sheetName);  
        // 表格样式  
        XSSFCellStyle style = wb.createCellStyle(); 
        style.setAlignment(XSSFCellStyle.ALIGN_CENTER);// 指定单元格居中对齐  
// 在索引0的位置创建第一行     
            XSSFRow row = sheet.createRow(rowNum);    
            XSSFCell cell = row.createCell(colNum);
            cell.setCellValue(setValue);
            cell.setCellStyle(style);         
        // 导出文件  
        FileOutputStream fileout = new FileOutputStream(filePath);  
        wb.write(fileout);  
        fileout.close();  
   } 
    
    
//更新指定单元格位置的Excel内容 ，更新double类型数据
    public static void updateExcelDouble(String filePath,int rowNum,int cellNum,double setValue) throws FileNotFoundException, InvalidFormatException, IOException{
    	File file = new File(filePath);
        InputStream is = null;
        try {
            is = new FileInputStream(file);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ExcelUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        Workbook wb = new XSSFWorkbook(is);
//获取第一个sheet			
		Sheet sheet = wb.getSheetAt(0);
//设置获取第几行			
		Row rowX = sheet.getRow(rowNum);
//设置获取第几列
			Cell celX = rowX.getCell(cellNum);
			celX.setCellType(Cell.CELL_TYPE_STRING);
			celX.setCellValue(setValue);
		// Write the output to a file
		FileOutputStream fileOut = new FileOutputStream(filePath);
		wb.write(fileOut);
		fileOut.close();
    }

    
  //更新指定单元格位置的Excel内容 ，更新double类型数据
    public static void updateExcelString(String filePath,int rowNum,int cellNum,String setValue) throws FileNotFoundException, InvalidFormatException, IOException{
    	File file = new File(filePath);
        InputStream is = null;
        try {
            is = new FileInputStream(file);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ExcelUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        Workbook wb = new XSSFWorkbook(is);
//获取第一个sheet			
		Sheet sheet = wb.getSheetAt(0);
//设置获取第几行			
		Row rowX = sheet.getRow(rowNum);
//设置获取第几列
			Cell celX = rowX.getCell(cellNum);
			celX.setCellType(Cell.CELL_TYPE_STRING);
			celX.setCellValue(setValue);
		// Write the output to a file
		FileOutputStream fileOut = new FileOutputStream(filePath);
		wb.write(fileOut);
		fileOut.close();
    }
    
    
//给指定单元格位置设置公式
    public static void setCellFormula(String filePath,int rowNum,int cellNum,String setFormula) throws Exception{
    	File file = new File(filePath);
        InputStream is = null;
        try {
            is = new FileInputStream(file);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ExcelUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        Workbook wb = new XSSFWorkbook(is);
//获取第一个sheet			
		Sheet sheet = wb.getSheetAt(0);
//设置获取第几行			
		Row rowX = sheet.getRow(rowNum);
//设置获取第几列
		Cell celX = rowX.getCell(cellNum);
//设置强制执行再运算		
		sheet.setForceFormulaRecalculation(true);
		celX.setCellFormula(setFormula);
		// Write the output to a file
		FileOutputStream fileOut = new FileOutputStream(filePath);
		wb.write(fileOut);
		fileOut.close();
    }
         
//读取指定单个单元格的数据    
    
    public static String getValue(String excelPath,String cellLocation) throws Exception {           
        String value = null;
        FileInputStream fis = new FileInputStream(excelPath);
        Workbook wb = new HSSFWorkbook(fis);
        Sheet sheet = wb.getSheetAt(0);
        CellReference cellReference = new CellReference(cellLocation);   
        Row row = sheet.getRow(cellReference.getRow());  
        Cell cell = row.getCell(cellReference.getCol());
        FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
        switch (cell.getCellType()) {  
            case HSSFCell.CELL_TYPE_NUMERIC:                        //数值型  
                if (HSSFDateUtil.isCellDateFormatted(cell)) {       //如果是时间类型  
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");  
                    value = format.format(cell.getDateCellValue());  
                } else {                                            //纯数字  
                    value = String.valueOf(cell.getNumericCellValue());  
                }  
                break;  
            case HSSFCell.CELL_TYPE_STRING:                         //字符串型  
                value = cell.getStringCellValue();  
                break;  
            case HSSFCell.CELL_TYPE_BOOLEAN:                        //布尔  
                value = " " + cell.getBooleanCellValue();  
                break;  
            case HSSFCell.CELL_TYPE_BLANK:                          //空值  
                value = "";  
                break;  
            case HSSFCell.CELL_TYPE_ERROR:                          //故障  
                value = "";  
                break;  
            case HSSFCell.CELL_TYPE_FORMULA:                        //公式型  
                try {  
                    CellValue cellValue;  
                    cellValue = evaluator.evaluate(cell);  
                    switch (cellValue.getCellType()) {              //判断公式类型  
                        case Cell.CELL_TYPE_BOOLEAN:  
                            value  = String.valueOf(cellValue.getBooleanValue());  
                            break;  
                        case Cell.CELL_TYPE_NUMERIC:  
                            // 处理日期    
                            if (DateUtil.isCellDateFormatted(cell)) {    
                               SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");    
                               Date date = cell.getDateCellValue();    
                               value = format.format(date);  
                            } else {    
                               value  = String.valueOf(cellValue.getNumberValue());  
                            }  
                            break;  
                        case Cell.CELL_TYPE_STRING:  
                            value  = cellValue.getStringValue();  
                            break;  
                        case Cell.CELL_TYPE_BLANK:  
                            value = "";  
                            break;  
                        case Cell.CELL_TYPE_ERROR:  
                            value = "";  
                            break;  
                        case Cell.CELL_TYPE_FORMULA:  
                            value = "";  
                            break;  
                    }  
                } catch (Exception e) {  
                    value = cell.getStringCellValue().toString();  
                    cell.getCellFormula();  
                }  
                break;  
            default:  
                value = cell.getStringCellValue().toString();  
                break;  
        }  
        return value;  
    }  
/*    
    public static void main(String[] args) throws Exception {

    List<String[]> list = ExcelHelper.readExcel("E:/2013年度员工体检名单.xlsx");
    if (list != null) {
        for (int i = 0; i < list.size(); i++) {
            System.out.println("第" + (i + 1) + "行");
            String[] cellList = list.get(i);
            for (int j = 0; j < cellList.length; j++) {
                System.out.print("\t第" + (j + 1) + "列值：");
                System.out.println(cellList[j]);
            }
        }
    }
//     ExcelHelper.writeExcelOldVersion("E:/群组.xls","测试生成的sheetName");
//     ExcelHelper.writeExcelNewVersion("E:/群组.xlsx","测试生成的sheetName");
//    	ExcelHelper.createExcelNewVersion("E:/test.xlsx","sheet4Hjg",777,0,0);
    	ExcelHelper.updateExcelDouble("E:/test.xlsx", 0, 0, 12473.35);
    	ExcelHelper.updateExcelDouble("E:/test.xlsx", 0, 1, 438.1036);
    	ExcelHelper.updateExcelDouble("E:/test.xlsx", 0, 2, 13.15539);
    	ExcelHelper.setCellFormula("E:/test.xlsx", 0, 3, "AVERAGE(A1:C1)");
    } */
    public static void main(String[] args) throws Exception {
    	ExcelHelper.getValue("./log/TestLog/MerrillLynch/ML-44/YieldRateCalc.xlsx","D1");
    }
    
}
