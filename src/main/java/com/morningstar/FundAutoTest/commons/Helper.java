package com.morningstar.FundAutoTest.commons;

import java.io.BufferedReader;
import java.io.File;   
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;   
import java.io.InputStream;
import java.io.LineNumberReader;
import java.math.BigDecimal;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.regex.*;

import bsh.ParseException;

public class Helper {


//以字节形式读取文本内容,返回给字符串	
	public static void readFileByBytes(String fileName,int byteSize) {
//	        File file = new File(fileName);
	        InputStream in = null;
/*	        try {
	            in = new FileInputStream(file);
	            int tempbyte;
	            while ((tempbyte = in.read()) != -1) {
	                System.out.write(tempbyte);
	            }
	            in.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	            return;
	        }*/
	        

//byteSize为byte数组的大小，取整数
	        try {
	            byte[] tempbytes = new byte[byteSize];
	            int byteread = 0;
	            in = new FileInputStream(fileName);
	            while ((byteread = in.read(tempbytes)) != -1) {
	                System.out.write(tempbytes, 0, byteread);
	            }
	        } catch (Exception e1) {
	            e1.printStackTrace();
	        } finally {
	            if (in != null) {
	                try {
	                    in.close();
	                } catch (IOException e1) {
	                	e1.printStackTrace();
	                }
	            }
	        }
	    }

//逐行去读取一个文件的所有内容，返回一个字符串	
    public static String readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        String text = "";
        try {
            String tempString = null;
            reader = new BufferedReader(new FileReader(file));
// 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {             
            	text += tempString.trim();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                	e1.getMessage();
                }
            }
        }
		return text;
    }
    
    
//按照行读取数据并且返回到一个ArrayList中
    public static List<String> readFileList(String filePath) throws IOException {
    	File file = new File(filePath);
        BufferedReader reader = null;
        List<String> fileList = new ArrayList<String>();
        try {
            String tempString = null;
            reader = new BufferedReader(new FileReader(file));
            // 一次读入一行，直到读入null为文件结束，返回到一个List中，利用List来指定读取哪行的信息
            while ((tempString = reader.readLine()) != null) {
            	fileList.add(tempString);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                	e1.getMessage();
                }
            }
        }
        return fileList;	
    }
    
        
//读取指定行的内容
    public static String readFileInLines(String filePath,int lineNum)  throws IOException{
    	List<String> lineContentList = new ArrayList<String>();
    	if(lineContentList.size() < 1){
    		lineContentList = readFileList(filePath);
    	}
    	int lineNumAct = lineNum - 1;
    	String text = lineContentList.get(lineNumAct);
    	return text;
    }
 
// 文件内容的总行数。 
    public static int getTotalLinesOfFile(String filePath) throws IOException {
        FileReader in = new FileReader(filePath);
        LineNumberReader reader = new LineNumberReader(in);
        String s = reader.readLine();
        int lines = 0;
        while (s != null) {
            lines++;
            s = reader.readLine();
        }
        reader.close();
        in.close();
        return lines;
    }
         
    private enum ValueType{
    	enumInt,enumFloat,enumDouble,enumBoolean
    }
//生成随机数 
    public static Object getRandom(ValueType type){
    	switch(type){
    		case enumInt:
    			return getRandomInt(0,9999);
    		case enumFloat:
    			return getRandomFloat();
    		case enumDouble:
    			return getRandomDouble();
    		case enumBoolean:
    			return getRandomBoolean();
    		default:
    			return null;
    	}    	
    }

 //设置一个返回范围，从min到max的指定范围内的随机整数   
    public static int getRandomInt(int min,int max){
    	Random random = new Random();
    	int intNum = random.nextInt(max)%(max-min+1) + min;
    	return intNum;
    }
    private static Object getRandomFloat(){
    	Random random = new Random();
    	float floatNum = random.nextFloat();
    	return floatNum;
    }
    private static Object getRandomDouble(){
    	Random random = new Random();
    	double doubleNum = random.nextDouble();
    	return doubleNum;
    }
    private static Object getRandomBoolean(){
    	Random random = new Random();
    	boolean booleanNum = random.nextBoolean();
    	return booleanNum;
    }

    
//判断一个字符串对应的数是否为小数(无论正负小树，都可以被判断出来)
    public static boolean isDecimal(String str){
//正则表达式判断是否为小数 
//    	return Pattern.compile("(\\d+\\.\\d+)").matcher(str).matches();
    	return Pattern.compile("[-+]{0,1}\\d+\\.\\d*|[-+]{0,1}\\d*\\.\\d+").matcher(str).matches();    	
    }
//获取小数点位数
    public static int getDecimalScale(String decimal){
    	int digitsNum = 0;
    	if(isDecimal(decimal)){
//获取小数点位置    		
    		int bitPos = decimal.indexOf(".");
//获取小数位数
    		int numOfBits = decimal.length() - bitPos - 1;
    		digitsNum = numOfBits;
    	}
    	else{
    		System.out.println("Not a decimal,can't get the scale of it!");
    		System.out.println(decimal);
    	}
    	return digitsNum;
    }
    
//在字符串中返回某子字符串出现的次数，正则方式(仅适合字符串较短的重复比较，性能不够好)
    public static int getMatchCount(String matchStr,String targetStr){ 
        Pattern p = Pattern.compile(matchStr);  
        Matcher m = p.matcher(targetStr);       
        int matchcount = 0;  
        while(m.find()){  
        	matchcount++;  
        }  
        return matchcount;            
    }
    
//设置Double精度
    public static double setDoublePrecision(double doubleValue, int scale,int roundingMode) {  
       BigDecimal bd = new BigDecimal(doubleValue);  
       bd = bd.setScale(scale,roundingMode);  
       double d = bd.doubleValue();  
       bd = null;  
       return d;  
   }
    
//设置Double多余位补零 用法：String realYieldRateInDb = Helper.addZeroForDouble(DactYieldRateInDb, "0.00000");
    public static String addZeroForDouble(double value,String doubleFormat){
    	DecimalFormat df = new DecimalFormat(doubleFormat);
    	String dValue = df.format(value);
    	return dValue;
    }
    
    public static String doubleToString( Double input )
    {
        if ( input.isNaN() || input.isInfinite() )
            return null;
        else            
        {
            //return m_f.format(input);
            String result =(new DecimalFormat("0.00000000000000")).format(input);
            
            if(result.contains("."))
            {
                while ( result.endsWith("0") )
                {
                    result = result.substring(0, result.length()-1);
                }
            }
            return result;
        }
    }
    
//处理日期类数据
//用法，如：String strDate = Helper.dateFormat(testdate,"yyyy-MM-dd")    
    public static  String dateFormat(Date date,String dateFormat)throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        return sdf.format(date);
   }

//用法，如：Date testDate = Helper.dateParse(testdate,"yyyy-MM-dd")      
   public static Date dateParse(String strDate,String dateFormat) throws ParseException, java.text.ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        return sdf.parse(strDate);
   }
//获取指定日期所在月的最后一天
    public static Date lastDayOfMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.roll(Calendar.DAY_OF_MONTH, -1);
        return cal.getTime();
    }

//转换日期的月份,要求日期的格式必须为yyyy-MM-dd,false为后退，true为向前,monthRange必须为12以内的值，用法String newDate = changeMonthOfDate("2013-1-4",4,false)
    public static String changeMonthOfDate(String date,int monthRange,boolean isPositive){
    	String newDate = "";
    	String newMonthStr = "";
    	String newYearStr = "";
    	boolean isLeapYear = false;
    	int diffValue = 0;
    	int realMonth = 0;
    	int realYear = 0;
    	String[] ele = date.split("-", 3);
    	String year = ele[0];
    	String month = ele[1];
    	String day = ele[2];
    	int monthInt = Integer.parseInt(month);
    	
    	if(isPositive == true){
    		int newMonthInt = monthInt + monthRange;
    		diffValue = newMonthInt - 12;
    		if(diffValue == 0){
    			newYearStr = year;
//月份变成12月   			
    			newMonthStr = "12";
//若日期为月底最后一天，则全部转成12月31号    			
    			if(day=="28"||day=="29"||day=="30"||day=="31"){
    				newDate = newYearStr+"-"+newMonthStr+"-"+"31";
    			}else{
    			newDate = newYearStr+"-"+newMonthStr+"-"+day;
    			}
    		}
    		if(diffValue < 0){
    			realYear = Integer.parseInt(year);
    			newYearStr =String.valueOf(realYear);
    			realMonth = newMonthInt;
    			if(day.equals("28")||day.equals("29")||day.equals("30")||day.equals("31")){
//1,3,5,7,8,10,12月的日期全部变成31    			
        			if(realMonth==1||realMonth==3||realMonth==5||realMonth==7||realMonth==8||realMonth==10||realMonth==12){
        				newMonthStr = String.valueOf(realMonth);
            			newDate = newYearStr+"-"+newMonthStr+"-"+"31";
        			}
//2月份需判断平年闰年        			
        			if(realMonth==2){
        			 isLeapYear = isCommonOrLeapYear(newYearStr);
	        			 if(isLeapYear==true){
	        				newMonthStr = String.valueOf(realMonth);
	             			newDate = newYearStr+"-"+newMonthStr+"-"+"29"; 
	        			 }else{
	        				newMonthStr = String.valueOf(realMonth);
	              			newDate = newYearStr+"-"+newMonthStr+"-"+"28";
	        			 }
        			}
//4,5,9,11月的日期全部变成30
        			if(realMonth==4||realMonth==5||realMonth==9||realMonth==11){
        				newMonthStr = String.valueOf(realMonth);
              			newDate = newYearStr+"-"+newMonthStr+"-"+"30";
        			}
    			}    			
    		}
    		if(diffValue > 0 && diffValue <12){
    			realYear = Integer.parseInt(year) + 1;
    			newYearStr = String.valueOf(realYear);
    			realMonth = diffValue;
    			newMonthStr = String.valueOf(realMonth);
    			newDate = newYearStr+"-"+newMonthStr+"-"+day;
    		}
    		if(diffValue == 12){
    			realYear = Integer.parseInt(year) + 2;
    			newYearStr = String.valueOf(realYear);
    			newMonthStr = month;
    			newDate = newYearStr+"-"+newMonthStr+"-"+day;
    		}
    		else if(monthRange>12){
    			System.out.println("Please setting your monthRange not over 12!");
    		}
    	}
    	
    	if(isPositive == false){
    		int newMonthInt = monthInt - monthRange;
    		if(newMonthInt > 0){
    			newYearStr = year;
    			newMonthStr = String.valueOf(newMonthInt);
    			if(day.equals("28")||day.equals("29")||day.equals("30")||day.equals("31")){
    				if(newMonthStr=="1"||newMonthStr=="3"||newMonthStr=="5"||newMonthStr=="7"||newMonthStr=="8"||newMonthStr=="10"||newMonthStr=="12"){
            			newDate = newYearStr+"-"+newMonthStr+"-"+"31";
        			}
    				if(newMonthInt == 2){   					
    					isLeapYear = isCommonOrLeapYear(newYearStr);
    					if(isLeapYear==true){
    						newDate = newYearStr+"-"+newMonthStr+"-"+"29";
    					}else{
    						newDate = newYearStr+"-"+newMonthStr+"-"+"28";
    					}
    				}
    				if(newMonthInt==4||newMonthInt==6||newMonthInt==9||newMonthInt==11){
    					newMonthStr = String.valueOf(newMonthInt);
    					newDate = newYearStr+"-"+newMonthStr+"-"+"30";
    				}
    			}else{
    				newMonthStr = String.valueOf(newMonthInt);
					newDate = newYearStr+"-"+newMonthStr+"-"+day;
    			}
    			
    		}
    		if(newMonthInt == 0){
    			realYear = Integer.parseInt(year)-1;
    			newYearStr = String.valueOf(realYear);
    			newMonthStr = "12";
    			newDate = newYearStr+"-"+newMonthStr+"-"+day;
    		}
    		if(newMonthInt < 0 && newMonthInt > -12){
    			realYear = Integer.parseInt(year)-1;
    			newYearStr = String.valueOf(realYear);
    			realMonth = 12 + newMonthInt;
    			newMonthStr =String.valueOf(realMonth);
    			if(day.equals("28")||day.equals("29")||day.equals("30")|day.equals("31")){
    				if(realMonth==1||realMonth==3||realMonth==5||realMonth==7||realMonth==8||realMonth==10||realMonth==12){
    					newDate = newYearStr+"-"+newMonthStr+"-"+"31";
    				}
    				if(realMonth==2){
    					isLeapYear = isCommonOrLeapYear(newYearStr);
    					if(isLeapYear==true){
    						newDate = newYearStr+"-"+newMonthStr+"-"+"29";
    					}else{
    						newDate = newYearStr+"-"+newMonthStr+"-"+"28";
    					}
    				}
    				if(realMonth==4||realMonth==6||realMonth==9||realMonth==11){
    					newDate = newYearStr+"-"+newMonthStr+"-"+"30";
    				}
    			}else{
    			newDate = newYearStr+"-"+newMonthStr+"-"+day;
    			}
    		}
    		if(newMonthInt == -12){
    			realYear = Integer.parseInt(year)-2;
    			newYearStr = String.valueOf(realYear);
    			newMonthStr ="12";
    			newDate = newYearStr+"-"+newMonthStr+"-"+day;
    		}
    		else if(monthRange>12){
    			System.out.println("Please setting your monthRange not over 12!");
    		}
    	}  	
    	return newDate;
    }
 
//false为平年 true为闰年    
    public static boolean isCommonOrLeapYear(String year){
    	boolean result = false;
    	int yearInt = Integer.parseInt(year);
    	if(yearInt % 4 == 0){
    		if(yearInt % 400 ==0){
    			result = true;
    		}else if(yearInt % 100 != 0){
    			result = true;
    		}
    	}else{
    		result = false;
    	}
        return result;
    }
    
    public static void main(String[] args) throws ParseException, Exception{
//    	String dateStr1 = "2014-03-31";
//    	Date date1 = Helper.dateParse(dateStr1, "yyyy-MM-dd");
//    	Date monthEndOfCurrentMonth = lastDayOfMonth(date1);
//    	String newdate = changeMonthOfDate(dateStr1, 4, false);
//    	System.out.println(newdate);
    }
}
