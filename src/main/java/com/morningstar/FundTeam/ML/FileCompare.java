package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.morningstar.FundAutoTest.commons.Helper;

public class FileCompare {
	private static Logger log = Logger.getLogger(FileCompare.class);
	private static String sourceFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/GIDRetirement/StagingENV_NEW_GIDSOURCE/PLP320XB.MSHYBFND";
	private static String targetFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/GIDRetirement/ProdENV_OLD_GIDSOURCE/PLP320XB.MSHYBFND";
	private static List<String> sourceListContent = new ArrayList<String>();
	private static List<String> targetListContent = new ArrayList<String>();
	private static Map<String,String> sourceMap = new HashMap<String,String>();
	private static Map<String,String> targetMap = new HashMap<String,String>();
   	
	private static void lineCompareFundSEDOL(String sourceFilePath,String targetFilePath) throws IOException{		
		sourceListContent = Helper.readFileList(sourceFilePath);
		targetListContent = Helper.readFileList(targetFilePath);

		String fileSEDOL = "";
		String fileSecId = "";
		
		System.out.println("Source File lines number: "+sourceListContent.size());
		System.out.println("Target File lines number: "+targetListContent.size());

//Prepare Source DataMap
		long startTimeSource = System.currentTimeMillis();
		for(String line:sourceListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);				
				fileSEDOL = element[1];
				fileSecId = element[7];
				sourceMap.put(fileSecId, fileSEDOL);
			}
		}
		long endTimeSource = System.currentTimeMillis() - startTimeSource;
		System.out.println("[INFO]Building source DataMap finished,totally cost: "+endTimeSource/1000+" s");
	
//Prepare Target DataMap
		long startTimeTarget = System.currentTimeMillis();
		for(String line:targetListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);
				
				fileSEDOL = element[1];			
				fileSecId = element[7];
				targetMap.put(fileSecId, fileSEDOL);
			}
		}
		long endTimeTarget = System.currentTimeMillis() - startTimeTarget;
		System.out.println("[INFO]Building target DataMap finished,totally cost: "+endTimeTarget/1000+" s");
//Compare
//Base from source
		System.out.println("[INFO]Begin to compare... ...");
		long startTimeCompare = System.currentTimeMillis();
		if(!sourceMap.isEmpty() && !targetMap.isEmpty()){
			for(Entry<String,String> entry:sourceMap.entrySet()){
				if(targetMap.containsKey(entry.getKey())){
					if(!entry.getValue().isEmpty() && !targetMap.get(entry.getKey()).isEmpty()){
						if(!entry.getValue().equals(targetMap.get(entry.getKey()))){
							log.error("InvestmentId="+entry.getKey());
							log.error("SEDOL mismatch!SEDOL from source: "+entry.getValue()+" || SEDOL from target: "+targetMap.get(entry.getKey()));
						}
					}					
				}
			}
		}
		long endTimeCompare = System.currentTimeMillis() - startTimeCompare;
		System.out.println("[INFO]Compare finished,time cost: "+endTimeCompare/1000+" s");
		if(!sourceMap.isEmpty()){
			sourceMap.clear();
		}
		if(!targetMap.isEmpty()){
			targetMap.clear();
		}
	}
	
	private static void lineCompareFundCUSIP(String sourceFilePath,String targetFilePath) throws IOException{		
		sourceListContent = Helper.readFileList(sourceFilePath);
		targetListContent = Helper.readFileList(targetFilePath);

		String fileCUSIP = "";
		String fileSecId = "";
		
		System.out.println("Source File lines number: "+sourceListContent.size());
		System.out.println("Target File lines number: "+targetListContent.size());

//Prepare Source DataMap
		long startTimeSource = System.currentTimeMillis();
		for(String line:sourceListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);				
				fileCUSIP = element[0];
				fileSecId = element[7];
				sourceMap.put(fileSecId, fileCUSIP);
			}
		}
		long endTimeSource = System.currentTimeMillis() - startTimeSource;
		System.out.println("[INFO]Building source DataMap finished,totally cost: "+endTimeSource/1000+" s");
	
//Prepare Target DataMap
		long startTimeTarget = System.currentTimeMillis();
		for(String line:targetListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);
				
				fileCUSIP = element[0];			
				fileSecId = element[7];
				targetMap.put(fileSecId, fileCUSIP);
			}
		}
		long endTimeTarget = System.currentTimeMillis() - startTimeTarget;
		System.out.println("[INFO]Building target DataMap finished,totally cost: "+endTimeTarget/1000+" s");
//Compare
//Base from source
		System.out.println("[INFO]Begin to compare... ...");
		long startTimeCompare = System.currentTimeMillis();
		if(!sourceMap.isEmpty() && !targetMap.isEmpty()){
			for(Entry<String,String> entry:sourceMap.entrySet()){
				if(targetMap.containsKey(entry.getKey())){
					if(!entry.getValue().isEmpty() && !targetMap.get(entry.getKey()).isEmpty()){
						if(!entry.getValue().equals(targetMap.get(entry.getKey()))){
							log.error("InvestmentId="+entry.getKey());
							log.error("CUSIP mismatch!CUSIP from source: "+entry.getValue()+" || CUSIP from target: "+targetMap.get(entry.getKey()));
						}
					}					
				}
			}
		}
		long endTimeCompare = System.currentTimeMillis() - startTimeCompare;
		System.out.println("[INFO]Compare finished,time cost: "+endTimeCompare/1000+" s");
		if(!sourceMap.isEmpty()){
			sourceMap.clear();
		}
		if(!targetMap.isEmpty()){
			targetMap.clear();
		}
	}
	
	private static void lineCompareFundISIN(String sourceFilePath,String targetFilePath) throws IOException{		
		sourceListContent = Helper.readFileList(sourceFilePath);
		targetListContent = Helper.readFileList(targetFilePath);

		String fileISIN = "";
		String fileSecId = "";
		
		System.out.println("Source File lines number: "+sourceListContent.size());
		System.out.println("Target File lines number: "+targetListContent.size());

//Prepare Source DataMap
		long startTimeSource = System.currentTimeMillis();
		for(String line:sourceListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);				
				fileISIN = element[2];
				fileSecId = element[7];
				sourceMap.put(fileSecId, fileISIN);
			}
		}
		long endTimeSource = System.currentTimeMillis() - startTimeSource;
		System.out.println("[INFO]Building source DataMap finished,totally cost: "+endTimeSource/1000+" s");
	
//Prepare Target DataMap
		long startTimeTarget = System.currentTimeMillis();
		for(String line:targetListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);
				
				fileISIN = element[2];			
				fileSecId = element[7];
				targetMap.put(fileSecId, fileISIN);
			}
		}
		long endTimeTarget = System.currentTimeMillis() - startTimeTarget;
		System.out.println("[INFO]Building target DataMap finished,totally cost: "+endTimeTarget/1000+" s");
//Compare
//Base from source
		System.out.println("[INFO]Begin to compare... ...");
		long startTimeCompare = System.currentTimeMillis();
		if(!sourceMap.isEmpty() && !targetMap.isEmpty()){
			for(Entry<String,String> entry:sourceMap.entrySet()){
				if(targetMap.containsKey(entry.getKey())){
					if(!entry.getValue().isEmpty() && !targetMap.get(entry.getKey()).isEmpty()){
						if(!entry.getValue().equals(targetMap.get(entry.getKey()))){
							log.error("InvestmentId="+entry.getKey());
							log.error("ISIN mismatch!ISIN from source: "+entry.getValue()+" || ISIN from target: "+targetMap.get(entry.getKey()));
						}
					}					
				}
			}
		}
		long endTimeCompare = System.currentTimeMillis() - startTimeCompare;
		System.out.println("[INFO]Compare finished,time cost: "+endTimeCompare/1000+" s");
		if(!sourceMap.isEmpty()){
			sourceMap.clear();
		}
		if(!targetMap.isEmpty()){
			targetMap.clear();
		}
	}
	
	private static void lineCompareFundExchangeId(String sourceFilePath,String targetFilePath) throws IOException{		
		sourceListContent = Helper.readFileList(sourceFilePath);
		targetListContent = Helper.readFileList(targetFilePath);

		String fileExchangeId = "";
		String fileSecId = "";
		
		System.out.println("Source File lines number: "+sourceListContent.size());
		System.out.println("Target File lines number: "+targetListContent.size());

//Prepare Source DataMap
		long startTimeSource = System.currentTimeMillis();
		for(String line:sourceListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);				
				fileExchangeId = element[3];
				fileSecId = element[7];
				sourceMap.put(fileSecId, fileExchangeId);
			}
		}
		long endTimeSource = System.currentTimeMillis() - startTimeSource;
		System.out.println("[INFO]Building source DataMap finished,totally cost: "+endTimeSource/1000+" s");
	
//Prepare Target DataMap
		long startTimeTarget = System.currentTimeMillis();
		for(String line:targetListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);
				
				fileExchangeId = element[3];			
				fileSecId = element[7];
				targetMap.put(fileSecId, fileExchangeId);
			}
		}
		long endTimeTarget = System.currentTimeMillis() - startTimeTarget;
		System.out.println("[INFO]Building target DataMap finished,totally cost: "+endTimeTarget/1000+" s");
//Compare
//Base from source
		System.out.println("[INFO]Begin to compare... ...");
		long startTimeCompare = System.currentTimeMillis();
		if(!sourceMap.isEmpty() && !targetMap.isEmpty()){
			for(Entry<String,String> entry:sourceMap.entrySet()){
				if(targetMap.containsKey(entry.getKey())){
					if(!entry.getValue().isEmpty() && !targetMap.get(entry.getKey()).isEmpty()){
						if(!entry.getValue().equals(targetMap.get(entry.getKey()))){
							log.error("InvestmentId="+entry.getKey());
							log.error("ExchangeId mismatch!ISIN from source: "+entry.getValue()+" || ExchangeId from target: "+targetMap.get(entry.getKey()));
						}
					}					
				}
			}
		}
		long endTimeCompare = System.currentTimeMillis() - startTimeCompare;
		System.out.println("[INFO]Compare finished,time cost: "+endTimeCompare/1000+" s");
		if(!sourceMap.isEmpty()){
			sourceMap.clear();
		}
		if(!targetMap.isEmpty()){
			targetMap.clear();
		}
	}
	
	private static void lineCompareFundSymbol(String sourceFilePath,String targetFilePath) throws IOException{		
		sourceListContent = Helper.readFileList(sourceFilePath);
		targetListContent = Helper.readFileList(targetFilePath);

		String fileTickerSymbol = "";
		String fileSecId = "";
		
		System.out.println("Source File lines number: "+sourceListContent.size());
		System.out.println("Target File lines number: "+targetListContent.size());

//Prepare Source DataMap
		long startTimeSource = System.currentTimeMillis();
		for(String line:sourceListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);				
				fileTickerSymbol = element[4];
				fileSecId = element[7];
				sourceMap.put(fileSecId, fileTickerSymbol);
			}
		}
		long endTimeSource = System.currentTimeMillis() - startTimeSource;
		System.out.println("[INFO]Building source DataMap finished,totally cost: "+endTimeSource/1000+" s");
	
//Prepare Target DataMap
		long startTimeTarget = System.currentTimeMillis();
		for(String line:targetListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);
				
				fileTickerSymbol = element[4];			
				fileSecId = element[7];
				targetMap.put(fileSecId, fileTickerSymbol);
			}
		}
		long endTimeTarget = System.currentTimeMillis() - startTimeTarget;
		System.out.println("[INFO]Building target DataMap finished,totally cost: "+endTimeTarget/1000+" s");
//Compare
//Base from source
		System.out.println("[INFO]Begin to compare... ...");
		long startTimeCompare = System.currentTimeMillis();
		if(!sourceMap.isEmpty() && !targetMap.isEmpty()){
			for(Entry<String,String> entry:sourceMap.entrySet()){
				if(targetMap.containsKey(entry.getKey())){
					if(!entry.getValue().isEmpty() && !targetMap.get(entry.getKey()).isEmpty()){
						if(!entry.getValue().equals(targetMap.get(entry.getKey()))){
							log.error("InvestmentId="+entry.getKey());
							log.error("Symbol mismatch!Symbol from source: "+entry.getValue()+" || Symbol from target: "+targetMap.get(entry.getKey()));
						}
					}					
				}
			}
		}
		long endTimeCompare = System.currentTimeMillis() - startTimeCompare;
		System.out.println("[INFO]Compare finished,time cost: "+endTimeCompare/1000+" s");
		if(!sourceMap.isEmpty()){
			sourceMap.clear();
		}
		if(!targetMap.isEmpty()){
			targetMap.clear();
		}
	}
	
	private static void lineCompareFundDomicile(String sourceFilePath,String targetFilePath) throws IOException{		
		sourceListContent = Helper.readFileList(sourceFilePath);
		targetListContent = Helper.readFileList(targetFilePath);

		String fileDomicile = "";
		String fileSecId = "";
		
		System.out.println("Source File lines number: "+sourceListContent.size());
		System.out.println("Target File lines number: "+targetListContent.size());

//Prepare Source DataMap
		long startTimeSource = System.currentTimeMillis();
		for(String line:sourceListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);				
				fileDomicile = element[5];
				fileSecId = element[7];
				sourceMap.put(fileSecId, fileDomicile);
			}
		}
		long endTimeSource = System.currentTimeMillis() - startTimeSource;
		System.out.println("[INFO]Building source DataMap finished,totally cost: "+endTimeSource/1000+" s");
	
//Prepare Target DataMap
		long startTimeTarget = System.currentTimeMillis();
		for(String line:targetListContent){
			if(line.contains("UHDR") || line.contains("UTRL")){
				continue;
			}else{
				String element[] = line.split("~",16);
				
				fileDomicile = element[5];			
				fileSecId = element[7];
				targetMap.put(fileSecId, fileDomicile);
			}
		}
		long endTimeTarget = System.currentTimeMillis() - startTimeTarget;
		System.out.println("[INFO]Building target DataMap finished,totally cost: "+endTimeTarget/1000+" s");
//Compare
//Base from source
		System.out.println("[INFO]Begin to compare... ...");
		long startTimeCompare = System.currentTimeMillis();
		if(!sourceMap.isEmpty() && !targetMap.isEmpty()){
			for(Entry<String,String> entry:sourceMap.entrySet()){
				if(targetMap.containsKey(entry.getKey())){
					if(!entry.getValue().isEmpty() && !targetMap.get(entry.getKey()).isEmpty()){
						if(!entry.getValue().equals(targetMap.get(entry.getKey()))){
							log.error("InvestmentId="+entry.getKey());
							log.error("Domicile mismatch!Symbol from source: "+entry.getValue()+" || Domicile from target: "+targetMap.get(entry.getKey()));
						}
					}					
				}
			}
		}
		long endTimeCompare = System.currentTimeMillis() - startTimeCompare;
		System.out.println("[INFO]Compare finished,time cost: "+endTimeCompare/1000+" s");
		if(!sourceMap.isEmpty()){
			sourceMap.clear();
		}
		if(!targetMap.isEmpty()){
			targetMap.clear();
		}
	}
	
	public static void main(String[] args) {
		try {
			lineCompareFundSEDOL(sourceFilePath,targetFilePath);
//			lineCompareFundCUSIP(sourceFilePath,targetFilePath);
//			lineCompareFundISIN(sourceFilePath,targetFilePath);
//			lineCompareFundExchangeId(sourceFilePath,targetFilePath);
//			lineCompareFundSymbol(sourceFilePath,targetFilePath);
//			lineCompareFundDomicile(sourceFilePath,targetFilePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
