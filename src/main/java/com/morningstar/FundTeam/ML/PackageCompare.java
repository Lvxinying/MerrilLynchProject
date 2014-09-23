package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.morningstar.FundAutoTest.commons.Helper;

public class PackageCompare {

	/**
	 * @category FTP Connection环节生成的文件与之前生成的文件的比较
	 * @param args
	 */
	
	private static String sourceFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/StockDemoFile/DevENV/20140529/PLP320XC.MSSTOCKS.OLD";
	private static String targetFilePath = "C:/HJG_WORK/HJG_Project/ML_Project/StockDemoFile/DevENV/20140529/PLP320XC.MSSTOCKS.NEW";
	private static List<String> list1 = new ArrayList<String>();
	private static List<String> list2 = new ArrayList<String>();
	
/*	private static void isLineNumSame(String filePathBeforeFTP,String filePathAfterFTP) throws IOException{
		int lineNum1 = Helper.getTotalLinesOfFile(filePathBeforeFTP);
		int lineNum2 = Helper.getTotalLinesOfFile(filePathAfterFTP);
		if(lineNum1 == lineNum2){
			System.out.println("File LineNum is same!");
			System.out.println("LineNum before FTP: "+lineNum1);
			System.out.println("LineNum after FTP: "+lineNum2);
		}else{
			System.out.println("File LineNum isn't same!");
			System.out.println("LineNum before FTP: "+lineNum1);
			System.out.println("LineNum after FTP: "+lineNum2);
		}
	}*/
	
	
	private static void lineCompare(String sourceFilePath,String targetFilePath) throws IOException{		
		List<String> compareResultList = new ArrayList<String>();
		list1 = Helper.readFileList(sourceFilePath);
		list2 = Helper.readFileList(targetFilePath);
		System.out.println("Source File lines number: "+list1.size());
		System.out.println("Target File lines number: "+list2.size());
		if(list1.size() >= list2.size()){
			list1.removeAll(list2);
			compareResultList = list1;
			if(!compareResultList.isEmpty()){
				for(String misContent:compareResultList){
					System.out.println("Miss Content in target file: " + misContent);
				}
			}else{
				System.out.println("No miss contents were found between source and target file!");
			}
		}else{
			list2.removeAll(list1);
			compareResultList = list2;
			if(!compareResultList.isEmpty()){
				for(String misContent : compareResultList){
					System.out.println("Miss Content in source file: " + misContent);
				}				
			}
		}
		
	}
	
	public static void main(String[] args) {
//		System.out.println("[Info]Begin to test Line Number... ...");
//		try {
//			isLineNumSame(filePathBeforeFTP,filePathAfterFTP);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		System.out.println("[Info]Begin to test compare... ...");
//		try {
//			lineCompare(filePathBeforeFTP,filePathAfterFTP);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		try {
			lineCompare(sourceFilePath,targetFilePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
