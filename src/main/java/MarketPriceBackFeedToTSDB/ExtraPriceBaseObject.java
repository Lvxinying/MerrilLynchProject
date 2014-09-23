package MarketPriceBackFeedToTSDB;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.data.tsapi.TSException;

public class ExtraPriceBaseObject {
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName1 = "MissingRecord_DBI_ExtraPrice(BaseObject).log";
	private static String logName2 = "ContentChecking_DBI_ExtraPrice(BaseObject).log";
	static SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
//Get PerformanceIdList(EOD Mode)
	private static List<String> getUpdatedPerformanceIdListEODMode(String tableName,String createdOnTime) throws SQLException{
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//保留CREATEDON的使用
		if(createdOnTime.isEmpty()){
			createdOnTime = "2000-01-01";
		}
		String sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
								 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
								 " WHERE ND.IDTYPE = 2 AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
		list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got "+ list.size()+" performanceids(EOD MODE),time cost: "+endTime+" ms");
		return list;
	}

//Get PerformanceIdList(Individual Mode)
	private static List<String> getUpdatedPerformanceIdListIndividualMode(String testedTableName,int TsType, int testedIdCounts){
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//选取IdType != 2的所有情况，且PROCESSNAME为DMRI for Equity或者DMRI for ETF才能保证是BASE OBJECT的		
		String sqlToGetUpdatedPerformanceIdList = "SELECT ID FROM ADT_NOTIFICATIONDRIVER WHERE PROCESSNAME IN('DMRI for Equity','DMRI for ETF') " +
				"AND IDTYPE != 2 TSTYPE = " + TsType + " AND TABLENAME = '" + testedTableName + "' LIMIT "+testedIdCounts;
		try {
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceIdList, Database.Netezza1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got " + list.size() +" performanceids(Individual MODE), TSTYPE = "+ TsType + ",time cost: "+endTime+" ms");
		return list;
	}
	
//	检测Netezza到TsDB更新后的数据量是否保持一致
	public static void compareUpdatedData(TsBlobDataTypeBaseObject tsType, List<String> perfIdList,int bufferSize) throws ParseException{
 		Map<String,Map<String,List<String>>> netezzaValueMap = new HashMap<String,Map<String,List<String>>>();
 		Map<String,Map<String,List<String>>> tsdbValueMap = new HashMap<String,Map<String,List<String>>>();		
		int i = 0;
		int size = perfIdList.size();
		String strTsDB = "";
		Double doubleTsDB = null;
		Double doubleNetezza = null;
		List<ArrayList<String>> perfIdContainer = new ArrayList<ArrayList<String>>();

//Table DBI_EXTRAPRICE				
		switch(tsType){
		case tsCalculatedTurnOver:
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_ExtraPrice(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_ExtraPrice(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of ExtraPrice between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsCalculatedTurnOver =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsCalculatedTurnOver++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsCalculatedTurnOver*bufferSize,size - countTsCalculatedTurnOver*bufferSize);
				long startTime1 = System.currentTimeMillis();		
				tsdbValueMap = ExtraPriceBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = ExtraPriceBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EXTRAPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsTradeCount:
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_ExtraPrice(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_ExtraPrice(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of ExtraPrice between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsTradeCount =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsTradeCount++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsTradeCount*bufferSize,size - countTsTradeCount*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = ExtraPriceBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = ExtraPriceBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EXTRAPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsExchangeTurnOver:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_ExtraPrice(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_ExtraPrice(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of ExtraPrice between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsExchangeTurnOver =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsExchangeTurnOver++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsExchangeTurnOver*bufferSize,size - countTsExchangeTurnOver*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = ExtraPriceBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = ExtraPriceBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EXTRAPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}			
			break;
			
		case tsBidOfferSpread:
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_ExtraPrice(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_ExtraPrice(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of ExtraPrice between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsBidOfferSpread =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();
				
				countTsBidOfferSpread++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsBidOfferSpread*bufferSize,size - countTsBidOfferSpread*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = ExtraPriceBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = ExtraPriceBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EXTRAPRICE");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			default:
			break;
		}		
	}
	
	@SuppressWarnings("unchecked")
	private static List<ArrayList<String>> getSubList(List<String> perfIdListFull,int bufferSize){
		List<ArrayList<String>> listContainer = new ArrayList<ArrayList<String>>();
		ArrayList<String> list = new ArrayList<String>();
		int flag = 0;
		int buffer = bufferSize;
		int count = 0;
		int size = perfIdListFull.size();
		for(String perfId:perfIdListFull){
			flag++;
			if(flag%buffer !=0){
				list.add(perfId);
			}else{
				list.add(perfId);
				ArrayList<String> sublist = new ArrayList<String>();
				sublist = (ArrayList<String>) list.clone();
				listContainer.add(sublist);
				list.clear();
				flag=0;
				count++;
			}
			if(size-count*buffer>0 && size-count*buffer<buffer){
				for(int j=count*buffer;j<size;j++){
					list.add(perfIdListFull.get(j));
				}
				listContainer.add(list);
				break;
			}
		}
		return listContainer;
	}
	
	private static List<String> getMissingDate(List<String> tsdbDateList,List<String> netezzaDateList) throws ParseException{
		List<String> resultList = new ArrayList<String>();
		List<String> tempList = new ArrayList<String>();
		Date newDate;
		boolean bl = false;
		for(String dateStr:tsdbDateList){
			newDate = df2.parse(dateStr);
			dateStr = df1.format(newDate);
			tempList.add(dateStr);
		}
		bl = netezzaDateList.removeAll(tempList);
		if(bl){
			resultList = netezzaDateList;
		}
		if(!tempList.isEmpty()){
			tempList.clear();
		}
		return resultList;
	}
	
	public static void main(String[] args) throws TSException, Exception{
		List<String> individualPerfIdtsCalculatedTurnOverList = new ArrayList<String>();
		List<String> individualPerfIdtsTradeCountList = new ArrayList<String>();
		List<String> individualPerfIdtsExchangeTurnOverList = new ArrayList<String>();
		List<String> individualPerfIdtsBidOfferSpreadList = new ArrayList<String>();
		
		String tableName = "DBI_EXTRAPRICE";
		String createdOnTime = "2014-06-16";
		int limitCount = 200;
		List<String> performanceIdListEODMode = new ArrayList<String>();

//EOD MODE
		performanceIdListEODMode = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime);
		if(!performanceIdListEODMode.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCalculatedTurnOver,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradeCount,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsExchangeTurnOver,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsBidOfferSpread,performanceIdListEODMode,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,ignore this QA check!");
		}
		
//Individual PerformanceId mode
		individualPerfIdtsCalculatedTurnOverList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsCalculatedTurnOver.TsType,limitCount);
		if(!individualPerfIdtsCalculatedTurnOverList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCalculatedTurnOver, individualPerfIdtsCalculatedTurnOverList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsCalculatedTurnOver list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsTradeCountList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsTradeCount.TsType,limitCount);
		if(!individualPerfIdtsTradeCountList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsTradeCount, individualPerfIdtsTradeCountList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsTradeCount list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsExchangeTurnOverList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsExchangeTurnOver.TsType,limitCount);
		if(!individualPerfIdtsExchangeTurnOverList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsExchangeTurnOver, individualPerfIdtsExchangeTurnOverList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsExchangeTurnOver list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsBidOfferSpreadList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsBidOfferSpread.TsType,limitCount);
		if(!individualPerfIdtsBidOfferSpreadList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsBidOfferSpread, individualPerfIdtsBidOfferSpreadList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsBidOfferSpread list is empty,ignore this QA check!");
		}	
	}
}
