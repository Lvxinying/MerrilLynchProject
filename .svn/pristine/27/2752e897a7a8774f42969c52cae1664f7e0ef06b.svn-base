package com.morningstar.Fund.TestRunner;

import org.testng.annotations.Test;
import java.io.File;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.TestNG;

import com.morningstar.FundAutoTest.commons.ResourceManager;
import com.morningstar.FundAutoTest.commons.SendResultByEmail;
import com.morningstar.FundAutoTest.commons.testbase.Base;
import com.morningstar.FundAutoTest.commons.testbase.DotTestListener;


public class RunTestCases {
	private static final Logger logger = LoggerFactory.getLogger(RunTestCases.class);
	private static String round1ResultOutput = "";
	private static String round2ResultOutput = "";
	static DotTestListener listener = new DotTestListener();
	static DotTestListener r2Listener = new DotTestListener();
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	
//Stefan修改，不需要第一个参数选择测试环境，现屏蔽
	public static void main(String[] args) throws Exception {
		if (args == null || args.length <= 1)
//		if (args == null){
			throw new IllegalArgumentException(
					"Please use \"java -jar ... [Dev,QA,Live,UAT] [quickly,normal,fully] [OPTIONAL: severSite]\" to start");
//					"Please use \"java -jar ... [quickly,normal,fully] [OPTIONAL: severSite]\" to start");
		
		new File("./FixIncomeQAcompareresult.txt").deleteOnExit();
		
		if (args.length > 2) 	ResourceManager.serverSite = args[2].trim();

		TestNG ng = new TestNG();
		setOutputDirectoryName(ng, args);
		setTestSuites(ng, args);
		setListener(ng);
		ng.run();
		sendMail("1");

		logger.info("Done");
		System.exit(0);
	}

	private static void round1Run(String[] args) throws Exception{
		//Start to run the normalize process
				TestNG ng = new TestNG();
				setOutputDirectoryName(ng, args);
				setTestSuites(ng, args);
				setListener(ng);
				ng.run();
				sendMail("1");
	}
	private static void round2Run(String[] args){
		TestNG fng = new TestNG();
		setFailOutputDirectoryName(fng, args);
		setFailTestSuites(fng, args);
		setFailListener(fng);
		fng.run();
		sendMail("2");

	}
//Stefan.hou修改
	private static void setOutputDirectoryName(TestNG ng, String[] args) throws Exception{

		if (args.length >= 1) {
			round1ResultOutput = "testNG-round1-output/" + args[0] + "." + args[1];
//			round1ResultOutput = "testNG-round1-output/" + args[0];

			// args[0] should only be QA, UAT, Live
			// args[1] should only be Fully, Daily, Monthly, Yearly
			Base.siteTag = args[0];
			Base.runFrequency = args[1];
		}
		ng.setOutputDirectory(round1ResultOutput);
	}
	
	//fail test cases result will be output here
	private static void setFailOutputDirectoryName(TestNG ng, String[] args) {
	
		if (args.length >= 2) {
			round2ResultOutput = "testNG-round2-output/" + args[0] + "." + args[1];
//			round2ResultOutput = "testNG-round2-output/" + args[0];

			// args[0] should only be QA, UAT, Live
			// args[1] should only be Fully, Daily, Monthly, Yearly
			Base.siteTag = args[0];
			Base.runFrequency = args[1];			
		}

		ng.setOutputDirectory(round2ResultOutput);
	}

	private static void setFailListener(TestNG ng) {
		ng.addListener(r2Listener);
	}

	private static void setTestSuites(TestNG ng, String[] args) {
		List<String> suites = new ArrayList<String>();
		String testngXML;
//		if (args[0].equalsIgnoreCase("fully") || args[1].equalsIgnoreCase("monthly"))
		if (args[0].equalsIgnoreCase("fully"))	
		{
			testngXML = ResourceManager.getSlowTestngXML();
		}		
//		else if (args[0].equalsIgnoreCase("quickly") || args[1].equalsIgnoreCase("release"))
		else if (args[0].equalsIgnoreCase("quickly"))
		{
			testngXML = ResourceManager.getQuickTestngXML();
		}
		else
		{
			testngXML = ResourceManager.getNormalTestXML();
		}
		if (testngXMLExists(testngXML))
			suites.add(testngXML);
		else {
			logger.error("Can not find " + testngXML);
			System.exit(0);
		}
		ng.setTestSuites(suites);
	}
	
	//set fail test xml to suite for running again
	private static void setFailTestSuites(TestNG ng, String[] args) {
		List<String> suites = new ArrayList<String>();

		String failXMLPath = round1ResultOutput + "/testng-failed.xml";

		if (failedXMLPathExists(failXMLPath))
			suites.add(failXMLPath);
		else {
			logger.error("Can not find " + failXMLPath);
			System.exit(0);
		}
		ng.setTestSuites(suites);
	}
	private static void setListener(TestNG ng) {
		ng.addListener(listener);
	}

	@Test(enabled = false)
	private static boolean testngXMLExists(String testngXML) {
		return new File(testngXML).exists();
	}

	private static boolean failedXMLPathExists(String failedXMLPath) {
		return new File(failedXMLPath).exists();
	}

	private static void sendMail(String round) {

		SendResultByEmail email = new SendResultByEmail();
		if(round.equals("1"))
			email.sendMail(listener, listener,"1",round1ResultOutput);
		if(round.equals("2"))
			email.sendMail(listener, r2Listener,"2",round2ResultOutput);
	}

}
