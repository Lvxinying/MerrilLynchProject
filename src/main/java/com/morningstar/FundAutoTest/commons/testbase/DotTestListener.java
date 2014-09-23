package com.morningstar.FundAutoTest.commons.testbase;

import org.testng.ITestResult;
import org.testng.TestListenerAdapter;


public class DotTestListener extends TestListenerAdapter {

	private int m_count = 0;
	private int Passed = 0;
	private int failed = 0;
	private int skipped = 0;

	@Override
	public void onTestFailure(ITestResult tr) {

//		log("F");
		log(tr.getMethod().getRealClass().getSimpleName()+"."+tr.getMethod().getMethodName()+"     [Failed]"+'\n');
		failed++;

	}

	@Override
	public void onTestSkipped(ITestResult tr) {

		log(tr.getMethod().getRealClass().getSimpleName()+"."+tr.getMethod().getMethodName()+"     [Skipped]"+'\n');
		skipped++;
	}

	@Override
	public void onTestSuccess(ITestResult tr) {

		log(tr.getMethod().getRealClass().getSimpleName()+"."+tr.getMethod().getMethodName()+"     [Passed]"+'\n');
		Passed++;
		
	}

	private void log(String string) {

		System.out.print(string);

		if (++m_count % 40 == 0) {

			System.out.println("");

		}
		
		
	}
	public int getPassed() {
		
		return this.Passed;
	}

	public int getFailed() {
		return this.failed;
	}

	public int getSkipped() {
		return this.skipped;
	}

}
