package MarketPriceBackFeedToTSDB;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class MultyThread implements Runnable{
	private String className = null;
	private TsBlobDataTypeBaseObject tsType = null;
	private List<String> perfIdList = new ArrayList<String>();
	private int bufferSize;

	public MultyThread(String className,TsBlobDataTypeBaseObject tsType,List<String> perfIdList,int bufferSize){		
		if(this.className == null){
			this.className = className;
		}					
		if(this.tsType == null){
			this.tsType = tsType;
		}
		if(this.perfIdList != null){
			this.perfIdList = perfIdList;
		}
		this.bufferSize = bufferSize;
	}
	
	@Override
	public void run() {	
		switch(this.className){
		case "MarketPriceForEquityAndETFBaseObject":
			try {
				MarketPriceForEquityAndETFBaseObject obj = new MarketPriceForEquityAndETFBaseObject();
				obj.compareUpdatedData(this.tsType, this.perfIdList, this.bufferSize);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			break;
		default:
			break;
		}		
	}
}
