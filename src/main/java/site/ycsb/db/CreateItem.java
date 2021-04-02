package site.ycsb.db;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import site.ycsb.db.model.TimeSeries;

public class CreateItem extends DynamoDBSample {

	public void run() {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();

        Date date = Calendar.getInstance().getTime();  
        String strDate = sdf.format(date);  

		createTimeSeries(randomUUIDString, strDate, strDate);
		createTimeSeries(randomUUIDString, strDate, strDate);
		createTimeSeries(randomUUIDString, strDate, strDate);
		createTimeSeries(randomUUIDString, strDate, strDate);
	}

	private void createTimeSeries(String TimeSeriesKey, String ValidTime, String TransactionTime) {
		System.out.println("Creating TimeSeries!");
		TimeSeries timeseries = new TimeSeries(TimeSeriesKey, ValidTime, TransactionTime);
		mapper.save(timeseries);
		System.out.println("TimeSeries created!");
	}

	public static void main(String[] args) {
		new CreateItem().run();
	}

}