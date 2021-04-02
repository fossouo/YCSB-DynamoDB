package site.ycsb.db.model;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "TimeSeries")
public class TimeSeries {

	private String TimeSeriesKey;
	private String ValidTime;
	private String TransactionTime;
	/* private String Blobkey;
    private String ReservedPayload;
    private */ 

	public TimeSeries() {}

	public TimeSeries(String TimeSeriesKey) {
		this();
		this.TimeSeriesKey = TimeSeriesKey;
	}

	public TimeSeries(String TimeSeriesKey, String ValidTime) {
		this(TimeSeriesKey);
		this.ValidTime = ValidTime;
	}

	public TimeSeries(String TimeSeriesKey, String ValidTime, String TransactionTime) {
		this(TimeSeriesKey, ValidTime);
		this.TransactionTime = TransactionTime;
		/* this.Blobkey = Blobkey;
        this.ReservedPayload = ReservedPayload; */
	}

	@DynamoDBHashKey(attributeName = "TimeSeriesKey")
	public String getId() {
		return TimeSeriesKey;
	}

	public void setId(String TimeSeriesKey) {
		this.TimeSeriesKey = TimeSeriesKey;
	}

	@DynamoDBRangeKey(attributeName = "ValidTime")
	public String getName() {
		return ValidTime;
	}

	public void setValidTime(String ValidTime) {
		this.ValidTime = ValidTime;
	}

    public void setTransacTime(String TransactionTime) {
		this.TransactionTime = TransactionTime;
	}

	/* public void setBlobkey(String Blobkey) {
		this.Blobkey = Blobkey;
	}

    public void setReservedPayload(String ReservedPayload) {
		this.ReservedPayload = ReservedPayload;  
	} */

	@DynamoDBIndexRangeKey(attributeName = "TransactionTime", localSecondaryIndexName = "TransactionTime_index")
	public String getTransactionTime() {
		return TransactionTime;
	}

	public void setTransactionTime(String TransactionTime) {
		this.TransactionTime = TransactionTime;
	}

}
