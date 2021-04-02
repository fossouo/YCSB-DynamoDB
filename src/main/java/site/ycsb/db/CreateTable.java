package site.ycsb.db;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class CreateTable extends DynamoDBSample {

	public void run() {
		System.out.println("Creating table TimeSeries!");
		CreateTableRequest request = new CreateTableRequest();
		request.setTableName("TimeSeries");

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(5000L);
		provisionedThroughput.setWriteCapacityUnits(5000L);
		request.setProvisionedThroughput(provisionedThroughput);

		List<AttributeDefinition> attributes = new ArrayList<AttributeDefinition>();
		attributes.add(new AttributeDefinition("TimeSeriesKey", ScalarAttributeType.S));
		attributes.add(new AttributeDefinition("ValidTime", ScalarAttributeType.S));
        attributes.add(new AttributeDefinition("TransactionTime", ScalarAttributeType.S));
		request.setAttributeDefinitions(attributes);

		List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
		keySchema.add(new KeySchemaElement("TimeSeriesKey", KeyType.HASH));
		keySchema.add(new KeySchemaElement("ValidTime", KeyType.RANGE));
		request.setKeySchema(keySchema);

		List<KeySchemaElement> indexKeySchema = new ArrayList<KeySchemaElement>();
		indexKeySchema.add(new KeySchemaElement("TimeSeriesKey", KeyType.HASH));
		indexKeySchema.add(new KeySchemaElement("TransactionTime", KeyType.RANGE));
		List<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
		localSecondaryIndexes.add(new LocalSecondaryIndex().withIndexName("TransactionTime_index").withKeySchema(indexKeySchema)
				.withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
		request.setLocalSecondaryIndexes(localSecondaryIndexes);

		createTableWithRetries(request, 10);
		System.out.println("Table TimeSeries created!");

		waitForTableAvailable("TimeSeries", 10);
	}

	private void waitForTableAvailable(String tableName, int retries) {
		System.out.println(String.format("Waiting for table %s to be available!", tableName));
		DescribeTableRequest request = new DescribeTableRequest();
		request.setTableName(tableName);

		for (int i = 1; i < retries; i++) {
			DescribeTableResult result = client.describeTable(request);
			if ("ACTIVE".equals(result.getTable().getTableStatus()))
				return;

			System.out.println(String.format("Table %s not available yet, try %d of %d!", tableName, i, retries));
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				//ignore
			}
		}
	}

	private void createTableWithRetries(CreateTableRequest request, int retries) {
		for (int i = 1; i < retries; i++) {
			try {
				client.createTable(request);
				return;
			} catch (Exception e) {
				System.out.println(String.format("Failed to create table %s, try %d of %d : %s", request.getTableName(), i, retries, e.getMessage()));
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					//ignore
				}
			}
		}
	}

	public static void main(String[] args) {
		new CreateTable().run();
	}

}