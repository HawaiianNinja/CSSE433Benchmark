import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class Main {

	private static StringSerializer stringSerializer = StringSerializer.get();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CassandraHostConfigurator chost = new CassandraHostConfigurator();
		chost.setHosts("137.112.150.61");
		chost.setPort(9160);
		//chost.setMaxActive(4);
		Cluster cluster = HFactory.getOrCreateCluster("MyCluster", chost);
		Keyspace keyspaceOperator = HFactory.createKeyspace("test",
				cluster);
		try {
			Mutator<String> mutator = HFactory.createMutator(keyspaceOperator,
					StringSerializer.get());
			HFactory.createColumn("password", "1234", StringSerializer.get(),
					StringSerializer.get());
			//mutator.insert("john smith", "users",
			//		HFactory.createStringColumn("first", "Kohn"));
			//ColumnQuery<String, String, String> columnQuery = HFactory
			//		.createStringColumnQuery(keyspaceOperator);
			//QueryResult<HColumn<String, String>> result = columnQuery.execute();
			//System.out.println("Read HColumn from cassandra: " + result.get());
			System.out
					.println("----------------------------------------------");
		} catch (HectorException e) {
			e.printStackTrace();
		}
		cluster.getConnectionManager().shutdown();

	}

}
