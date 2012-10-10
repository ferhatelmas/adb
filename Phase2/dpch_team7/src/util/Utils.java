package util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mysql.jdbc.Connection;

public class Utils {

	static final boolean VERBOSE = false;
	private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
	private static final long TIMEOUT = 20; // in TIMEOUT_UNIT

	public static Vector<ResultSet> runDistributedQuery(List<Connection> connections, final String query, final List<Object> parameters)
			throws InterruptedException {
		ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());
		final Vector<ResultSet> results = new Vector<ResultSet>(); // threadSafe
		for (final Connection connection : connections) {
			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						ResultSet resultSet = Utils.runQuery(connection, query, parameters);
						results.add(resultSet);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			});
		}
		taskExecutor.shutdown();
		taskExecutor.awaitTermination(TIMEOUT, TIMEOUT_UNIT);
		return results;
	}

	public static ResultSet runQuery(Connection connection, String query, List<Object> parameters) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(query);
		int i = 1;
		for (Object parameter : parameters) {
			if (parameter instanceof Integer) {
				statement.setInt(i, (Integer) parameter);
			} else {
				statement.setString(i, (String) parameter);
			}
			i++;
		}

		if (VERBOSE) {
			System.out.println("Executing: " + statement.toString());
		}

		return statement.executeQuery();
	}
}
