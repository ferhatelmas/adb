package query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import operators.remotejoin.BloomJoin;
import util.Utils;

import com.mysql.jdbc.Connection;

import database.IcDataSrvConnectionFactory;
import database.SqlConnectionException;

public class Q7 {

	private IcDataSrvConnectionFactory connectionFactory;
	private String nation1;
	private String nation2;
	private int nation1Key;
    private int nation2Key;

	public Q7(IcDataSrvConnectionFactory connectionFactory, String nation1, String nation2) {
		this.connectionFactory = connectionFactory;
		this.nation1 = nation1;
		this.nation2 = nation2;
	}

	public Q7(String nation1, String nation2) throws SqlConnectionException {
		this.connectionFactory = new IcDataSrvConnectionFactory();
		this.nation1 = nation1;
		this.nation2 = nation2;
	}

	public void run() throws SQLException, InterruptedException {

		//double start;

		//start = System.currentTimeMillis();
		String nationKeys = getNationKeys();
		//System.out.println("GetNationKeys: " + (System.currentTimeMillis() - start));

		String locBitVectorQuery =
				"lineitem, orders, customer where o_orderkey = l_orderkey and c_custkey = o_custkey "
						+ "and l_shipdate between date '1995-01-01' and date '1996-12-31' and c_nationkey IN " + nationKeys;
		String locFilterColumn = "l_suppkey";

		//start = System.currentTimeMillis();
		String supplierBitVector = BloomJoin.getBitVector(connectionFactory.getAllConnections(), locBitVectorQuery, locFilterColumn);
		//System.out.println("LOCBitVector: " + (System.currentTimeMillis() - start));

		String supplierQuerySelect = "SELECT s_suppkey, s_nationkey FROM supplier";
		String supplierQueryWhere = "s_nationkey IN " + nationKeys;
		String supplierQueryColumn = "s_suppkey";

		//start = System.currentTimeMillis();
		Vector<ResultSet> resultSets =
				BloomJoin.getBloomFilteredResults(connectionFactory.getAllConnections(), supplierBitVector, supplierQuerySelect, supplierQueryWhere,
						supplierQueryColumn);
		//System.out.println("SupplierFilteredByBitVector: " + (System.currentTimeMillis() - start));

		//start = System.currentTimeMillis();
		resultSets = getLocJoin(resultSets, nationKeys);
		//System.out.println("LocJoin: " + (System.currentTimeMillis() - start));

		//start = System.currentTimeMillis();
		doAggregationsAndPrintResult(resultSets);
		//System.out.println("Aggregates: " + (System.currentTimeMillis() - start));
	}

	private void doAggregationsAndPrintResult(Vector<ResultSet> resultSets) throws SQLException {

		TreeMap<String, Double> result = new TreeMap<String, Double>();
		for (ResultSet rs : resultSets) {
			while (rs.next()) {
				String key = nation1Key == rs.getInt(1) ? nation1 + "|" + nation2 + "|" : nation2 + "|" + nation1 + "|";
				key += rs.getString(3);
				if (result.containsKey(key)) {
					double sum = result.remove(key);
					sum += rs.getDouble(4);
					result.put(key, sum);
				} else {
					result.put(key, rs.getDouble(4));
				}
			}
		}

		System.out.println("supp_nation|cust_nation|l_year|revenue");
		for (String key : result.keySet()) {
			System.out.println(key + "|" + result.get(key));
		}
	}

	private Vector<ResultSet> getLocJoin(final Vector<ResultSet> values, String nationKeys) throws SQLException, InterruptedException {
		ExecutorService taskExecutor = Executors.newFixedThreadPool(connectionFactory.getAllConnections().size());

        final String temporaryTableSuppIndexing = "CREATE INDEX temp_supp_idx ON supplier_temp (s_suppkey);";
        final String temporaryTableNationIndexing = "CREATE INDEX temp_nation_idx ON supplier_temp (s_nationkey);";
		final String temporaryTableQuery = "create temporary table supplier_temp (s_suppkey integer, s_nationkey integer);";
		final String insertIntoTemporaryTableQuery = "insert into supplier_temp values (?, ?);";
        final String joinQuery =
                "SELECT s_nationkey, c_nationkey, extract(year from l_shipdate) as l_year, sum(l_extendedprice * (1-l_discount)) as volume "
                + "FROM lineitem, orders, customer, supplier_temp "
                + "WHERE s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND l_shipdate between date '1995-01-01' "
                + "AND date '1996-12-31' AND ((c_nationkey = " + nation2Key + " AND s_nationkey = " + nation1Key + ") OR (c_nationkey = "
                + nation1Key + " AND s_nationkey = " + nation2Key + ")) GROUP BY s_nationkey, c_nationkey, l_year;";

		final Vector<ResultSet> resultSets = new Vector<ResultSet>(); // threadSafe
		final Vector<Integer> vector = new Vector<Integer>();
		StringBuilder sb = new StringBuilder(insertIntoTemporaryTableQuery.substring(0, insertIntoTemporaryTableQuery.length()-7));
		for (ResultSet rs : values) {
			rs.beforeFirst();
			while (rs.next()) {
				vector.add(rs.getInt(1));
				vector.add(rs.getInt(2));
				sb.append("(?, ?),");
			}
		}
		final String insertQuery = sb.substring(0, sb.length()-1) + ";";

		for (final Connection connection : connectionFactory.getAllConnections()) {

			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						connection.createStatement().executeUpdate(temporaryTableQuery);
                        connection.createStatement().executeUpdate(temporaryTableSuppIndexing);
                        connection.createStatement().executeUpdate(temporaryTableNationIndexing);
						PreparedStatement stmt = connection.prepareStatement(insertQuery);

						/* One by one insert */
						/*
                        for (int i = 0; i < vector.size(); i += 2) {
							stmt.setInt(1, vector.get(i));
							stmt.setInt(2, vector.get(i + 1));
							stmt.executeUpdate();
						}
                        */

						/* Batch insert */
						for (int i=1; i<=vector.size(); i++) { stmt.setInt(i, vector.get(i-1)); }
						stmt.executeUpdate();

						resultSets.add(connection.createStatement().executeQuery(joinQuery));
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			});
		}
		taskExecutor.shutdown();
		taskExecutor.awaitTermination(20, TimeUnit.MINUTES);

		return resultSets;
	}

	private String getNationKeys() throws SQLException {
		String query = "SELECT N.n_nationkey, N.n_name FROM nation N WHERE N.n_name = ? OR N.n_name = ?;";
		List<Object> parameters = new ArrayList<Object>();
		parameters.add(nation1);
		parameters.add(nation2);

		ResultSet rs = Utils.runQuery(connectionFactory.getConnectionAtIndex(connectionFactory.getNationAndRegionServerIndex()), query, parameters);

		String keys = "(";
		while (rs.next()) {
			keys += "'" + rs.getString(1) + "',";
			if (nation1.equals(rs.getString(2))) {
				nation1Key = rs.getInt(1);
			} else {
                nation2Key = rs.getInt(1);
            }
			//System.out.println(rs.getString(1));
		}
		return keys.substring(0, keys.length() - 1) + ")";
	}
}
