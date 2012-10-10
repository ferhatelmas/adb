package query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import operators.remotejoin.BloomJoin;
import util.Utils;

import com.mysql.jdbc.Connection;

import database.IcDataSrvConnectionFactory;
import database.SqlConnectionException;

/**
 * Class for executing the following query: select o_year, sum(case when nation = '[NATION]' then volume else 0 end) /
 * sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as
 * volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where
 * p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey
 * = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = '[REGION]' and s_nationkey = n2.n_nationkey and
 * o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = '[TYPE]' ) as all_nations group by o_year
 * order by o_year;
 */
public class Q8 {

	String nation;
	String region;
	String type;
	int year1 = 1995;
	int year2 = 1996;

	static final boolean VERBOSE = false;

	IcDataSrvConnectionFactory connectionFactory;
	private ResultSet nationResults;
	private HashMap<Integer, String> nationHashMap = new HashMap<Integer, String>();
	private Integer n2key;

	public Q8() throws SqlConnectionException {
		super();
		nation = "BRAZIL";
		region = "AMERICA";
		type = "ECONOMY ANODIZED STEEL";
		connectionFactory = new IcDataSrvConnectionFactory();
	}

	public Q8(IcDataSrvConnectionFactory connectionFactory) throws SqlConnectionException {
		super();
		nation = "BRAZIL";
		region = "AMERICA";
		type = "ECONOMY ANODIZED STEEL";
		this.connectionFactory = connectionFactory;
	}

	public Q8(String nation, String region, String type) throws SqlConnectionException {
		super();
		this.nation = nation;
		this.region = region;
		this.type = type;
		connectionFactory = new IcDataSrvConnectionFactory();
	}

	/**
	 * select (o_year, SUM(volume if nation=?) ){ select ( o_year, l_volume, n2.n_name as nation) { [SQL2:
	 * filter_p_type(Part) ] <bloom-join> [SQL1: LineItem |><| filter_date(Order) |><| Customer |><| Nation N1 |><|
	 * filter_r_name (Region) ] <bloom-join on l_supplier> [SQL3: Nation N2 |><| Supplier ] }
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	public void run() throws SQLException, InterruptedException {
		long start;
		final List<Connection> connections = connectionFactory.getAllConnections();

		// Begin Nation N1 |><| filter_r_name (Region)
		String n1Keys = getN1Keys(connections);

		getNation2Key(connections.get(connectionFactory.getNationAndRegionServerIndex()));

		// LineItem |><| filter_date(Order) |><| Customer |><| IntdN1Region and join with Part and n2supplier
		Vector<ResultSet> results = locJoin(connections, n1Keys);

		doAggregationsAndPrintResult(results);
	}

	private void doAggregationsAndPrintResult(Vector<ResultSet> resultSets) throws SQLException {

		double sumYear1Nation = 0.0, sumYear2Nation = 0.0;
		double sumYear1AllNations = 0.0, sumYear2AllNations = 0.0;
		for (ResultSet rs : resultSets) {
			rs.beforeFirst();
			while (rs.next()) {
				int year = rs.getInt(1);
				if (year == year1) {
					sumYear1Nation += rs.getDouble(2);
					sumYear1AllNations += rs.getDouble(3);
				} else {
					sumYear2Nation += rs.getDouble(2);
					sumYear2AllNations += rs.getDouble(3);
				}
			}
		}

		System.out.println("YEAR|MKT_SHARE");
		System.out.println(year1 + "|" + sumYear1Nation / sumYear1AllNations);
		System.out.println(year2 + "|" + sumYear2Nation / sumYear2AllNations);
	}

	private Vector<ResultSet> locJoin(final List<Connection> connections, String n1Keys) throws InterruptedException, SQLException {
		// Get bitvector for LineItem |><| filter_date(Order) |><| Customer |><| IntdN1Region (|><| Part)
		final String bigQueryTableAndFilter =
				"lineitem, orders, customer WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND o_orderdate between date '1995-01-01' and date '1996-12-31' AND c_nationkey IN "
						+ n1Keys;
		ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());

		// Get partkeys and insert into temporary tables
		taskExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					String bitvectorForPart = BloomJoin.getBitVector(connections, bigQueryTableAndFilter, "l_partkey");

					// Get Part keys using bit vector
					String sql1SelectClause = "SELECT p_partkey FROM part";
					String sql1WhereClause = "p_type ='" + type + "'";
					String sql1Column = "p_partkey";
					Vector<ResultSet> partKeysResultSets;

					partKeysResultSets = BloomJoin.getBloomFilteredResults(connections, bitvectorForPart, sql1SelectClause, sql1WhereClause, sql1Column);

					// Insert partkeys in temporary table
					final String temporaryTableQuery = "create temporary table part_temp (p_partkey integer);";
                    final String temporaryTablePartIndexing = "CREATE INDEX temp_part_idx ON part_temp (p_partkey);";
                    final String[] indexingQueries = new String[1];
                    indexingQueries[0] = temporaryTablePartIndexing;

                    final String insertIntoTemporaryTableQuery = "insert into part_temp values (?);";

                    createTemporaryTable(connections, temporaryTableQuery, indexingQueries,
                            insertIntoTemporaryTableQuery, partKeysResultSets, 1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});

		// Get suppkeys and nationkeys and insert into temporary tables
		taskExecutor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					String bitvectorForSupp = BloomJoin.getBitVector(connections, bigQueryTableAndFilter, "l_suppkey");

					// Get Suppkeys and nationkeys using bit vector
					String sql1SelectClause = "SELECT s_suppkey, s_nationkey FROM supplier";
					String sql1WhereClause = "1=1";
					String sql1Column = "s_suppkey";
					Vector<ResultSet> suppKeysResultSets;

					suppKeysResultSets = BloomJoin.getBloomFilteredResults(connections, bitvectorForSupp, sql1SelectClause, sql1WhereClause, sql1Column);
					// Insert suppkeys and nationkeys in temporary table
					final String temporaryTableQuery = "create temporary table supplier_temp (s_suppkey integer, s_nationkey integer);";
                    final String temporaryTableSuppIndexing = "CREATE INDEX temp_supp_idx ON supplier_temp (s_suppkey);";
                    final String temporaryTableNationIndexing = "CREATE INDEX temp_nation_idx ON supplier_temp (s_nationkey);";
                    final String[] indexingQueries = new String[2];
                    indexingQueries[0] = temporaryTableSuppIndexing;
                    indexingQueries[1] = temporaryTableNationIndexing;
					final String insertIntoTemporaryTableQuery = "insert into supplier_temp values (?, ?);";
                    createTemporaryTable(connections, temporaryTableQuery, indexingQueries,
                            insertIntoTemporaryTableQuery, suppKeysResultSets, 2);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		});

		taskExecutor.shutdown();
		taskExecutor.awaitTermination(20, TimeUnit.MINUTES);

		// Get the big query result
		String joinQuery =
				"SELECT o_year, SUM(case when nationkey=? then volume else 0 end) AS nationvolume, SUM(volume) AS allnationvolume FROM "
						+ " ( SELECT extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, s.s_nationkey as nationkey "
						+ " FROM lineitem, orders, customer, supplier_temp s, part_temp p "
						+ " WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND o_orderdate between date '1995-01-01' and date '1996-12-31' "
						+ " AND s.s_suppkey = l_suppkey AND p.p_partkey = l_partkey AND c_nationkey IN " + n1Keys + ") as allnations "
						+ "  GROUP BY o_year ORDER BY o_year;";
		List<Object> parameters = new ArrayList<Object>();
		parameters.add(n2key);
		Vector<ResultSet> results = Utils.runDistributedQuery(connections, joinQuery, parameters);
		if (VERBOSE) {
			for (ResultSet rs : results) {
				rs.beforeFirst();
				while (rs.next()) {
					System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
				}
			}
		}
		return results;

	}

	protected void createTemporaryTable(List<Connection> connections, final String temporaryTableQuery, final String[] indexingQueries,
            final String insertIntoTemporaryTableQuery, final Vector<ResultSet> resultSets, final int numberOfParameters)
            throws InterruptedException, SQLException {

		ExecutorService taskExecutor = Executors.newFixedThreadPool(connectionFactory.getAllConnections().size());
		if (VERBOSE) {
			System.out.println(insertIntoTemporaryTableQuery + " NParameters = " + numberOfParameters);
		}
		final Vector<Integer> vector = new Vector<Integer>();
		//StringBuilder sb = new StringBuilder(insertIntoTemporaryTableQuery.substring(0, insertIntoTemporaryTableQuery.length()-(numberOfParameters==1 ?  4 : 7)));
        for (ResultSet rs : resultSets) {
			rs.beforeFirst();
			while (rs.next()) {
				for (int i = 1; i <= numberOfParameters; ++i) {
					vector.add(rs.getInt(i));
				}
                //if(numberOfParameters == 1) sb.append("(?),");
                //else sb.append("(?, ?),");
			}
		}
        //final String insertQuery = sb.substring(0, sb.length()-1) + ";";

		for (final Connection connection : connectionFactory.getAllConnections()) {
			taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						connection.createStatement().execute(temporaryTableQuery);
						PreparedStatement stmt = connection.prepareStatement(insertIntoTemporaryTableQuery);
						int i = 0;
                        while (i < vector.size()) {
                            for (int j = 1; j <= numberOfParameters && i < vector.size(); ++j, ++i) {
                                stmt.setInt(j, vector.get(i));
                            }
                            stmt.executeUpdate();
                        }
                        /*
                        while (i < vector.size()) {
                            stmt.setInt(i+1, vector.get(i));
                            i++;
						}
                        stmt.executeUpdate();
                        */
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			});

		}
		taskExecutor.shutdown();
		taskExecutor.awaitTermination(20, TimeUnit.MINUTES);
		if (VERBOSE) {
			System.out.println("Done inserting: " + insertIntoTemporaryTableQuery);
		}
	}

	/**
	 * @param connections
	 * @return
	 * @throws SQLException
	 */
	private String getN1Keys(List<Connection> connections) throws SQLException {
		String joinNationRegionQuery = "SELECT N1.N_NATIONKEY AS N1Key FROM nation N1, region R WHERE N1.N_REGIONKEY = R.R_REGIONKEY AND R.R_NAME = ?;";
		List<Object> parameters = new ArrayList<Object>();
		parameters.add(region);

		ResultSet IntdN1Region = Utils.runQuery(connections.get(connectionFactory.getNationAndRegionServerIndex()), joinNationRegionQuery, parameters);// statement.executeQuery(joinNationRegionQuery);

		String n1Keys = "(";
		while (IntdN1Region.next()) {
			n1Keys += "'" + IntdN1Region.getString(1) + "',";
			if (VERBOSE) {
				System.out.println(IntdN1Region.getString(1));
			}
		}
		n1Keys = n1Keys.substring(0, n1Keys.length() - 1) + ")"; // trim the trailing ','

		if (VERBOSE) {
			System.out.println("N1 keys: " + n1Keys);
		}
		return n1Keys;
	}

	private void getNation2Key(Connection connection) throws SQLException {
		PreparedStatement ps = connection.prepareStatement("SELECT n_nationkey FROM nation WHERE n_name=?");
		ps.setString(1, nation);
		nationResults = ps.executeQuery();
		while (nationResults.next()) {
			n2key = nationResults.getInt(1);
		}
	}
}
