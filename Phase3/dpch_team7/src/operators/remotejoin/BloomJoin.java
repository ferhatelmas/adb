package operators.remotejoin;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import database.IcDataSrvConnectionFactory;

public class BloomJoin {

  static final boolean VERBOSE = false;

  public static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
  public static final long TIMEOUT = 10; // in TIMEOUT_UNIT

  public static String getHashFunctionSQL(String local_col) {
    return " (( " + local_col + " *107 + 1009) MOD " + hashlen + ") ";
  }

  // -- some other primes: 11447; 31567; 99881
  public static int hashlen = 99881;

  public static void run(IcDataSrvConnectionFactory icDataSrvConnectionFactory) {
    // Connection conn = icDataSrvConnectionFactory.getConnectionAtIndex(1);

    // TODO: the connection variables are not initialized properly otherwise
    Connection conn = icDataSrvConnectionFactory.getAllConnections().get(0);

    // missing joins: "s_suppkey = l_suppkey"
    String tables_and_filters = "lineitem,  orders,customer, nation n2 where "
        + " o_orderkey = l_orderkey " + " and c_custkey = o_custkey "
        + " AND c_nationkey = n2.n_nationkey"
        + " AND (n2.n_name = 'GERMANY' OR n2.n_name = 'FRANCE')"
        + " AND l_shipdate between date '1995-01-01' and date '1996-12-31'";

    // System.out.println(table_and_filters);

    String query = getBitVector_SQL("L_SUPPKEY", tables_and_filters, false);
    System.out.println(query);

    // TODO: does that do what I expect?

    try {
      Statement stmt = (Statement) conn.createStatement();

      // this is very important!
      stmt.execute("SET session group_concat_max_len=" + (hashlen + 1));

      // create bit_vector
      stmt = (Statement) conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);

      Blob bit_vector = null;

      if (rs.next()) {
        bit_vector = rs.getBlob("compressed_bitvector");
        System.out.println(bit_vector.length());
      }

      // TODO: this to be executed on another machine
      String bloomFilterSql = bloomFilterSQL(
          "SELECT * FROM supplier JOIN nation N1 ON (S_NATIONKEY = N_NATIONKEY) ",
          "N_NAME = 'FRANCE' OR N_NAME = 'GERMANY'", "S_SUPPKEY", false);
      System.out.println(bloomFilterSql);

      PreparedStatement prepStmt = conn.prepareStatement(bloomFilterSql);
      prepStmt.setBlob(1, bit_vector);
      ResultSet rs1 = prepStmt.executeQuery();

      int i = 0;
      while (rs1.next()) {
        System.out.println("row returned:" + rs1.getString(1));
      }

    } catch (SQLException e) {
      e.printStackTrace();

    } finally {
      try {
        conn.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    System.out.println("Disconnected from database");

  }

  /**
   * Create query returning tuples that pass bloom-filter Given tables,
   * selections (where clause) use PreparedStatement.setBlob(parameterIndex,
   * Blob bit_vector) to set the compressed bit_vector
   * 
   * @param tables
   * @param selections
   * @param local_column
   * @return
   */
  public static String bloomFilterSQL(String tables, String selections, String local_column,
      boolean compressed) {

    StringBuilder b = new StringBuilder();
    b.append(tables);

    if (!"".equals(selections)) {
      b.append(" WHERE (" + selections + ") AND ");
    } else {
      b.append(" WHERE ");
    }

    // TODO: create debuging function that uncompresses and presents results

    // TODO: Uncompress ?
    if (compressed)
      b.append(getHashFunctionSQL(local_column) + "IN ( select hash from ( "
          + "SELECT SUBSTRING(s, number, 1) AS bit, number as hash FROM all_numbers,"
          + " (SELECT UNCOMPRESS(?) AS s) s1 WHERE  number < char_length(s) "
          + ") as bit_vector where bit='1' )");
    else {
      b.append(getHashFunctionSQL(local_column) + "IN ( select hash from ( "
          + "SELECT SUBSTRING(s, number, 1) AS bit, number as hash FROM all_numbers,"
          + " (SELECT ? AS s) s1 WHERE  number < char_length(s) "
          + ") as bit_vector where bit='1' )");
    }
    return b.toString();
  }

  /**
   * @param local_column
   * @param table_and_filters
   * @return
   */
  public static String getBitVector_SQL(String local_column, String table_and_filters,
      boolean compressed) {

    String local_query = "SELECT DISTINCT " + getHashFunctionSQL(local_column)
        + "AS hash, 1 AS active FROM " + table_and_filters;

    String query;
    // TODO: compress
    if (compressed)
      query = "select COMPRESS(bit_vector) as  compressed_bitvector FROM ( "
          + "SELECT group_concat(active order by hash asc SEPARATOR '') as bit_vector FROM ("
          + "        select MAX(active) AS active, hash FROM   ("
          // -- do selection on local relationship on the bloom-join column
          + "             select hash, active from (" + local_query + ") as hashes "
          + "        Union "
          + "             select number as hash, 0 as active from all_numbers where number < "
          + hashlen + ") as hashitems  group by hash ) as b) as c;";
    else
      query = "select bit_vector as  compressed_bitvector FROM ( "
          + "SELECT group_concat(active order by hash asc SEPARATOR '') as bit_vector FROM ("
          + "        select MAX(active) AS active, hash FROM   ("
          // -- do selection on local relationship on the bloom-join column
          + "             select hash, active from (" + local_query + ") as hashes "
          + "        Union "
          + "             select number as hash, 0 as active from all_numbers where number < "
          + hashlen + ") as hashitems  group by hash ) as b) as c;";

    return query;
  }

  /**
   * @param conn
   * @param tables_and_filters
   * @param filterColumn
   * @return
   * @throws SQLException
   */
  public static String getBitVector(Connection conn, String tables_and_filters, String filterColumn)
      throws SQLException {

    String query = getBitVector_SQL(filterColumn, tables_and_filters, false);
    if (VERBOSE) {
      System.out.println(query);
    }

    // TODO: does that do what you expect?
    Statement stmt = (Statement) conn.createStatement();

    // this is very important!
    stmt.execute("SET session group_concat_max_len=" + (hashlen + 1));

    // create bit_vector
    stmt = (Statement) conn.createStatement();
    ResultSet rs = stmt.executeQuery(query);

    String bit_vector = null;

    if (rs.next()) {
      bit_vector = rs.getString("compressed_bitvector");
      if (VERBOSE) {
        System.out.println(bit_vector.length());
      }
      return bit_vector;
    }
    return null;
  }

  public static Blob getBitVector_binary(Connection conn, String tables_and_filters,
      String filterColumn) throws SQLException {

    String query = getBitVector_SQL(filterColumn, tables_and_filters, true);
    if (VERBOSE) {
      System.out.println(query);
    }

    // TODO: does that do what you expect?
    Statement stmt = (Statement) conn.createStatement();

    // this is very important!
    stmt.execute("SET session group_concat_max_len=" + (hashlen + 1));

    // create bit_vector
    stmt = (Statement) conn.createStatement();
    ResultSet rs = stmt.executeQuery(query);

    Blob bit_vector = null;

    if (rs.next()) {
      bit_vector = rs.getBlob("compressed_bitvector");
      if (VERBOSE) {
        System.out.println(bit_vector.length());
      }
      return bit_vector;
    }
    return null;
  }

  /**
   * @param connection
   * @param bitvector
   * @param tables
   * @param selections
   * @param column
   * @return
   * @throws SQLException
   */
  public static ResultSet getBloomFilteredResult(Connection connection, String bitvector,
      String tables, String selections, String column) throws SQLException {

    String bloomFilterSql = bloomFilterSQL(tables, selections, column, false);
    if (VERBOSE) {
      System.out.println(bloomFilterSql);
    }

    PreparedStatement prepStmt = connection.prepareStatement(bloomFilterSql);
    prepStmt.setString(1, bitvector);
    ResultSet rs1 = prepStmt.executeQuery();

    while (rs1.next()) {
      if (VERBOSE) {
        System.out.println("row returned: " + rs1.getString(1));
      }
    }
    rs1.beforeFirst();
    return rs1;
  }

  public static ResultSet getBloomFilteredResult_binary(Connection connection, Blob bitvector,
      String tables, String selections, String column) throws SQLException {

    String bloomFilterSql = bloomFilterSQL(tables, selections, column, true);
    if (VERBOSE) {
      System.out.println(bloomFilterSql);
    }

    PreparedStatement prepStmt = connection.prepareStatement(bloomFilterSql);
    prepStmt.setBlob(1, bitvector);
    ResultSet rs1 = prepStmt.executeQuery();

    while (rs1.next()) {
      if (VERBOSE) {
        System.out.println("row returned: " + rs1.getString(1));
      }
    }
    rs1.beforeFirst();
    return rs1;
  }

  /**
   * @param connections
   * @param tableAndFilters
   * @param filterColumn
   * @return
   * @throws InterruptedException
   */
  public static String getBitVector(List<Connection> connections, final String tableAndFilters,
      final String filterColumn) throws InterruptedException {
    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());

    final Vector<String> bitvectors = new Vector<String>(); // threadSafe
    for (final Connection connection : connections) {

      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            String bitvector = BloomJoin.getBitVector(connection, tableAndFilters, filterColumn);
            bitvectors.add(bitvector);
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      });
    }

    taskExecutor.shutdown();
    taskExecutor.awaitTermination(TIMEOUT, TIMEOUT_UNIT);

    return getORedBitVectors(bitvectors);
  }

  /**
   * @param connections
   * @param bitvector
   * @param selectClause
   * @param whereClause
   * @param column
   * @return
   * @throws InterruptedException
   */
  public static Vector<ResultSet> getBloomFilteredResults(List<Connection> connections,
      final String bitvector, final String selectClause, final String whereClause,
      final String column) throws InterruptedException {

    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());
    final Vector<ResultSet> results = new Vector<ResultSet>(); // threadSafe
    for (final Connection connection : connections) {
      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            ResultSet resultSet = BloomJoin.getBloomFilteredResult(connection, bitvector,
                selectClause, whereClause, column);

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

  /**
   * @param bitvectors
   * @return
   */
  private static String getORedBitVectors(Vector<String> bitvectors) {
    StringBuilder bitvector = new StringBuilder(hashlen);
    // OR the bitvectors

    for (int i = 0; i < bitvectors.get(0).length(); ++i) {
      boolean isCurrentBitSet = false;
      for (String bitString : bitvectors) {
        if (bitString.charAt(i) == '1') {
          isCurrentBitSet = true;
          break;
        }
      }
      bitvector.append(isCurrentBitSet ? "1" : "0");
    }
    return bitvector.toString();
  }
}
