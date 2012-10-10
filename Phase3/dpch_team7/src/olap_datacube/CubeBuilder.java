package olap_datacube;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
 * Our main class Comments on some design decisions: - Design for only the first
 * top K rows of the result (unless chart plotting was used more is not possible
 * to process, and usual methods include tables with scrolling which could show
 * only limited number of rows) ---> this allows saving space in the data-cube,
 * allowing to fit more zoom-levels for time dimension Datacube storage: - no
 * strings TODO DataCube partitioning: - the biggest data is then product_id is
 * not NULL, partition this by product_id % node_id - [we do not care anymore
 * about partitioning by customer] - what's left, can be stored on one machine +
 * nation + region We could use FEDERATED engine instead, however it is not
 * installed. Try to save space by selecting appropriate datatypes TODO: shall
 * we try to store the data for different views on separate tables so to improve
 * the performance by having sort of clustered indexes?!! Then I would do this
 * in two steps: - build the huge table with all relations fully joined - from
 * that build the cube
 * 
 * @author vidma ----- [Tran Le Hung] Keeping the same strategy, total running
 *         time on scale factor 0.1 is ~150sec: + The current bottleneck is
 *         create Part temporary table ~110sec (get Bitvector from Lineitem on
 *         partkey and send everywhere to get Partkeys and then send the
 *         retrieved Partkeys to each node to join with Lineitem) + The final
 *         join and datacube only took ~40sec TO CLEAN UP DATABASE: drop table
 *         if exists nation_region_temp; drop table if exists supp_temp;
 */
public class CubeBuilder {

  String nation;
  String region;
  String type;
  int year1 = 1995;
  int year2 = 1996;

  static final boolean VERBOSE = false;
  static final boolean RUN_ALL_TESTS = false;

  static final boolean BUILD_DATACUBE = false;
  private static final boolean PRINT_ALL_STATS = false;
  private static final boolean PRINT_TABLE_STATS = true;

  IcDataSrvConnectionFactory connectionFactory;

  public CubeBuilder() throws SqlConnectionException {
    super();
    if (Utils.RUNNING_LOCALLY) {
      connectionFactory = new IcDataSrvConnectionFactory(true);
    } else {
      connectionFactory = new IcDataSrvConnectionFactory();
    }

  }

  /**
   * Create the data cube, then run some queries on it.
   * 
   * @throws SQLException
   * @throws InterruptedException
   */
  public void run() throws SQLException, InterruptedException {
    final List<Connection> connections = connectionFactory.getAllConnections();

    long start = System.currentTimeMillis();

    /*
     * TODO: Store all temporary results in materialized table, as we'll need
     * this for query answering then the result is not materialized If we can't
     * afford storing part_temp data, we may workaround this by only storing
     * required rows, and deleting afterwards
     */

    // Join Nation and Region
    ResultSet intRegion = getRegion(connections);
    ResultSet intdNationRegion = getNationAndRegion(connections);
    if (VERBOSE) {
      System.out.println("Got copy of Region; Joined Nation and Region");
    }

    // Get Supplier, Nation and Region and insert everywhere
    createSuppLocations(connections, intRegion, intdNationRegion);
    if (VERBOSE) {
      System.out.println("Created permanent copy of Nation and Region everywhere");
    }

    if (VERBOSE) {
      System.out.println("Building the Data Cube...");
    }
    // Join everything and get cube
    buildDataCube(connections);
    if (VERBOSE) {
      System.out.println("Data Cube built");
    }

    long cubeBuiltIn = System.currentTimeMillis() - start;

    if (VERBOSE) {
      System.out.println("Getting statistics...");
    }
    // Get statistics regarding size of the cube
    getStatistics(connections);

    System.out.println("Cube Building was Completed in " + (cubeBuiltIn) + "msecs");

    if (VERBOSE) {
      System.out.println("trying to replicate product titles...");
    }
    CubeParameterSet.saveAllProductNames(connections);

    long allDoneIn = System.currentTimeMillis() - start;

    if (VERBOSE) {
      System.out.println("Getting statistics with product replicated...");
    }
    getStatistics(connections);

    System.out.println("All done including distribution of heavy replicas of product rows in: "
        + (allDoneIn) + "msecs; ");

    /*
     * // OLAP test if (VERBOSE) {
     * System.out.println("Testing materialized views"); } olapTest(connections,
     * false); if (VERBOSE) {
     * System.out.println("Testing NON-materialized views"); }
     * olapTest(connections, true); if (RUN_ALL_TESTS) { if (VERBOSE) {
     * System.out
     * .println("simple OLAP test done. Testing all materialized tables"); } //
     * Test all subcubes (tables) testAllSubCubes(connections); }
     */

  }

  public static void getStatistics(List<Connection> connections) throws InterruptedException,
      SQLException {
    // Get statistics about size and rows in cube

    String cubeTablesSizeQuery = "SELECT TABLE_NAME, table_rows, round(((data_length + index_length) / 1024 / 1024),2) As SizeMB "
        + " FROM information_schema.TABLES "
        + " WHERE table_schema = \"dbcourse1\" AND (TABLE_NAME LIKE 'cube_results%'  OR TABLE_NAME LIKE '%_temp')  "
        + " order by sizeMB desc;";
    Vector<ResultSet> results = Utils.runDistributedQuery(connections, cubeTablesSizeQuery,
        new ArrayList<Object>());

    if (PRINT_ALL_STATS) {
      for (int i = 1; i <= results.get(0).getMetaData().getColumnCount(); ++i) {
        System.out.print(results.get(0).getMetaData().getColumnName(i) + "\t");
      }
      System.out.println();
    }

    // Aggregate sizes
    HashMap<String, Double> tableSizesinMB = new HashMap<String, Double>();
    HashMap<String, Double> tableRowCounts = new HashMap<String, Double>();

    for (ResultSet rs : results) {
      while (rs.next()) {
        if (tableSizesinMB.containsKey(rs.getString(1))) {
          tableSizesinMB
              .put(rs.getString(1), tableSizesinMB.get(rs.getString(1)) + rs.getDouble(3));
          tableRowCounts
              .put(rs.getString(1), tableRowCounts.get(rs.getString(1)) + rs.getDouble(2));
        } else {
          tableSizesinMB.put(rs.getString(1), rs.getDouble(3));
          tableRowCounts.put(rs.getString(1), rs.getDouble(2));
        }
        if (PRINT_ALL_STATS) {
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            System.out.print(rs.getString(i) + "\t");
          }
          System.out.println();
        }
      }
      if (PRINT_ALL_STATS) {
        System.out.println();
      }
    }
    Double totalCubeSize = 0.0;
    Double totalCubeRowCount = 0.0;
    for (String table : tableSizesinMB.keySet()) {
      if (PRINT_TABLE_STATS) {
        System.out.println(table + "\t" + tableRowCounts.get(table) + "\t"
            + tableSizesinMB.get(table));
      }
      totalCubeSize += tableSizesinMB.get(table);
      totalCubeRowCount += tableRowCounts.get(table);
    }

    System.out.println("Total Cube size: " + totalCubeSize + " MB");
    System.out.println("Total Rows in cube: " + totalCubeRowCount);

    // Get statistics about size and rows in initial database
    String initialTableSizeQuery = "SELECT SUM(table_rows), SUM(round(((data_length + index_length) / 1024 / 1024),2)) As SizeMB "
        + " FROM information_schema.TABLES WHERE table_schema = \"dbcourse1\" "
        + " AND NOT (TABLE_NAME LIKE 'cube_results%'  OR TABLE_NAME LIKE '%_temp' OR TABLE_NAME = 'all_numbers') "
        + " order by sizeMB desc ;";

    Double initialTableSize = 0.0;
    Double initialTableRowCount = 0.0;
    results = Utils
        .runDistributedQuery(connections, initialTableSizeQuery, new ArrayList<Object>());
    for (ResultSet rs : results) {
      while (rs.next()) {
        initialTableRowCount += rs.getDouble(1);
        initialTableSize += rs.getDouble(2);
      }
    }

    System.out.println("Total size of initial data: " + initialTableSize + " MB");
    System.out.println("Total Rows in initial data: " + initialTableRowCount);
  }

  /**
   * @param connections
   * @throws InterruptedException
   * @throws SQLException
   */
  private void testAllSubCubes(final List<Connection> connections) throws InterruptedException,
      SQLException {
    for (CubeParameterSet subcube : CubeSchemaMgr.getInstance().getAttributeSuperSet()) {

      if (!subcube.materialized()) {
        if (VERBOSE) {
          System.out.println("not testing non-materialized subCube: " + subcube.getTableName());
        }
        continue;
      }

      String testCount = "SELECT COUNT(*) FROM " + subcube.getTableName();

      Vector<ResultSet> result = Utils.runDistributedQuery(connections, testCount,
          new ArrayList<Object>());
      for (ResultSet r : result) {
        r.beforeFirst();
        while (r.next()) {
          System.out.println(r.getInt(1) + ": " + subcube.getTableName());
        }
      }
    }
  }

  /***
   * TODO: Each of dimensions have multiple zoom-levels, hovever these are not
   * independent as they can not (do not need to ) be combined Dimensions: -
   * product: null, product_id - supplier: null, id TODO: expensive to
   * materialized. do we need it? - sup_location: null, region_id, nation_id (?)
   * -- also may make all too expensive, or we may want to to combine it with
   * everything - customer_location: null, region_id, nation_id - time: null,
   * year, year+month, exact date [ymd] aggregates: o_avgtotalprice,
   * o_totalprice. TODO: this is not valid with product taken into account, the
   * total price would be added multiple times!!!
   * 
   * @param connections
   * @throws InterruptedException
   */
  private void buildDataCube(final List<Connection> connections) throws InterruptedException {

    Set<CubeParameterSet> subcubes = CubeSchemaMgr.getInstance().getAttributeSuperSet();

    List<Query> queries_and_params = new ArrayList<Query>();

    // We will run the queries for dimension combinations in batch, as some may
    // be quick and others slow: first build the list of queries to run
    for (CubeParameterSet dimensionSet : subcubes) {

      // Drop tables if exist
      queries_and_params.add(new Query(dimensionSet.getTableDropSQL()));

      // Create table to store cube results
      String createCubeTable = dimensionSet.getTableCreationSQL();
      if (createCubeTable != null) {
        queries_and_params.add(new Query(createCubeTable));
      }

      // now either create materialized tables for the cube

      String materializeQ = dimensionSet.getDataMaterizationSQL();
      if (materializeQ != null) {
        queries_and_params.add(new Query(materializeQ));
      }
    }

    // Run queries in batch
    Utils.runDistributedMultipleUpdates(connections, queries_and_params);

  }

  /**
   * @param connections
   * @return r_name, r_regionkey
   * @throws SQLException
   */
  private ResultSet getRegion(List<Connection> connections) throws SQLException {
    return Utils.runQuery(connections.get(connectionFactory.getNationAndRegionServerIndex()),
        "SELECT r_name, r_regionkey  FROM region R", new ArrayList<Object>());
  }

  /**
   * @param connections
   * @return n_nationkey, n_name, r_name
   * @throws SQLException
   */
  ResultSet getNationAndRegion(List<Connection> connections) throws SQLException {
    String joinNationRegionQuery = "SELECT n_nationkey, n_name, r_name, r_regionkey "
        + "FROM nation N, region R WHERE N.n_regionkey = R.r_regionkey";
    List<Object> parameters = new ArrayList<Object>();
    ResultSet IntdNationRegion = Utils.runQuery(
        connections.get(connectionFactory.getNationAndRegionServerIndex()), joinNationRegionQuery,
        parameters);// statement.executeQuery(joinNationRegionQuery);
    return IntdNationRegion;
  }

  /**
   * Create supplier locations TODO: we use nation/region IDs instead of names!!
   * This still need clean up to remove not needed columns TODO: these tables
   * are now materialized, as will be used in OLAP (it shall work, but could be
   * rewritten in more clean way)
   * 
   * @param connections
   * @param IntdNationRegion
   * @param intdRegions
   * @throws InterruptedException
   * @throws SQLException
   */
  private void createSuppLocations(List<Connection> connections, ResultSet intdRegions,
      ResultSet IntdNationRegion) throws InterruptedException, SQLException {

    // Clean up if we have old table
    // TODO: if we used view we may want to empty the table, not drop it
    Utils.runDistributedMultipleUpdates(connections, "drop table if exists nation_region_temp;"
        + "drop table if exists region_temp;" + " drop table if exists supp_temp;");

    // create regions table

    // NOTE: if not exists used for local debugging

    Vector<ResultSet> region_resultSets = new Vector<ResultSet>();
    region_resultSets.add(intdRegions);
    createTemporaryTable(
        connections,
        new String[] { "create table if not exists region_temp (r_name CHAR(25), r_regionkey integer);" },
        "insert into region_temp values (?, ?);", region_resultSets, 2);

    // NOTE: if not exists used for local debugging
    final String[] prepTableQueries = new String[] { "create table if not exists "
        + "nation_region_temp (n_nationkey integer, n_name VARCHAR(25),"
        + "r_name CHAR(25), r_regionkey integer);" };

    final String insertIntoTemporaryTableQuery = "insert into nation_region_temp values (?, ?, ?, ?);";

    Vector<ResultSet> resultSets = new Vector<ResultSet>();
    resultSets.add(IntdNationRegion);
    createTemporaryTable(connections, prepTableQueries, insertIntoTemporaryTableQuery, resultSets,
        4);

    // TODO: supplier is not used yet! also it don't quite fit and is not
    // obligatory
    // Create suppliers table
    if (false) {

      // TODO: we are not using this yet!!!
      String suppLocationQuery = "SELECT s_suppkey, s_name, n_nationkey, r_regionkey "
          + "FROM nation_region_temp NR, supplier S WHERE NR.n_nationkey = S.s_nationkey";
      Vector<ResultSet> suppLocations = Utils.runDistributedQuery(connections, suppLocationQuery,
          new ArrayList<Object>());

      // Insert supp locations in temporary table
      final String[] prepTableQueries2 = new String[2];
      prepTableQueries2[0] = "create table if not exists "
          + "supp_temp (s_suppkey integer, s_name CHAR(25), n_nationkey integer, r_regionkey integer);";
      prepTableQueries2[1] = "CREATE INDEX temp_supp_idx ON supp_temp (s_suppkey);";

      final String suppLocationsInsertIntoTemporaryTableQuery = "insert into supp_temp values (?, ?, ?, ?);";

      createTemporaryTable(connections, prepTableQueries2,
          suppLocationsInsertIntoTemporaryTableQuery, suppLocations, 4);
    }

  }

  /**
   * @param connections
   * @throws InterruptedException
   *           we use a real materialized table, so it could be used by VIEWS
   *           later!!
   */
  private void createPartTemp(final List<Connection> connections) throws InterruptedException {
    // Get bitvector for LineItem (|><| Part)
    final String lineItemTableAndFilter = " lineitem ";
    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());

    for (final Connection c : connections) {
      // Get partkeys and insert into temporary tables
      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            String bitvectorForPart = BloomJoin
                .getBitVector(c, lineItemTableAndFilter, "l_partkey");

            // Get Part using bit vector
            // TODO: What is the hierarchy for product? We might not (be able to
            // handle/need) everything
            String partSelectClause = "SELECT p_partkey, p_name, p_type, p_brand FROM part";
            String partWhereClause = " 1=1 ";
            String partColumn = "p_partkey";
            Vector<ResultSet> partResultSets;

            partResultSets = BloomJoin.getBloomFilteredResults(connections, bitvectorForPart,
                partSelectClause, partWhereClause, partColumn);

            // Insert part in temporary table
            final String[] prepTableQueries = new String[2];
            prepTableQueries[0] = "create  table " + "part_temp (p_partkey integer, "
                + "p_name VARCHAR(255), p_type VARCHAR (25), p_brand CHAR(10));";
            prepTableQueries[1] = "CREATE INDEX temp_part_idx ON part_temp (p_partkey);";

            final String insertIntoTemporaryTableQuery = "insert into part_temp values (?, ?, ?, ?);";

            List<Connection> connections = new LinkedList<Connection>();
            connections.add(c);
            createTemporaryTable(connections, prepTableQueries, insertIntoTemporaryTableQuery,
                partResultSets, 4);
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      });
    }
    taskExecutor.shutdown();
    taskExecutor.awaitTermination(20, TimeUnit.MINUTES);
  }

  /**
   * Create a (temporary) table for each connection, inserting all provided
   * elements in each.
   * 
   * @param connections
   *          List of database connections
   * @param prepTableQueries
   *          List of queries to (re)create and set up table
   * @param insertIntoTemporaryTableQuery
   *          query template for inserting
   * @param resultSets
   *          a ResultSet for each parameter in insertIntoTemporaryTableQuery;
   *          each should have numberOfParameters elements
   * @param numberOfParameters
   *          Size of each ResultSet
   * @throws InterruptedException
   * @throws SQLException
   */
  protected void createTemporaryTable(List<Connection> connections,
      final String[] prepTableQueries, final String insertIntoTemporaryTableQuery,
      final Vector<ResultSet> resultSets, final int numberOfParameters)
      throws InterruptedException, SQLException {

    ExecutorService taskExecutor = Executors.newFixedThreadPool(connectionFactory
        .getAllConnections().size());
    if (VERBOSE) {
      // System.out.println(insertIntoTemporaryTableQuery + " NParameters = " +
      // numberOfParameters);
    }
    // add values from all result sets into a single vector
    // vector is copied to all Runnables?
    final Vector<String> vector = new Vector<String>();
    for (ResultSet rs : resultSets) {
      rs.beforeFirst();
      while (rs.next()) {
        for (int i = 1; i <= numberOfParameters; ++i) {
          vector.add(rs.getString(i));
        }
      }
    }

    for (final Connection connection : connectionFactory.getAllConnections()) {
      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // prepare table
            for (String query : prepTableQueries) {
              connection.createStatement().execute(query);
            }

            PreparedStatement stmt = connection.prepareStatement(insertIntoTemporaryTableQuery);
            int i = 0;
            while (i < vector.size()) {
              for (int j = 1; j <= numberOfParameters && i < vector.size(); ++j, ++i) {
                stmt.setString(j, vector.get(i));
              }
              stmt.executeUpdate();
            }
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      });
    }
    taskExecutor.shutdown();
    taskExecutor.awaitTermination(20, TimeUnit.MINUTES);
    if (VERBOSE) {
      // System.out.println("Done inserting: " + insertIntoTemporaryTableQuery);
    }
  }

  public static void main(String[] args) throws SQLException, InterruptedException,
      SqlConnectionException {
    new CubeBuilder().run();
  }
}
