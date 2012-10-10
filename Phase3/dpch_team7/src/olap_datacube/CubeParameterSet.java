package olap_datacube;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import operators.remotejoin.BloomJoin;

import com.mysql.jdbc.Connection;

import database.IcDataSrvConnectionFactory;
import database.SqlConnectionException;

import util.Utils;

public class CubeParameterSet {
  private static final boolean VERBOSE = true;
  private static final boolean RUNNING_LOCALLY = Utils.RUNNING_LOCALLY;

  /**
   * Do we replicate part locally, or would query all machine during OLAP for
   * product titles (if needed)?
   */
  private static final boolean PART_NAME_STORED_LOCALY = true;

  /**
   * Defines the part replication to be run in N^2 or N threads -- this could be
   * completely distributed through SSH processes
   */
  private static final boolean PART_REPLICATE_N_SQUARE_THREADS = true;

  public Set<CubeParameter> attributeSet = new HashSet<CubeParameter>();

  public HashMap<CubeParameter, ParameterSelection> attributeSelection = new HashMap<CubeParameter, ParameterSelection>();

  public CubeParameterSet() {
    super();
  }

  public void add(CubeParameter parameter) {
    attributeSet.add(parameter);
  }

  public void add(CubeParameter parameter, String value) {
    attributeSet.add(parameter);
    attributeSelection.put(parameter, new ParameterSelection(value, value));
  }

  public void add(CubeParameter parameter, ParameterSelection ps) {
    attributeSet.add(parameter);
    if (ps != null) {
      attributeSelection.put(parameter, ps);
    }
  }

  public CubeParameterSet(Set<CubeParameter> parameters) {
    super();
    this.attributeSet = parameters;
  }

  @Override
  public String toString() {
    // TODO Auto-generated method stub
    StringBuilder sb = new StringBuilder();
    for (CubeParameter param : getSortedParameterList()) {
      sb.append(param.name + "__");
    }

    return (materialized() ? "T__" : "V__") + sb.toString();
  }

  public Set<CubeParameter> getNonNullParameters() {
    Set<CubeParameter> r = new HashSet<CubeParameter>();
    r.addAll(attributeSet);

    // remove null parameter (i.e. ALL)
    r.remove(null);

    return r;
  }

  public ArrayList<CubeParameter> getSortedParameterList() {
    // TODO: this shall be better sorted by dimension name however!!
    return Utils.asSortedList(getNonNullParameters());

  }

  public String getTableName() {
    return "cube_results_" + this.toString();
  }

  public String getTableDropSQL() {

    String q = "DROP " + (materialized() ? "TABLE" : "VIEW") + " IF EXISTS " + this.getTableName()
        + ";";

    // if (VERBOSE) System.out.println(q);
    return q;
  }

  // TODO: use TEMPORARY for testing only!!!
  public String getTableCreationSQL() {
    if (this.materialized()) {
      String q = "CREATE " + (RUNNING_LOCALLY ? "TEMPORARY" : "") + " TABLE " + this.getTableName()
          + " (";
      String separator = "";
      for (CubeParameter cp : getSortedParameterList()) {
        q += separator + " " + cp.getName() + " " + cp.getType();
        separator = " , ";
      }
      q += separator + " sum_volume FLOAT ";

      separator = ",";
      q += separator + " count_volume INT ";

      q += ");";

      if (VERBOSE) {
        // System.out.println("CubeBuilder table query: " + q);
      }
      return q;
    }
    return null;
  }

  public String getQuerySQL() {
    return "SELECT " + getQueryProjection() + getQueryBody(true);
  }

  public String getQueryProjection() {
    return getFullQueryProjection();
  }

  /**
   * This is used both for building datacube and querying it
   * 
   * We need not the title string columns then building the datacube.
   * 
   * 
   * 
   * @return
   */
  public String _getQueryProjection(boolean for_olap_query, boolean include_distributed_data,
      boolean as_materialized) {
    boolean querying_datacube = !(!as_materialized || !for_olap_query);

    String cube_projection = "";
    String separator = " ";

    if (for_olap_query && as_materialized) {
      cube_projection = "subcube.*";
      separator = ",";
    }

    for (CubeParameter cp : getSortedParameterList()) {
      if (!querying_datacube) {
        cube_projection += separator + cp.getSelectClause();
        separator = ", ";
      }

      // TODO: this is a hack
      if (for_olap_query)
        if (cp.name.equals("c_region_id")) {
          cube_projection += separator + "n2.r_name AS c_region_name";
        } else if (cp.name.equals("c_nation_id")) {
          cube_projection += separator + "n2.n_name AS c_nation_name";
        } else if (include_distributed_data && cp.name.equals("product_id")) {
          cube_projection += separator + "pt_product_title AS product_name";
        }

    }

    // TODO: AVG don't make sense!
    if (!querying_datacube) {
      cube_projection = cube_projection + (cube_projection.isEmpty() ? "" : ",")
          + " SUM(L_EXTENDEDPRICE * (1.0 - L_DISCOUNT)) as sum_volume "
          + ", COUNT(*) as count_volume";
    }
    return cube_projection;
  }

  public String getCubeConstructionSQL() {
    return "(SELECT " + _getQueryProjection(false, false, this.materialized()) + " FROM "
        + getCubeBuildingQueryBody() + ")";
  }

  /**
   * Returns list of projections (columns) including the strings that are not
   * stored in the datacube itself. An extra join is automatically added by
   * getQueryBodyO() then querying the datacube.
   * 
   * @return
   */
  public String getFullQueryProjection() {
    // TODO vidmantas: fix
    return _getQueryProjection(true, true, this.materialized());
  }

  public Set<String> getFullQueryExtraJoins(boolean include_also_distributed,
      boolean as_materialized) {
    // TODO vidmantas: fix
    Set<String> extraRelations = new HashSet<String>();

    for (CubeParameter cp : getSortedParameterList()) {

      // these are needed only for materialized views
      if (as_materialized) {
        if (cp.name.equals("c_region_id")) {
          // TODO: we have a problem now!! we need a table with only region!!
          extraRelations.add(" LEFT JOIN region_temp n2 ON (c_region_id=n2.r_regionkey) ");

        }

        if (cp.name.equals("c_nation_id")) {
          extraRelations.add(" LEFT JOIN nation_region_temp n2 ON (c_nation_id=n2.n_nationkey) ");
        }
      }

      if (include_also_distributed) {
        // product is never materialized, therefore we may need it
        if (cp.name.equals("product_id")) {
          extraRelations
              .add(" LEFT JOIN product_temp ON (product_id = product_temp.pt_product_id) ");
        }

        // TODO: ooohh one more problem to look at, we need to download part if
        // querying per product_id!!!
        // But this could be done on main machine with the silly bloom join we
        // used for milestone 2

        // extraRelations.add(" LEFT JOIN part product ON (product_id = product.p_partkey) ");
      }
    }
    return extraRelations;
  }

  public String getQueryGroupBy() {
    String cube_grouping = "";

    String separator = "";
    for (CubeParameter cp : getSortedParameterList()) {
      cube_grouping += separator + " " + cp.getName();
      separator = " , ";
    }
    return ((cube_grouping != "") ? " GROUP BY " + cube_grouping : "");
  }

  public String getCubeBuildingQueryBody() {
    // Note: "supp_temp s" and where AND l_suppkey=s.s_suppkey is not needed for
    // now, but could be added if space allowed

    return _getQueryBody(new ArrayList<String>());

  }

  /**
   * @return
   */
  private String _getQueryBody(Iterable<String> extra_relations) {

    return " lineitem, orders, customer, nation_region_temp n2 "
        + Utils.joinStringList(extra_relations, "", " ") // This contains extra
                                                         // joins
        // then performing OLAP
        // queries
        + " WHERE l_orderkey=o_orderkey AND o_custkey=c_custkey AND c_nationkey=n2.n_nationkey"
        + getQueryGroupBy();
  }

  /***
   * Returns query body (without FROM) for accessing the data (not for building)
   * 
   * Automatically adds fields for what is not materialized directly in datacube
   * 
   * @return
   */
  public String getQueryBody(boolean include_also_distributed) {
    if (this.materialized()) {
      return this.getTableName()
          + " AS subcube "
          + Utils.joinStringList(
              getFullQueryExtraJoins(include_also_distributed, this.materialized()), " ", " ");

    } else {
      return this._getQueryBody(getFullQueryExtraJoins(include_also_distributed,
          this.materialized()));
    }
  }

  public String getQuerySelection(CubeParameter cp) {
    ParameterSelection ps = attributeSelection.get(cp);
    if (ps == null) {
      return null;
    } else {
      if (ps.lowerBound.equals(ps.upperBound)) {
        return cp.name + " = '" + ps.lowerBound + "'";
      } else {
        return cp.name + " >= '" + ps.lowerBound + "' AND " + cp.name + " <= '" + ps.upperBound
            + "'";
      }
    }
  }

  public String getQueryWhere() {
    // used on dice

    String cubeWhere = "";

    String separator = " ";
    for (CubeParameter cp : getSortedParameterList()) {
      String selection = getQuerySelection(cp);
      if (selection != null) {
        cubeWhere += separator + getQuerySelection(cp);
        separator = " AND ";
      }
    }

    return cubeWhere.isEmpty() ? "" : " WHERE " + cubeWhere;
  }

  /**
   * Now ordering is by dimensions
   * 
   * @return
   */
  public String getQueryOrderBy() {
    String cubeOrderby = "";

    String separator = " ";
    for (CubeParameter cp : getSortedParameterList()) {
      cubeOrderby += separator + cp.getName();
      separator = ", ";
    }
    return (cubeOrderby.isEmpty()) ? "" : " ORDER BY " + cubeOrderby;

  }

  // TODO: not using this in datacube, could it fit?: part_temp,
  // l_partkey=p_partkey

  // TODO: we DO NOT USE SUPPLIER YET

  public String getDataMaterizationSQL() {

    String cubeQuery;

    if (materialized()) {
      cubeQuery = "INSERT INTO " + getTableName() + " " + getCubeConstructionSQL();
    } else {
      return null;
    }
    return cubeQuery;
  }

  /**
   * Returns SQL for running OLAP query with currently set parameters Takes care
   * of the fact that subcube accessed may be materialized or not
   * 
   * @return
   * @throws InterruptedException
   */
  public Vector<ResultSet> getOLAPQueryResults(List<Connection> connections, int rowLimit)
      throws InterruptedException, SQLException {
    Vector<ResultSet> result;

    boolean DEBUGTHIS = false;

    if (VERBOSE)
      System.out.println("Getting results for these dimensions: "
          + Utils.joinStringList(attributeSet.iterator(), "", ", "));

    String cubeQuery = "";

    String bloomJoinQuery = "";

    if (this.materialized()) {
      cubeQuery = "SELECT " + this.getQueryProjection() + " FROM " + this.getQueryBody(true)
          + this.getQueryWhere() + this.getQueryOrderBy();

      bloomJoinQuery = "SELECT " + _getQueryProjection(true, false, this.materialized()) + " FROM "
          + this.getQueryBody(false) + this.getQueryWhere() + this.getQueryOrderBy();
    } else {
      // TODO: simplify stuff: get sort of basic query that would work just as
      // simple table
      // apply the same stuff...

      // get sort of basic query that would work just as a stored table
      String basicQuery = getCubeConstructionSQL();

      if (DEBUGTHIS)
        System.out.println("[DEBUG] basic Query: " + basicQuery);

      cubeQuery = "SELECT " + _getQueryProjection(true, false, true) + " FROM " + basicQuery
          + " AS  subcube " + Utils.joinStringList(getFullQueryExtraJoins(true, true), " ", " ")
          + this.getQueryWhere() + this.getQueryOrderBy();

      bloomJoinQuery = "SELECT " + _getQueryProjection(true, false, true) + " FROM " + basicQuery
          + " AS  subcube " + Utils.joinStringList(getFullQueryExtraJoins(false, true), " ", " ")
          + " " + this.getQueryWhere() + this.getQueryOrderBy();

      if (DEBUGTHIS)
        System.out.println("[DEBUG] cube Query: " + cubeQuery);
      if (DEBUGTHIS)
        System.out.println("[DEBUG] cube Query: " + bloomJoinQuery);

      //
      // cubeQuery = "SELECT * FROM (SELECT " + this.getQueryProjection() +
      // " FROM "
      // + this.getQueryBody(true) + " ) AS cubeq " + this.getQueryWhere()
      // + this.getQueryOrderBy();
      //
      // bloomJoinQuery = "SELECT * FROM (SELECT " + _getQueryProjection(true,
      // false) + " FROM "
      // + this.getQueryBody(false) + " ) AS cubeq " + this.getQueryWhere()
      // + this.getQueryOrderBy();

    }

    if (rowLimit > 0) {
      cubeQuery += " LIMIT " + rowLimit;
      bloomJoinQuery += " LIMIT " + rowLimit;
    }

    // TODO: if we are querying Product we need a bloom-join to fetch
    // product_names

    if (!PART_NAME_STORED_LOCALY)
      if (this.attributeSet.contains(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_PRODUCT,
          CubeSchemaMgr.PRODUCT_ZOOM_ID))) {

        // System.out.println(bloomJoinQuery);

        ssh_fetchProductRows(connections, bloomJoinQuery);
        // fetchProductRows(connections, bloomJoinQuery);

      }

    result = Utils.runDistributedQuery(connections, cubeQuery, new ArrayList<Object>());

    return result;

  }

  // save all products as workaround
  public static void saveAllProductNames(List<Connection> connections) throws InterruptedException,
      SQLException {
    String bloomJoinQuery = "select l_partkey AS product_id from lineitem";

    ssh_fetchProductRows(connections, bloomJoinQuery);

  }

  /**
   * @param connections
   * @param cubeQuery
   * @throws InterruptedException
   * @throws SQLException
   */
  private static void ssh_fetchProductRows(List<Connection> connections, String cubeQuery)
      throws InterruptedException, SQLException {
    if (VERBOSE)
      System.out.println("Need to fetch product titles with BloomJoin. This will take time...");

    // each machine execute this, but on every server
    final String tableAndFilters = "( SELECT * FROM (" + cubeQuery + ") AS prod) AS prod1 ", filterColumn = "product_id";

    // TODO: use sort of transaction/query id
    final String transID = "";

    // TODO: just for testing now, we have to switch to SSH
    final ArrayList<Connection> final_connections = new ArrayList<Connection>(connections);

    // We either use N or N^2 Threads to simulate completely distributed
    // processing
    if (!PART_REPLICATE_N_SQUARE_THREADS) {
      for (final Connection connection : connections) {
        ssh_onEachMachine(final_connections, connection, tableAndFilters, "product_id", transID);
      }
    } else {
      ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());

      for (final Connection connection : connections) {

        taskExecutor.execute(new Runnable() {
          @Override
          public void run() {

            // TODO: this will have to be called through SSH including the
            // arguments!!!

            try {

              // TODO: pass tableAndFilters, currentMachine
              ssh_onEachMachine(final_connections, connection, tableAndFilters, "product_id",
                  transID);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (SQLException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }

            // TODO: start SSH, send the parameters and wait for completion
            // after individual completion we could even start fetching the
            // results

          }

        });

      }
      taskExecutor.shutdown();
      taskExecutor.awaitTermination(200, TimeUnit.MINUTES);
    }

    if (VERBOSE)
      System.out.println("Products fetched.");
  }

  /**
   * @param connections
   * @param tableAndFilters
   * @param filterColumn
   * @throws InterruptedException
   * @throws SQLException
   */
  private static void ssh_onEachMachine(List<Connection> connections,
      final Connection localMachine, final String tableAndFilters, final String filterColumn,
      String transID) throws InterruptedException, SQLException {

    // TODO: append transaction ID

    localMachine.createStatement()
        .executeUpdate("drop table if exists " + transID + "product_temp");

    localMachine.createStatement().executeUpdate(
        "create table " + transID
            + "product_temp (pt_product_id integer PRIMARY KEY, pt_product_title varchar(55));");
    // TODO: create primary key: pt_product_id

    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());

    // Fetch bit vector from local machine SQL Server
    final Blob bitvector = BloomJoin.getBitVector_binary(localMachine, tableAndFilters,
        filterColumn);

    if (VERBOSE)
      System.out.println("Bitvector fetched. fetching values...");

    for (final Connection connection : connections) {

      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {

            // Fetch tuples from particular SQL Server

            ResultSet resultSet = BloomJoin.getBloomFilteredResult_binary(connection, bitvector,
                "SELECT P_PARTKEY AS PRODUCT_ID, P_NAME as product_title FROM part", "",
                "P_PARTKEY");

            // now as I have bitvector from particular machine, fetch the
            // strings locally
            PreparedStatement stmt = localMachine
                .prepareStatement("insert into product_temp values (?, ?);");

            long maxCountBeforeCommit = 100;
            long count_in_batch = 0;

            resultSet.beforeFirst();
            while (resultSet.next()) {
              count_in_batch++;

              if (count_in_batch > maxCountBeforeCommit) {
                stmt.executeBatch();
                count_in_batch = 0;
              }

              int id = resultSet.getInt("PRODUCT_ID");
              String title = resultSet.getString("product_title");
              stmt.setInt(1, id);
              stmt.setString(2, title);

              // System.out.println("Adding:" + id + ":" + new String(title));

              // TODO: can we batch items in prepared statement?
              // stmt.executeUpdate();
              stmt.addBatch();
            }
            if (count_in_batch > 0)
              stmt.executeBatch();
            stmt.close();

            // bitvectors.add(bitvector);
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      });
    }
    taskExecutor.shutdown();
    taskExecutor.awaitTermination(20, TimeUnit.MINUTES);

    if (VERBOSE)
      System.out.println("product copying done");

  }

  /**
   * @param connections
   * @param cubeQuery
   * @throws InterruptedException
   * @throws SQLException
   */
  private void fetchProductRows(List<Connection> connections, String cubeQuery)
      throws InterruptedException, SQLException {
    if (VERBOSE)
      System.out.println("Need to fetch product titles with BloomJoin. This will take time...");

    // clean up from earlier runs
    Utils.runDistributedMultipleUpdates(connections, "drop temporary table if exists product_temp");

    String productBitVector = BloomJoin.getBitVector(connections, "( SELECT * FROM (" + cubeQuery
        + ") AS prod) AS prod1 ", "product_id");

    if (VERBOSE)
      System.out.println("Bitvector fetched. fetching values...");

    // start = System.currentTimeMillis();
    Vector<ResultSet> resultSets = BloomJoin.getBloomFilteredResults(connections, productBitVector,
        "SELECT P_PARTKEY AS PRODUCT_ID, P_NAME as product_title FROM part", "", "P_PARTKEY");
    // it's not worth sending them...

    HashMap<Integer, String> names = new HashMap<Integer, String>();
    for (ResultSet rs : resultSets) {
      rs.beforeFirst();
      while (rs.next()) {
        int id = rs.getInt("PRODUCT_ID");
        String title = rs.getString("product_title");
        names.put(new Integer(id), title);
      }
    }

    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());

    final Vector<Entry<Integer, String>> names_final = new Vector<Entry<Integer, String>>(
        (Collection<? extends Entry<Integer, String>>) names.entrySet());

    for (final Connection connection : connections) {

      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {

          try {
            connection
                .createStatement()
                .executeUpdate(

                "create temporary table product_temp (pt_product_id integer, pt_product_title varchar(55));");

            PreparedStatement stmt = connection
                .prepareStatement("insert into product_temp values (?, ?);");

            for (Entry<Integer, String> entry : names_final) {
              stmt.setInt(1, entry.getKey());
              stmt.setString(2, entry.getValue());
              // System.out.println("Adding:" + entry.getValue());

              // this shall be slower: stmt.executeUpdate();
              stmt.addBatch();

            }
            stmt.executeBatch();
            stmt.close();

          } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }

      });
    }

    taskExecutor.shutdown();
    taskExecutor.awaitTermination(20, TimeUnit.MINUTES);

    if (VERBOSE)
      System.out.println("Products fetched.");
  }

  /* whether to materialized or just create a view */
  public boolean materialized() {
    // for now we don't only product ID with time [per yearmonth and date]
    if (attributeSet.contains(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_PRODUCT,
        CubeSchemaMgr.PRODUCT_ZOOM_ID))
        && (attributeSet.contains(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_TIME,
            CubeSchemaMgr.TIME_ZOOM_DATE)) || attributeSet.contains(CubeDimension.getByKey(
            CubeSchemaMgr.DIMENSION_TIME, CubeSchemaMgr.TIME_ZOOM_YEARMONTH)))) {

      return false;
    }
    return true;
  }

  public static void main(String[] args) throws InterruptedException, SQLException {
    testProduct();

  }

  public static void testProduct() throws InterruptedException, SQLException {
    IcDataSrvConnectionFactory connectionFactory = null;
    try {
      if (Utils.RUNNING_LOCALLY)
        connectionFactory = new IcDataSrvConnectionFactory(true);
      else
        connectionFactory = new IcDataSrvConnectionFactory();
    } catch (SqlConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    final List<Connection> connections = connectionFactory.getAllConnections();

    long start = System.currentTimeMillis();

    CubeBuilder.getStatistics(connections);

    CubeOperations olap = new CubeOperations(20);
    olap.currentView.add(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_CUST_LOCATION,
        CubeSchemaMgr.LOCATION_ZOOM_REGION));
    olap.currentView.add(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_TIME,
        CubeSchemaMgr.TIME_ZOOM_YEARMONTH));

    olap.currentView.add(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_PRODUCT,
        CubeSchemaMgr.PRODUCT_ZOOM_ID));

    olap.drillDown(CubeSchemaMgr.DIMENSION_CUST_LOCATION);
    olap.rollUp(CubeSchemaMgr.DIMENSION_TIME);

    // olap.drillDown(CubeSchemaMgr.DIMENSION_PRODUCT);
    // Testing slice plus
    // olap.slice(CubeSchemaMgr.DIMENSION_TIME, "1997");

    olap.runQuery(connections);

    System.out.println("Completed in " + (System.currentTimeMillis() - start) + "msecs");

  }

  /**
   * @throws IOException
   * @throws InterruptedException
   * 
   *           TODO: This do not work as expected
   */
  private static void sshTest() throws IOException, InterruptedException {
    /**
     * TO run we first have to do: ssh-agent bash ssh-add ~/.ssh/id_rsa
     */
    System.out.println("Starting SSH");

    Process p = Runtime.getRuntime().exec(
        new String[] { "ssh", "-i", "~/.ssh/id_rsa", "team7@icdatasrv1", "echo aa > ~/test" });

    // "'echo aa > /export/home/team7/test'"
    PrintStream out = new PrintStream(p.getOutputStream());
    BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));

    //
    // 'ls ~'
    out.println("ls");

    out.println("echo aa > ~/test");
    while (in.ready()) {
      String s = in.readLine();
      System.out.println(s);
    }
    System.out.println("DoneLoop");

    out.println("exit");
    p.destroy();
    // p.waitFor();
    System.out.println("Done");

  }
}
