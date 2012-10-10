package util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import olap_datacube.Query;

import com.mysql.jdbc.Connection;

public class Utils {

  public static final boolean RUNNING_LOCALLY = false;

  static final boolean VERBOSE = false;
  private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MINUTES;
  private static final long TIMEOUT = 20; // in TIMEOUT_UNIT

  /**
   * Returns an iterable joined by separator
   * 
   * @param it
   * @return
   */
  public static String joinStringList(Iterable<String> it, String first_separator, String SEPARATOR) {
    StringBuilder sb = new StringBuilder();

    String separator = "";

    for (String s : it) {
      sb.append(separator);
      sb.append(s);
      separator = SEPARATOR;
    }
    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  public static String joinStringList(Iterator it, String first_separator, String SEPARATOR) {

    StringBuilder sb = new StringBuilder();

    String separator = "";

    while (it.hasNext()) {
      String s = it.next().toString();

      sb.append(separator);
      sb.append(s);
      separator = SEPARATOR;
    }
    return sb.toString();
    // TODO Auto-generated method stub
  }

  public static Vector<ResultSet> runDistributedQuery(List<Connection> connections,
      final String query, final List<Object> parameters) throws InterruptedException {
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

  public static ResultSet runQuery(Connection connection, String query, List<Object> parameters)
      throws SQLException {
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

  public static void runDistributedUpdate(List<Connection> connections, final String query,
      final List<Object> parameters) throws InterruptedException {
    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());
    for (final Connection connection : connections) {
      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            Utils.runUpdate(connection, query, parameters);
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      });
    }
    taskExecutor.shutdown();
    taskExecutor.awaitTermination(TIMEOUT, TIMEOUT_UNIT);
  }

  public static void runDistributedMultipleUpdates(List<Connection> connections,
      final List<Query> queries_and_params) throws InterruptedException {
    ExecutorService taskExecutor = Executors.newFixedThreadPool(connections.size());
    if (VERBOSE)
      System.out.println("Will runDistributedMultipleUpdates:" + queries_and_params);

    for (final Connection connection : connections) {
      taskExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            for (Query qp : queries_and_params) {
              if (VERBOSE)
                System.out.println("runDistributedMultipleUpdates:" + qp.query);
              Utils.runUpdate(connection, qp.query, qp.params);
            }
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      });
    }
    taskExecutor.shutdown();
    taskExecutor.awaitTermination(TIMEOUT, TIMEOUT_UNIT);
  }

  /**
   * Allows running multiple queries specified as strings with ; as delimiter
   * 
   * @param connections
   * @param queries
   * @throws InterruptedException
   */
  public static void runDistributedMultipleUpdates(List<Connection> connections, String queries)
      throws InterruptedException {
    if (VERBOSE)
      System.out.println("Will runDistributedMultipleUpdates:" + queries);
    List<Query> queries_and_params = new ArrayList<Query>();
    for (String q : queries.split(";"))
      queries_and_params.add(new Query(q));
    runDistributedMultipleUpdates(connections, queries_and_params);
  }

  public static void runUpdate(Connection connection, String query, List<Object> parameters)
      throws SQLException {
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

    statement.executeUpdate();
  }

  public static <T> Set<Set<T>> product(Set<Set<T>> superset_to_multiply, Set<T> set_multiply_by) {
    Set<Set<T>> result = new HashSet<Set<T>>();

    // multiply with each
    for (Set<T> original : superset_to_multiply) {
      for (T elm : set_multiply_by) {
        Set<T> set = new HashSet<T>();
        set.addAll(original);
        set.add(elm);
        result.add(set);
      }
    }

    return result;

  }

  public static <T> Set<Set<T>> powerSet(Set<T> originalSet) {
    Set<Set<T>> sets = new HashSet<Set<T>>();
    if (originalSet.isEmpty()) {
      sets.add(new HashSet<T>());
      return sets;
    }
    List<T> list = new ArrayList<T>(originalSet);
    T head = list.get(0);
    Set<T> rest = new HashSet<T>(list.subList(1, list.size()));
    for (Set<T> set : powerSet(rest)) {
      Set<T> newSet = new HashSet<T>();
      newSet.add(head);
      newSet.addAll(set);
      sets.add(newSet);
      sets.add(set);
    }
    return sets;
  }

  /**
   * Returns a collection sorted
   * 
   * credit:
   * http://stackoverflow.com/questions/740299/how-do-i-sort-a-set-to-a-list
   * -in-java/740301#740301
   * 
   * @param c
   * @return
   */
  public static <T extends Comparable<? super T>> ArrayList<T> asSortedList(Collection<T> c) {
    ArrayList<T> list = new ArrayList<T>(c);
    java.util.Collections.sort(list);
    return list;
  }

}
