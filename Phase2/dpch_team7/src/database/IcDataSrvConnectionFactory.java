package database;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.Connection;

public class IcDataSrvConnectionFactory {

	String baseConnectionUrl = "jdbc:mysql://";
	String host = "icdatasrv";
	String localhost = "localhost";
	String port = "3306";
	private String dbName = "dbcourse1";
	String driver = "com.mysql.jdbc.Driver";
	String userName = "team7";
	String password = "EkfoytBeoc";
	String testUserName = "root";
	String testPassword = "";

	int nationAndRegionServerIndex = 7;

	Connection icdatasrv1_7;
	Connection icdatasrv2_7;
	Connection icdatasrv3_7;
	Connection icdatasrv4_7;
	Connection icdatasrv1_18;
	Connection icdatasrv2_18;
	Connection icdatasrv3_18;
	Connection icdatasrv4_18;

	public IcDataSrvConnectionFactory() throws SqlConnectionException {
		super();
		try {
			icdatasrv1_7 = makeConnection(1, 7);
			icdatasrv2_7 = makeConnection(2, 7);
			icdatasrv3_7 = makeConnection(3, 7);
			icdatasrv4_7 = makeConnection(4, 7);
			icdatasrv1_18 = makeConnection(1, 18);
			icdatasrv2_18 = makeConnection(2, 18);
			icdatasrv3_18 = makeConnection(3, 18);
			icdatasrv4_18 = makeConnection(4, 18);
		} catch (InstantiationException e) {
			throw new SqlConnectionException();
		} catch (IllegalAccessException e) {
			throw new SqlConnectionException();
		} catch (ClassNotFoundException e) {
			throw new SqlConnectionException();
		} catch (SQLException e) {
			throw new SqlConnectionException();
		}
	}

	public IcDataSrvConnectionFactory(boolean test) throws SqlConnectionException {
		if (test) {
			try {
				icdatasrv1_7 = makeTestConnection();
				icdatasrv2_7 = makeTestConnection();
				icdatasrv3_7 = makeTestConnection();
				icdatasrv4_7 = makeTestConnection();
				icdatasrv1_18 = makeTestConnection();
				icdatasrv2_18 = makeTestConnection();
				icdatasrv3_18 = makeTestConnection();
				icdatasrv4_18 = makeTestConnection();
			} catch (InstantiationException e) {
				throw new SqlConnectionException();
			} catch (IllegalAccessException e) {
				throw new SqlConnectionException();
			} catch (ClassNotFoundException e) {
				throw new SqlConnectionException();
			} catch (SQLException e) {
				throw new SqlConnectionException();
			}
		}
	}

	private Connection makeTestConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String url = baseConnectionUrl + localhost + ":" + port + "/";
		Class.forName(driver).newInstance();
		Connection conn = (Connection) DriverManager.getConnection(url + getDbName() + "?group_concat_max_len=100000", testUserName, testPassword);
		// System.out.println("Connection to " + localhost + " successful");
		return conn;
	}

	public List<Connection> getAllConnections() {
		List<Connection> connections = new ArrayList<Connection>();
		connections.add(icdatasrv1_7); // Server 0
		connections.add(icdatasrv2_7);
		connections.add(icdatasrv3_7);
		connections.add(icdatasrv4_7);
		connections.add(icdatasrv1_18);
		connections.add(icdatasrv2_18);
		connections.add(icdatasrv3_18);
		connections.add(icdatasrv4_18); // Server 7
		return connections;
	}

	private Connection makeConnection(int node, int zone) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection conn;
		String url = baseConnectionUrl + host + node + "-" + zone + ":" + port + "/";
		Class.forName(driver).newInstance();
		conn = (Connection) DriverManager.getConnection(url + dbName, userName, password);
		// System.out.println("Connection to " + host + node + "-" + zone + " successful");
		return conn;
	}

	public Connection getConnection(int node, int zone) {
		switch (node) {
		case 1:
			return zone == 7 ? icdatasrv1_7 : icdatasrv1_18;
		case 2:
			return zone == 7 ? icdatasrv2_7 : icdatasrv2_18;
		case 3:
			return zone == 7 ? icdatasrv3_7 : icdatasrv3_18;
		case 4:
			return zone == 7 ? icdatasrv4_7 : icdatasrv4_18;
		}
		return null;
	}

	@Override
	public void finalize() {
		try {
			close();
		} catch (SQLException e) {
			System.err.println("Unable to close database connections");
			e.printStackTrace();
		}
	}

	public void close() throws SQLException {
		if (icdatasrv1_7 != null) {
			icdatasrv1_7.close();
		}
		if (icdatasrv2_7 != null) {
			icdatasrv2_7.close();
		}
		if (icdatasrv3_7 != null) {
			icdatasrv3_7.close();
		}
		if (icdatasrv4_7 != null) {
			icdatasrv4_7.close();
		}
		if (icdatasrv1_18 != null) {
			icdatasrv1_18.close();
		}
		if (icdatasrv2_18 != null) {
			icdatasrv2_18.close();
		}
		if (icdatasrv3_18 != null) {
			icdatasrv3_18.close();
		}
		if (icdatasrv4_18 != null) {
			icdatasrv4_18.close();
		}
	}

	public String getDbName() {
		return dbName;
	}

	public Connection getConnectionAtIndex(int i) {
		return getAllConnections().get(i);
	}

	public int getNationAndRegionServerIndex() {
		return nationAndRegionServerIndex;
	}
}
