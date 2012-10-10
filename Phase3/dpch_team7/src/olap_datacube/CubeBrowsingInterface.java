package olap_datacube;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import util.Utils;

import com.mysql.jdbc.Connection;

import database.IcDataSrvConnectionFactory;
import database.SqlConnectionException;

public class CubeBrowsingInterface {

	public static void main(String[] args) throws IOException, InterruptedException, SQLException {
		IcDataSrvConnectionFactory connectionFactory = null;
		try {
			if (Utils.RUNNING_LOCALLY) {
				connectionFactory = new IcDataSrvConnectionFactory(true);
			} else {
				connectionFactory = new IcDataSrvConnectionFactory();
			}
		} catch (SqlConnectionException e) {
            System.err.println("Connection error:");
            e.printStackTrace();
            return;
        }
		final List<Connection> connections = connectionFactory.getAllConnections();

		long start = System.currentTimeMillis();
		CubeOperations olap = new CubeOperations(20);

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        olap.runQuery(connections);
        while (true) {
            System.out.print("command=> "); System.out.flush();
			String line = in.readLine();
            if (line == null){   // Ctrl+D
                System.out.println();
                break;
            }
			String input[] = line.split(" ");
			if (input.length == 0) {
				System.out.println("Command not recognized!");
				printHelp();
				continue;
			} else {
				if (input.length >= 2 && "drilldown".equalsIgnoreCase(input[0])) {
					if (olap.drillDown(input[1])) {
						olap.runQuery(connections);
					} else {
						printWarning();
					}
				} else if (input.length >= 2 && "rollup".equalsIgnoreCase(input[0])) {
					if (olap.rollUp(input[1])) {
						olap.runQuery(connections);
					} else {
						printWarning();
					}

				} else if (input.length >= 3 && "slice".equalsIgnoreCase(input[0])) {
					if (olap.slice(input[1], input[2])) {
						olap.runQuery(connections);
					} else {
						printWarning();
					}

				} else if (input.length >= 4 && "dice".equalsIgnoreCase(input[0])) {
					HashMap<String, ParameterSelection> dice = new HashMap<String, ParameterSelection>();
					dice.put(input[1], new ParameterSelection(input[2], input[3]));
					if (olap.dice(dice)) {

						olap.runQuery(connections);
					} else {
						printWarning();
					}
                } else if (input.length >= 2 && "limit".equalsIgnoreCase(input[0])){
                    olap.setLimit(Integer.parseInt(input[1]));
                    olap.runQuery(connections);
                } else if (input.length >= 2 && "dump".equalsIgnoreCase(input[0])){
                    int limit = 1000;
                    if (input.length >= 3)
                        limit = Integer.parseInt(input[2]);
                    try {
                        File file = new File(input[1]);
                        FileOutputStream fos = new FileOutputStream(file);
                        BufferedOutputStream bos = new BufferedOutputStream(fos);
                        PrintStream ps = new PrintStream(bos);
                        olap.dumpQuery(connections, ps, limit);
                        System.out.println("Dumped to "+file.getCanonicalPath());
                        ps.close();
                    } catch (IOException e){
                        System.err.println(e.getMessage());
                        e.printStackTrace();
                    }
                } else if ("undo".equalsIgnoreCase(input[0])) {
					if (olap.stepBack()) {
						olap.runQuery(connections);
					} else {
						printWarning();
					}
				} else if ("exit".equalsIgnoreCase(input[0]) || "quit".equalsIgnoreCase(input[0])) {
					break;
				} else {
					System.out.println("Command not recognized!");
					printHelp();
					continue;
				}
			}
		}
	}

	private static void printWarning() {
		System.out.println("Unable to perform operation.");
		System.out.println("Available dimensions are: product, time, customer_location");
	}

	private static void printHelp() {
		System.out.println("Available commands are: ");
		System.out.println("drilldown [dimension]");
		System.out.println("rollup [dimension]");
		System.out.println("slice [dimension] [arg1]");
		System.out.println("dice [dimension] [arg1] [arg2]");
        System.out.println("limit [num]\t\tSet maximum number of result to display");
        System.out.println("dump [file] [num]\t\tDump results of the last query to disk, with some upper limit (default 1000)");
        System.out.println("undo");
        System.out.println("exit");
		System.out.println("---");
		System.out.println("Available dimensions are: product, time, customer_location");
		System.out.println("---");
	}

}
