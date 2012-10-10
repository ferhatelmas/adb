import java.sql.SQLException;

import query.Q7;
import query.Q8;
import database.SqlConnectionException;

public class Main {
	public static void main(String[] args) throws SqlConnectionException, SQLException, InterruptedException {

		long start = System.currentTimeMillis();

		if (args.length > 0) {
			if (args[0].equalsIgnoreCase("Q7") && args.length == 3) {
				new Q7(args[1].toUpperCase(), args[2].toUpperCase()).run();
                System.out.println("Completed in " + (System.currentTimeMillis() - start) + "msecs");
			} else if(args[0].equalsIgnoreCase("Q8") && args.length == 4){
				new Q8(args[1].toUpperCase(), args[2].toUpperCase(), args[3].toUpperCase()).run();
                System.out.println("Completed in " + (System.currentTimeMillis() - start) + "msecs");
            } else {
                printUsage();
            }
        } else {
            printUsage();
		}
	}

    private static void printUsage() {
        System.out.println("\nUsage: java -jar dpch-team.jar <Query> <Query Parameters>");
        System.out.println("<Query> : \t\tQ7 or Q8");
        System.out.println("<Query Parameters> : \tFor Q7 <Nation1> <Nation2>");
        System.out.println("\t\t\tFor Q8 <Nation> <Region> <Type>");
        System.out.println("For input with spaces, quote the string.");
    }
}
