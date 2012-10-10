import database.IcDataSrvConnectionFactory;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class Validator {
    public static void main(String[] args) throws Exception {

        IcDataSrvConnectionFactory connections = new IcDataSrvConnectionFactory();

        String[] tables = {"part" ,"supplier", "customer", "lineitem", "orders", "nation", "region", "partsupp"};
        String query = "select * from ";

        for(String table : tables) {

            PrintWriter pw = new PrintWriter(new FileOutputStream(table));

            for(Connection connection : connections.getAllConnections()) {

                ResultSet rs = connection.createStatement().executeQuery(query + table + ";");

                ResultSetMetaData rsmd = rs.getMetaData();
                int cnt = rsmd.getColumnCount();

                while(rs.next()) {
                    for(int i=1; i<=cnt; i++) {

                        String s = rsmd.getColumnTypeName(i);
                        if(s.equals("INT")) pw.print("\"" + rs.getInt(i) + "\"");
                        else if(s.equals("VARCHAR")) pw.print("\"" + rs.getString(i) + "\"");
                        else if(s.equals("CHAR")) pw.print("\"" + rs.getString(i) + "\"");
                        else if(s.equals("DECIMAL")) pw.print("\"" + rs.getDouble(i) + "\"");
                        else  pw.print("\"" + rs.getDate(i) + "\"");

                        if(i!=cnt) pw.print(",");
                    }
                    pw.print("\n");
                }

            }

            pw.close();

        }

    }

}
