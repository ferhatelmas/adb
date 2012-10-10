package olap_datacube;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.Vector;

import util.Utils;

import com.mysql.jdbc.Connection;

import database.IcDataSrvConnectionFactory;
import database.SqlConnectionException;

public class CubeOperations {

    private Stack<CubeParameterSet> viewStack;

    public CubeParameterSet currentView;

    int rowLimit;

    public CubeOperations(int rowLimit) {
        this.rowLimit = rowLimit;

        // initializing starting view
        currentView = new CubeParameterSet();
        viewStack = new Stack<CubeParameterSet>();
    }

    public boolean rollUp(String dimension) {
        CubeParameterSet newCps = new CubeParameterSet();
        boolean isValid = false;

        CubeDimension cd = CubeSchemaMgr.getInstance().cubeDimensions.get(dimension);
        if (cd == null) {
            return false;
        }
        for (CubeParameter cp : currentView.attributeSet) {
            if (cd.zoom_level_list.contains(cp)) {
                if ((cp = cd.getPreviousZoomLevel(cp)) != null) {
                    newCps.add(cp);
                } else {
                    // Still valid. Just don't add the attribute to the new list of attributes.
                }
                isValid = true;
            } else {
                newCps.add(cp, currentView.attributeSelection.get(cp));
            }
        }

        if (isValid) {
            viewStack.push(currentView);
            currentView = newCps;
        }

        return isValid;
    }

    public boolean drillDown(String dimension) {
        CubeParameterSet newCps = new CubeParameterSet();
        boolean isValid = true;
        boolean isExists = false;

        System.out.println(dimension);
        CubeDimension cd = CubeSchemaMgr.getInstance().cubeDimensions.get(dimension);
        if (cd == null) {
            return false;
        }
        for (CubeParameter cp : currentView.attributeSet) {
            ParameterSelection ps = currentView.attributeSelection.get(cp);
            if (cd.zoom_level_list.contains(cp)) {
                isExists = true;
                if (cp == cd.getNextZoomLevel(cp)) {
                    isValid = false;
                    break;
                } else {
                    cp = cd.getNextZoomLevel(cp);
                    newCps.add(cp, cd.getPreviousZoomLevelValue(currentView, cp));
                }
            } else {
                newCps.add(cp, ps);
            }
        }

        if (!isExists) {
            newCps.add(cd.zoom_level_list.get(0));
        }

        if (isValid) {
            viewStack.push(currentView);
            currentView = newCps;
        }

        return isValid;
    }

    public boolean stepBack() {
        boolean isValid = false;
        if (!viewStack.empty()) {
            currentView = viewStack.pop();
            isValid = true;
        }
        return isValid;
    }

    public boolean slice(String dimension, String value) {
        CubeParameterSet newCps = new CubeParameterSet();
        boolean isValid = true;

        CubeDimension cd = CubeSchemaMgr.getInstance().cubeDimensions.get(dimension);
        if (cd == null) {
            return false;
        }
        for (CubeParameter cp : currentView.attributeSet) {
            ParameterSelection ps = currentView.attributeSelection.get(cp);
            if (cd.zoom_level_list.contains(cp)) {
                // check slice validity
                if (ps != null && (value.compareTo(ps.lowerBound) == -1 || value.compareTo(ps.upperBound) == 1)) {
                    isValid = false;
                    break;
                } else {
                    newCps.add(cp, value);
                }
            } else {
                newCps.add(cp, ps);
            }
        }

        if (isValid) {
            viewStack.push(currentView);
            currentView = newCps;
        }

        return isValid;
    }

    public boolean dice(HashMap<String, ParameterSelection> dimensionValues) {
        CubeParameterSet newCps = new CubeParameterSet();
        boolean isValid = true;

        for (String dimension : dimensionValues.keySet()) {
            CubeDimension cd = CubeSchemaMgr.getInstance().cubeDimensions.get(dimension);
            if (cd == null) {
                return false;
            }
            ParameterSelection reqPs = dimensionValues.get(dimension);
            for (CubeParameter cp : currentView.attributeSet) {
                ParameterSelection ps = currentView.attributeSelection.get(cp);
                if (cd.zoom_level_list.contains(cp)) {
                    // check dice validity
                    if (ps != null && !(reqPs.lowerBound.compareTo(ps.lowerBound) > -1 && reqPs.upperBound.compareTo(ps.upperBound) < 1)) {
                        isValid = false;
                        break;
                    } else {
                        newCps.add(cp, reqPs);
                    }
                } else {
                    newCps.add(cp, ps);
                }
            }
        }

        if (isValid) {
            viewStack.push(currentView);
            currentView = newCps;
        }

        return isValid;
    }

    public void runQuery(List<Connection> connections)  {
        dumpQuery(connections, System.out, rowLimit);
    }

    public void dumpQuery(List<Connection> connections, PrintStream out, int limit){
        try {
            Vector<ResultSet> result = currentView.getOLAPQueryResults(connections, limit);

            ResultSet rs = result.get(0);
            int colCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= colCount; i++) {
                if (!"sum_volume".equals(rs.getMetaData().getColumnName(i)) && !"count_volume".equals(rs.getMetaData().getColumnName(i))) {
                    out.format("%16s", rs.getMetaData().getColumnName(i));
                }
            }
            out.format("%16s%16s\n", "sum_volume", "count_volume");

            // initialize the result set
            int j = 0;
            while (j < result.size()) {
                ResultSet r = result.get(j);
                if (!r.next()) {
                    result.remove(j);
                } else {
                    j++;
                }
            }

            // merge result set
            while (!result.isEmpty()) {
                CubeTupleComparator tupleComparator = new CubeTupleComparator();
                Collections.sort(result, tupleComparator);

                for (int i = 1; i <= colCount; i++) {
                    if (!"sum_volume".equals(rs.getMetaData().getColumnName(i)) && !"count_volume".equals(rs.getMetaData().getColumnName(i))) {
                        out.format("%16s", result.get(0).getString(i));
                    }
                }

                int i = 1;
                double sum = result.get(0).getDouble("sum_volume");
                double count = result.get(0).getDouble("count_volume");
                while (i < result.size() && tupleComparator.compare(result.get(0), result.get(i)) == 0) {
                    sum += result.get(i).getDouble("sum_volume");
                    count += result.get(i).getDouble("count_volume");
                    if (!result.get(i).next()) {
                        result.remove(i);
                    } else {
                        i++;
                    }
                }
                if (!result.get(0).next()) {
                    result.remove(0);
                }

                out.format("%16e%16.0f\n", sum, count);
            }
        } catch (SQLException e){
            System.err.println("Error performing query in DB:");
            e.printStackTrace();
        } catch (InterruptedException e){
            System.err.println("Interruption error:");
            e.printStackTrace();
        }
    }

    public void generate(List<Connection> connections, int operationLength) {

        Random r = new Random();

        DimensionList dl = CubeSchemaMgr.instance.getDimensions();
        for (int i = 0; i < operationLength; i++) {
            double d = r.nextDouble();

            // for later usage
            // CubeDimension cd;
            String dimension;

            if (d <= .33) {
                dimension = CubeSchemaMgr.DIMENSION_CUST_LOCATION;
            } else if (d <= .66) {
                dimension = CubeSchemaMgr.DIMENSION_PRODUCT;
            } else {
                dimension = CubeSchemaMgr.DIMENSION_TIME;
            }

            d = r.nextDouble();
            if (d < .5) {
                rollUp(dimension);
            } else {
                drillDown(dimension);
            }

            try {
                runQuery(connections);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
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
        // olap.currentView.add(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_CUST_LOCATION,
        // CubeSchemaMgr.LOCATION_ZOOM_NATION));
        // olap.currentView.add(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_TIME,
        // CubeSchemaMgr.TIME_ZOOM_YEAR));
        // olap.currentView.add(CubeDimension.getByKey(CubeSchemaMgr.DIMENSION_PRODUCT,
        // CubeSchemaMgr.PRODUCT_ZOOM_ID));

        {
            olap.runQuery(connections);

            olap.drillDown(CubeSchemaMgr.DIMENSION_PRODUCT);
            olap.runQuery(connections);

            olap.drillDown(CubeSchemaMgr.DIMENSION_TIME);
            olap.runQuery(connections);

            HashMap<String, ParameterSelection> dice = new HashMap<String, ParameterSelection>();
            dice.put(CubeSchemaMgr.DIMENSION_TIME, new ParameterSelection("1992", "1996"));
            olap.dice(dice);
            olap.runQuery(connections);

            olap.drillDown(CubeSchemaMgr.DIMENSION_TIME);
            olap.runQuery(connections);

            olap.slice(CubeSchemaMgr.DIMENSION_TIME, "199301");
            olap.runQuery(connections);

            olap.drillDown(CubeSchemaMgr.DIMENSION_CUST_LOCATION);
            olap.runQuery(connections);

            olap.rollUp(CubeSchemaMgr.DIMENSION_PRODUCT);
            olap.runQuery(connections);

        }

        System.out.println("Completed in " + (System.currentTimeMillis() - start) + "msecs");
    }

    public void setLimit(int i) {
        rowLimit = i;
    }
}
