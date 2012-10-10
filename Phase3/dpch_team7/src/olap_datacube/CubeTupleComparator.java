package olap_datacube;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

public class CubeTupleComparator implements Comparator<ResultSet> {
  @Override
  public int compare(ResultSet r1, ResultSet r2) {
    int colCount;
    try {
      colCount = r1.getMetaData().getColumnCount();

      if (colCount != r2.getMetaData().getColumnCount()) {
        return -1;
      } else {
        for (int i = 1; i <= colCount; i++) {
          if (!"sum_volume".equals(r1.getMetaData().getColumnName(i))
              && !"count_volume".equals(r1.getMetaData().getColumnName(i))) {

            // handle extreme cases
            if (r1.getString(i) == null || r2.getString(i) == null)
              return 0;

            if (!r1.getString(i).equals(r2.getString(i))) {
              return r1.getString(i).compareTo(r2.getString(i));
            }
          }
        }
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return 0;
  }
}