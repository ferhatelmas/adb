package operators.selection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import relations.Relation;

public class DateSelectionFilter implements SelectionFilterInterface {
  private int date_filter_column_index = -1;

  private SimpleDateFormat date_format;
  private Date date1;
  private Date date2;

  public static final String PARAM_DATEFILTER_PREFIX = "date_filter_column_index_";

  /**
   * @param conf
   * @param relation_name
   */
  public DateSelectionFilter(Configuration conf, String relation_name) {
    String date_filter_param = conf.get(PARAM_DATEFILTER_PREFIX + relation_name, "");

    // TODO: date filter is hard-coded for now. It is activated if
    // date_filter_column_index setting is set
    if (date_filter_param != "") {
      date_filter_column_index = Integer.parseInt(date_filter_param);
      System.out.println("date_filter_column_index for " + relation_name + ":"
          + date_filter_column_index);

      date_format = new SimpleDateFormat("yyyy-MM-dd");

      try {
        date1 = date_format.parse("1995-01-01");
        date2 = date_format.parse("1996-12-31");

      } catch (ParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  };

  public boolean checkSelection(String[] tuple) {
    // TODO: hard coded date filter
    if (date_filter_column_index >= 0) {

      try {
        Date date;
        date = date_format.parse(tuple[date_filter_column_index]);
        if (!(date1.compareTo(date) < 0 && date2.compareTo(date) > 0)) {
          return false;
        }
      } catch (ParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (Exception e) {
        // TODO: handle exception
        e.printStackTrace();

        System.out.println("error on Date.checkSelection for " + Arrays.toString(tuple));

      }

    }

    return true;

  }

  public static Configuration addSelection(Configuration conf, Relation rel, String columnName) {
    conf.set(DateSelectionFilter.PARAM_DATEFILTER_PREFIX + rel.name,
        rel.schema.getColumnIndex(columnName) + "");
    return conf;
  }

}
