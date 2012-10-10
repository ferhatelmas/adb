package olap_datacube;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class Query {
  public String query;
  public List<Object> params;

  public Query(String query, List<Object> params) {
    super();
    this.query = query;
    this.params = params;
  }

  public Query(String query) {
    super();
    this.query = query;
    this.params = new ArrayList<Object>();
  }

  @Override
  public String toString() {
    return query.toString();
  }
}
