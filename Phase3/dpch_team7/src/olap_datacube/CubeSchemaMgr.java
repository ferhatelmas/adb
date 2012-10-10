package olap_datacube;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import util.Utils;

class DimensionList extends HashMap<String, CubeDimension> {
  private static final long serialVersionUID = -8636672785517906858L;

  public void add(CubeDimension dim) {
    this.put(dim.getName(), dim);

  }

}

public class CubeSchemaMgr {

  public static final String DIMENSION_TIME = "time";

  public static final String DIMENSION_CUST_LOCATION = "customer_location";

  public static final String DIMENSION_PRODUCT = "product";

  public static final String PRODUCT_ZOOM_ID = "product_id";

  public static final String TIME_ZOOM_YEAR = "year";

  public static final String TIME_ZOOM_YEARMONTH = "yearmonth";

  public static final String TIME_ZOOM_DATE = "date";

  public static final String LOCATION_ZOOM_REGION = "region";

  public static final String LOCATION_ZOOM_NATION = "nation";

  DimensionList cubeDimensions = new DimensionList();

  static CubeSchemaMgr instance;

  private CubeSchemaMgr() {
    initDimensions();
  }

  public static CubeSchemaMgr getInstance() {
    if (instance == null) {
      instance = new CubeSchemaMgr();
    }

    return instance;
  }

  public DimensionList getDimensions() {
    return cubeDimensions;
  }

  /**
   * 
   * @return attribute set (product of dimension zoom levels) together with
   *         option if materialized or not
   */
  public Set<CubeParameterSet> getAttributeSuperSet() {

    // Set<CubeParameter> + is_materialized
    Set<CubeParameterSet> parameterSuperSet = new HashSet<CubeParameterSet>();

    // initialise empty superset, to multiply by each of dimension zoom-levels
    Set<Set<CubeParameter>> attributeSuperSet = new HashSet<Set<CubeParameter>>();
    attributeSuperSet.add(new HashSet<CubeParameter>());

    // calculate a product of dimension values
    for (CubeDimension dim : cubeDimensions.values()) {
      attributeSuperSet = Utils.product(attributeSuperSet, dim.getZoomLevels());
    }

    // clean up and decide if to materialized or not!!
    // System.out.prinln()
    for (Set<CubeParameter> attributeSet : attributeSuperSet) {

      CubeParameterSet params = new CubeParameterSet(attributeSet);

      parameterSuperSet.add(params);
    }

    return parameterSuperSet;

  }

  void initDimensions() {
    Set<CubeParameter> cubeParameters = new LinkedHashSet<CubeParameter>();

    CubeDimension time = new CubeDimension(DIMENSION_TIME);

    // gives values like: 199601 (6 digits)
    time.addZoomLevel(TIME_ZOOM_YEAR, new CubeParameter("o_year", "SMALLINT",
        "extract(year from o_orderdate) as o_year"));

    time.addZoomLevel(TIME_ZOOM_YEARMONTH, new CubeParameter("o_yearmonth", "MEDIUMINT",
        "EXTRACT(YEAR_MONTH FROM O_ORDERDATE) AS o_yearmonth"));

    time.addZoomLevel(TIME_ZOOM_DATE, new CubeParameter("o_date", "DATE", "o_orderdate AS o_date"));
    cubeDimensions.add(time);

    CubeDimension c_location = new CubeDimension(DIMENSION_CUST_LOCATION);
    c_location.addZoomLevel(LOCATION_ZOOM_REGION, new CubeParameter("c_region_id", "integer",
        "n2.r_regionkey AS c_region_id"));
    c_location.addZoomLevel(LOCATION_ZOOM_NATION, new CubeParameter("c_nation_id", "integer",
        "n2.n_nationkey AS c_nation_id"));
    cubeDimensions.add(c_location);

    CubeDimension product_id = new CubeDimension(DIMENSION_PRODUCT);
    // TODO: we shall not materialized product_id X Time (yearmonth, full date).
    // Though we could create an equally looking view
    product_id.addZoomLevel(PRODUCT_ZOOM_ID, new CubeParameter("product_id", "integer",
        "l_partkey AS product_id"));
    cubeDimensions.add(product_id);

    // TODO: do we need supplier, suplier nation, etc ID as dimensions? If we
    // don't it just waste the precious space
    /*
     * cubeParameters.add(new CubeParameter("supplier_id", "integer",
     * "s.s_suppkey AS supplier_id"));
     * 
     * cubeParameters.add(new CubeParameter("s_nation_id", "integer",
     * "s.n_nationkey AS s_nation")); cubeParameters.add(new
     * CubeParameter("s_region_id", "integer", "s.r_regionkey AS s_region"));
     */

  }

}
