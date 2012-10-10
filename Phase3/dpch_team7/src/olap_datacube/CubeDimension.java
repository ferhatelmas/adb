package olap_datacube;

import database.SqlConnectionException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class CubeDimension {
  private static HashMap<String, String> nation_region_map = null;
  public static final String ZOOM_LEVEL_ALL = "All";
  String name;

  // + implied null
  HashMap<String, CubeParameter> zoom_level_map = new HashMap<String, CubeParameter>();

  // this is to preserve the order
  ArrayList<CubeParameter> zoom_level_list = new ArrayList<CubeParameter>();

  CubeDimension(String name) {
    this.name = name;

    this.zoom_level_map.put(ZOOM_LEVEL_ALL, null);
  }

  public void addZoomLevel(String name, CubeParameter attribute) {
    zoom_level_map.put(name, attribute);
    zoom_level_list.add(attribute);
  }

  public static CubeParameter getByKey(String dimension_name, String zoom_level) {
    CubeParameter result = CubeSchemaMgr.getInstance().cubeDimensions.get(dimension_name).zoom_level_map
        .get(zoom_level);
    // System.out.println("CubeDimension.getByKey(" + dimension_name + " " +
    // zoom_level + "): "
    // + CubeSchemaMgr.getInstance().cubeDimensions.get(dimension_name) +
    // ", result: " + result);

    return result;

  }

  public Set<CubeParameter> getZoomLevels() {
    Set<CubeParameter> set = new HashSet<CubeParameter>();
    set.addAll(zoom_level_map.values());
    return set;
  }

  /**
   * returns next (deeper) zoom level for drill down
   * 
   * @param current
   * @return
   */
  public CubeParameter getNextZoomLevel(CubeParameter current) {
    int current_index = zoom_level_list.indexOf(current);
    if (current_index + 1 < zoom_level_list.size())
      return zoom_level_list.get(current_index + 1);

    return current;

  }

  /**
   * returns previous (more general) zoom level for roll up
   * 
   * @param current
   * @return
   */
  public CubeParameter getPreviousZoomLevel(CubeParameter current) {
    int current_index = zoom_level_list.indexOf(current);

    if (current_index > 0)
      return zoom_level_list.get(current_index - 1);

    return null;
  }

  public ParameterSelection getPreviousZoomLevelValue(CubeParameterSet cps, CubeParameter cp) {
    ParameterSelection ps = cps.attributeSelection.get(cp);
    if(ps == null) return null;

    ParameterSelection newPs = null;
    int index = zoom_level_list.indexOf(cp);

    if(CubeSchemaMgr.DIMENSION_TIME.equals(name)) {
      switch (index) {
        case 0: newPs = null; break;
        case 1: newPs = null; break;
        case 2: newPs = new ParameterSelection(ps.lowerBound.substring(0, 4), ps.upperBound.substring(0, 4)); break;
        case 3: newPs = new ParameterSelection(ps.lowerBound.substring(0, 6), ps.upperBound.substring(0, 6)); break;
      }
    } else if(CubeSchemaMgr.DIMENSION_CUST_LOCATION.equals(name)) {
      switch (index) {
        case 0: newPs = null; break;
        case 1: newPs = null; break;
        case 2:
          try {	
            if(nation_region_map == null) {
              CubeBuilder cb = new CubeBuilder();
              ResultSet rs = cb.getNationAndRegion(cb.connectionFactory.getAllConnections());

              nation_region_map = new HashMap<String, String>();
              while(rs.next()) {
                nation_region_map.put(rs.getString(1), rs.getString(4));
              }
              rs.close();
            }
          } catch (Exception e) {
        	  e.printStackTrace();
          }

          newPs = new ParameterSelection(nation_region_map.get(ps.lowerBound), ps.upperBound);
          break;
      }
    }
    return newPs;
  }
  
  public ParameterSelection getNextZoomLevelValue(CubeParameterSet cps, CubeParameter cp) {
	ParameterSelection ps = cps.attributeSelection.get(cp);
	if(ps == null) return null;
	
	ParameterSelection newPs = null;
	int index = zoom_level_list.indexOf(cp);
	
	if(CubeSchemaMgr.DIMENSION_TIME.equals(name)) {
	  switch (index) {
	    case 0: newPs = new ParameterSelection("1900", "2100"); break;
	    case 1: newPs = new ParameterSelection(ps.lowerBound + "01", ps.upperBound + "12"); break;
	    case 2: newPs = new ParameterSelection(ps.lowerBound + "31", ps.upperBound + "31"); break;
	    case 3: newPs = null; // shouldn't be called
	  }
	} else if(CubeSchemaMgr.DIMENSION_CUST_LOCATION.equals(name)) {
	  switch (index) {
	    case 0: newPs = new ParameterSelection("0", "4"); break;
	    case 1: newPs = new ParameterSelection("0", "24"); break;
	    case 2:
	      newPs = null; break; // shouldn't be called
	  }
	}
	return newPs;
	}
  public String getName() {
    return name;
  }
}
