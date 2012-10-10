package olap_datacube;

public class CubeParameter implements Comparable {
  String name = "";
  String type;
  String selectClause;

  public CubeParameter(String name, String type, String selectClause) {
    this.name = name;
    this.type = type;
    this.selectClause = selectClause;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSelectClause() {
    return selectClause;
  }

  public void setSelectClause(String selectClause) {
    this.selectClause = selectClause;
  }

  @Override
  public String toString() {
    return this.name;
  }
  
  
  /**
   * compare by names
   */
  @Override
  public int compareTo(Object other) {
    if (other instanceof CubeParameter) {
      return this.name.compareTo(((CubeParameter) other).name);
    }

    return 0;
  }
}
