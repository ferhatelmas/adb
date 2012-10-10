package relations;

public class Relation {

	/** default paths */
	static String inputPath = "";
	static String tmpPath = "";
	static String outputPath = "";

	/** relation schema */
	public Schema schema = null;
	
	/** data path */
	public String storageFileName;
	
	/** relation name */
	public String name = "";

	/** constructor for input relations only -- schema is provided explicitly */
	public Relation(Schema schema, String fileName) {
		this.schema = schema;
		this.storageFileName = inputPath + fileName;
		this.name = fileName;
	}
	
	/**
	 * constructor for intermediate relations only -- the schema will be specified
	 * inside join
	 */
	public Relation(String fileName) {
		this.storageFileName = tmpPath + fileName;
		this.name = fileName;
	}
	
	/** constructor for output relations */
	public Relation() {
		super();
		this.storageFileName = outputPath;
	}

	public static String getInputPath() {
		return inputPath;
	}

	public static void setInputPath(String inputFilesPrefix) {
		Relation.inputPath = inputFilesPrefix;
	}

	public static String getTmpPath() {
		return tmpPath;
	}

	public static void setTmpPath(String tmpFilesPrefix) {
		Relation.tmpPath = tmpFilesPrefix;
	}

	public static String getOutputPath() {
		return outputPath;
	}

	public static void setOutputPath(String outputPath) {
		Relation.outputPath = outputPath;
	}
}
