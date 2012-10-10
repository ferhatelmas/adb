import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import operators.groupby.GroupBy;
import operators.join.HadoopJoin;
import operators.selection.SelectionEntry;
import operators.selection.SelectionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import relations.Relation;
import relations.Schema;

public class dbq7 extends Configured implements Tool {

	/** input relations*/
	static Relation relSupplier, relNations, relLineItem, relCustomer, relOrder;

	/** intermediate result realtions */
	static Relation relImdN1Supplier, relImdN1SupplierLineItem, relImdN2Customer,
	relImdN2CustomerOrders, relImdJoinResult;

	/** result relation */
	static Relation relQueryResult;

	/** monitor interval*/
	static int monitorInterval = 5000;
	
	/** force reduce side join */
	static boolean forceReduceSideJoin = false;
	
	/** number of mapper per task*/
	static List<Integer> numMapper = Arrays.asList(0, 0, 0, 0, 0 ,0);
	
	/** number of reducer per task*/
	static List<Integer> numReducer = Arrays.asList(0, 0, 0, 0, 0, 0);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new dbq7(), args);
		System.exit(res);
	}

	private int printUsage() {
		System.out.println("dbq7 -i <inputdir> [-t <tempdir>] [-f] " +
				"[-m <memoryjoin_threshold>] [-map <list #mapper>] " +
				"[-red <list #reducer>] <nation1> <nation2>");
	    ToolRunner.printGenericCommandUsage(System.out);
	    return -1;
	}

	@Override
	public int run(String[] args) throws Exception {
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-i".equals(args[i])) {
					i++;
					if (!args[i].endsWith("/")) {
						args[i] = args[i] + "/";
					}
					Relation.setInputPath(args[i]);
				} else if ("-t".equals(args[i])) {
					i++;
					if (!args[i].endsWith("/")) {
						args[i] = args[i] + "/";
					}
					Relation.setTmpPath(args[i] + "tmp/");
					Relation.setOutputPath(args[i] + "out/");
				} else if ("-f".equals(args[i])) {
					forceReduceSideJoin = true;
				} else if ("-m".equals(args[i])) {
					i++;
					HadoopJoin.setMemoryBackedThreshold(Integer.parseInt(args[i]));
				} else if ("-map".equals(args[i])) {
					numMapper = new ArrayList<Integer>();
					for (int j = 1; j < 7; j++) {
						numMapper.add(Integer.parseInt(args[i+j]));
					}
					i = i+6;
				} else if ("-red".equals(args[i])) {
					numReducer = new ArrayList<Integer>();
					for (int j = 1; j < 7; j++) {
						numReducer.add(Integer.parseInt(args[i+j]));
					}
					i = i+6;
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i-1]);
				return printUsage();
			}
		}
		
		// Set temp directory if user didn't set
		if (Relation.getTmpPath().isEmpty()) {
			Relation.setTmpPath(Relation.getInputPath() + "../temp/tmp/");
			Relation.setOutputPath(Relation.getInputPath() + "../temp/out/");
		}
		
		//System.out.println(Arrays.toString(numMapper.toArray()));
		//System.out.println(Arrays.toString(numReducer.toArray()));
		
		// schema config
		schemaConfig();
		
		// mMake sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		// delete temporary directory
		deleteTemp();

		// nation parammeters
		String nation1 = other_args.get(0);
		String nation2 = other_args.get(1);

		//System.out.println(nation1 + " " + nation2);
		
		// Phase 1: Nation1 |><| Supplier and Nation2 |><| Customer
		System.out.println();
		Job n1_supp = new Job(nation1_Supplier(nation1, nation2)); 
		
		System.out.println();
		Job n2_cust = new Job(nation2_Customer(nation1, nation2));

		JobControl jobs = new JobControl("jbcntrl");
		jobs.addJob(n1_supp);
		jobs.addJob(n2_cust);

		Thread jobClient = 	new Thread(jobs);
		jobClient.start();

		while (!jobs.allFinished()) {
			//jobStatus(jobs);
		}

		// Phase 2: N1Supp |><| LineItem and N2Cust |><| Orders
		System.out.println();
		Job n1_supp_lineitem = new Job(n1Supp_LineItem(), new ArrayList<Job>(Arrays.asList(n1_supp)));
		
		System.out.println();
		Job n2_cust_orders = new Job(n2Cust_Orders(), new ArrayList<Job>(Arrays.asList(n2_cust)));
		
		jobs.addJob(n1_supp_lineitem);
		jobs.addJob(n2_cust_orders);

		while (!jobs.allFinished()) {
			//jobStatus(jobs);
		}

		// Phase 3: Final join and Group by
		System.out.println();
		Job finalJoin = new Job(finalJoin(), new ArrayList<Job>(Arrays.asList(n1_supp, n2_cust))); 
		
		System.out.println();
		Job groupOrder = new Job(groupBy(nation1, nation2), new ArrayList<Job>(Arrays.asList(finalJoin)));

		jobs.addJob(finalJoin);
		jobs.addJob(groupOrder);

		while (!jobs.allFinished()) {
			//jobStatus(jobs);
		}

		jobs.stop();
		
		// output final result
		outputFinalResult();

		return 0;
	}

	public static void schemaConfig() {	
		// input Relations
		relSupplier = new Relation(new Schema(Schema.SUPPLIER_FIELDS), "supplier.tbl");
		relNations = new Relation(new Schema(Schema.NATIONS_FIELDS), "nation.tbl");
		relLineItem = new Relation(new Schema(Schema.LINEITEM_FIELDS), "lineitem.tbl");
		relCustomer = new Relation(new Schema(Schema.CUSTOMER_FIELDS), "customer.tbl");
		relOrder = new Relation(new Schema(Schema.ORDERS_FIELDS), "orders.tbl");

		// Intermediate result files
		relImdN1Supplier = new Relation("ImdN1Supplier");
		relImdN1SupplierLineItem = new Relation("ImdN1SupplierLineItem");
		relImdN2Customer = new Relation("ImdN2Customer");
		relImdN2CustomerOrders = new Relation("ImdN2CustomerOrders");
		relImdJoinResult = new Relation("ImdJoinResult");

		// Result file
		relQueryResult = new Relation();
	}

	/** Nation1 |><| Supplier job configuration */
	public static JobConf nation1_Supplier(String nation1, String nation2) throws IOException{
		@SuppressWarnings("unchecked")
		List<SelectionEntry<String>> nationFilters = Arrays.asList(
				new SelectionEntry<String>("NAME", nation1, 
						SelectionFilter.AND + SelectionEntry.CTRL_DELIMITER + 
						Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), 
						new SelectionEntry<String>("NAME", nation2, SelectionFilter.OR + 
								SelectionEntry.CTRL_DELIMITER + Schema.STRING + 
								SelectionEntry.CTRL_DELIMITER +"="));

		String nation1Alias = "n1_";	
		List<String> nation1Projection = Arrays.asList("NAME", nation1Alias);
		List<String> supplierProjection = Arrays.asList("SUPPKEY", "");

		JobConf jobConf = HadoopJoin.join(relNations, relSupplier, relImdN1Supplier, 
				nationFilters, null, nation1Projection, supplierProjection, "NATIONKEY", 
				forceReduceSideJoin);
		
		if (numMapper.get(0) != 0) 
			jobConf.setNumMapTasks(numMapper.get(0));
		if (numReducer.get(0) != 0) 
			jobConf.setNumReduceTasks(numReducer.get(0));
		
		return jobConf;
	}

	/** N1Supp |><| LineItem job configuration */
	public static JobConf n1Supp_LineItem() throws IOException{
		@SuppressWarnings("unchecked")
		List<SelectionEntry<String>> shipdateFilters = Arrays.asList(
				new SelectionEntry<String>("SHIPDATE", "1995-01-01", 
						SelectionFilter.AND + 
						SelectionEntry.CTRL_DELIMITER + Schema.DATE +
						SelectionEntry.CTRL_DELIMITER + ">="), 
				new SelectionEntry<String>("SHIPDATE", "1996-12-31", 
						SelectionFilter.AND +
						SelectionEntry.CTRL_DELIMITER + Schema.DATE +
						SelectionEntry.CTRL_DELIMITER + "<="));

		String nation1Alias = "n1_";
		List<String> imdNation1Projection = Arrays.asList(nation1Alias + "NAME", "");
		List<String> lineItemProjection = Arrays.asList("ORDERKEY", "EXTENDEDPRICE", "DISCOUNT", "SHIPDATE", "");

		JobConf jobConf = HadoopJoin.join(relImdN1Supplier, relLineItem, relImdN1SupplierLineItem, 
				null, shipdateFilters, imdNation1Projection, lineItemProjection, "SUPPKEY", 
				forceReduceSideJoin); 
		
		if (numMapper.get(2) != 0) 
			jobConf.setNumMapTasks(numMapper.get(2));
		if (numReducer.get(2) != 0) 
			jobConf.setNumReduceTasks(numReducer.get(2));
		
		return jobConf;
	}

	/** Nation2 |><| Customer job configuration */
	public static JobConf nation2_Customer(String nation1, String nation2) throws IOException{
		@SuppressWarnings("unchecked")
		List<SelectionEntry<String>> nationFilters = Arrays.asList(
				new SelectionEntry<String>("NAME", nation1, 
						SelectionFilter.AND + 
						SelectionEntry.CTRL_DELIMITER + 
						Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), 
				new SelectionEntry<String>("NAME", nation2, 
						SelectionFilter.OR + 
						SelectionEntry.CTRL_DELIMITER + Schema.STRING + 
						SelectionEntry.CTRL_DELIMITER +"="));

		String nation2Alias = "n2_";	
		List<String> nation2Projection = Arrays.asList("NAME", nation2Alias);
		List<String> customerProjection = Arrays.asList("CUSTKEY", "");
		
		JobConf jobConf = HadoopJoin.join(relNations, relCustomer, relImdN2Customer, nationFilters, 
				null, nation2Projection, customerProjection, "NATIONKEY", 
				forceReduceSideJoin);
		
		if (numMapper.get(1) != 0) 
			jobConf.setNumMapTasks(numMapper.get(1));
		if (numReducer.get(1) != 0) 
			jobConf.setNumReduceTasks(numReducer.get(1));
		
		return jobConf;
	}

	/** N2Cust |><| Orders job configuration */
	public static JobConf n2Cust_Orders() throws IOException{
		String nation2Alias = "n2_";
		List<String> imdNation2Projection = Arrays.asList(nation2Alias + "NAME", "");
		List<String> orderProjection = Arrays.asList("ORDERKEY", "");

		JobConf jobConf = HadoopJoin.join(relImdN2Customer, relOrder, relImdN2CustomerOrders, 
				null, null, imdNation2Projection, orderProjection, "CUSTKEY", 
				forceReduceSideJoin);
		
		if (numMapper.get(3) != 0) 
			jobConf.setNumMapTasks(numMapper.get(3));
		if (numReducer.get(3) != 0) 
			jobConf.setNumReduceTasks(numReducer.get(3));
		
		return jobConf;
		
	}

	/** FinalJoin job configuration */
	public static JobConf finalJoin() throws IOException{
		String nation1Alias = "n1_";
		String nation2Alias = "n2_";
		List<String> imdN1LineItemProjection = Arrays.asList(nation1Alias + "NAME", "EXTENDEDPRICE", "DISCOUNT", "SHIPDATE", "");
		List<String> imdNation2Projection = Arrays.asList(nation2Alias + "NAME", "");

		JobConf jobConf = HadoopJoin.join(relImdN1SupplierLineItem, relImdN2CustomerOrders, 
				relImdJoinResult, null, null, imdN1LineItemProjection, 
				imdNation2Projection, "ORDERKEY", forceReduceSideJoin);
		
		jobConf.set("mapred.child.java.opts", "-Xms256m -Xmx2g -XX:+UseSerialGC");
		jobConf.set("mapred.job.map.memory.mb", "1024");

		if (numMapper.get(4) != 0) 
			jobConf.setNumMapTasks(numMapper.get(4));
		if (numReducer.get(4) != 0) 
			jobConf.setNumReduceTasks(numReducer.get(4));
		
		return jobConf;
	}

	/** GroupBy job configuration */
	public static JobConf groupBy(String nation1, String nation2) throws IOException{
		String nation1Alias = "n1_";
		String nation2Alias = "n2_";
		List<String> groupByFields = Arrays.asList(nation1Alias + "NAME", nation2Alias + "NAME", "SHIPDATE");

		@SuppressWarnings("unchecked")
		List<SelectionEntry<String>> nationFilters = Arrays.asList(
				new SelectionEntry<String>(nation1Alias + "NAME", nation1, 
						SelectionFilter.AND + 
						SelectionEntry.CTRL_DELIMITER + 
						Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), 
				new SelectionEntry<String>(nation2Alias + "NAME", nation2, 
						SelectionFilter.AND + 
						SelectionEntry.CTRL_DELIMITER + 
						Schema.STRING + SelectionEntry.CTRL_DELIMITER +"="),
				new SelectionEntry<String>(nation1Alias + "NAME", nation2, 
						SelectionFilter.OR + 
						SelectionEntry.CTRL_DELIMITER + 
						Schema.STRING + SelectionEntry.CTRL_DELIMITER + "="), 
				new SelectionEntry<String>(nation2Alias + "NAME", nation1, 
						SelectionFilter.AND + 
						SelectionEntry.CTRL_DELIMITER + 
						Schema.STRING + SelectionEntry.CTRL_DELIMITER +"="));
		
		JobConf jobConf = GroupBy.createJob(new Configuration(), relImdJoinResult, nationFilters, 
				null, groupByFields, relQueryResult);
		
		if (numMapper.get(5) != 0) 
			jobConf.setNumMapTasks(numMapper.get(5));
		if (numReducer.get(5) != 0) 
			jobConf.setNumReduceTasks(numReducer.get(5));
		
		return jobConf;
	}
	
	/** delete the temporary directory */
	public static void deleteTemp() {
		try {
			FileSystem fs = FileSystem.get(new JobConf());
			Path tempPath = new Path(Relation.getTmpPath() + "..");
			if (fs.delete(tempPath, true)) {
				System.out.println("Delete " + Relation.getTmpPath() + ".. successful");
			}
		} catch (Exception e) {
			System.out.println("Temporary directory doesn't exist");
		}
	}
	
	/** output final result */
	public static void outputFinalResult() {
		String outputFileName = "TCPH7FinalResult.txt";
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(outputFileName);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "utf-8"));

			FileSystem fs = FileSystem.get(new JobConf());
			Path outputPath = new Path(Relation.getOutputPath());
			FileStatus[] files = fs.listStatus(outputPath);
			
			for (FileStatus file : files) {
				if (!file.isDir()) {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					
					String line = null;
					while((line = br.readLine()) != null) {
						bw.write(line + "\n");
					}
				}
			}
			
			bw.flush();
			bw.close();
			fos.close();
		} catch (Exception e) {
			System.out.println("Can't write to output file TCPH7FinalResult.txt");
			e.printStackTrace();
		}
	}

	public static void jobStatus(JobControl jobs) {
		System.out.println("Jobs in waiting state: "
				+ jobs.getWaitingJobs().size());
		System.out.println("Jobs in ready state: "
				+ jobs.getReadyJobs().size());
		System.out.println("Jobs in running state: "
				+ jobs.getRunningJobs().size());
		System.out.println("Jobs in success state: "
				+ jobs.getSuccessfulJobs().size());
		System.out.println("Jobs in failed state: "
				+ jobs.getFailedJobs().size());
		System.out.println("\n");

		try {
			Thread.sleep(monitorInterval);
		} catch (Exception e) {  
			e.printStackTrace();
		}
	}
}
