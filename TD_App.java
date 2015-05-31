import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import com.treasure_data.jdbc.TreasureDataDriver;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;
import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobSummary;
import java.io.PrintWriter;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;

public class TD_App {
  public static void loadSystemProperties() throws IOException {
    Properties props = System.getProperties();
    props.load(TreasureDataDriver.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
  }

public static void main(String[] args) throws Exception {
   loadSystemProperties();
    try {

      Class.forName("com.treasure_data.jdbc.TreasureDataDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    // Assign each of the parameters from the user to variables
    String db_name = args[0];
    String table_name = args[1];
    String col_list = args[2];
    String min_time = args[3];
    String max_time = args[4];
    String query_engine = args[5];
    String output = args[6];
   
    // JDBC connection with the user define query engine
    Connection conn = DriverManager.getConnection(
      "jdbc:td://api.treasuredata.com/sample_datasets;useSSL=true;type=" + query_engine,
      "sean.fotoohi@gmail.com",
      "A1glasses");
    Statement stmt = conn.createStatement();
   
    String sql_stmt;
    
    // Construct the SQL statement based on provided start and end time by the 
    if (!min_time.equals("NULL")) {
        if (!max_time.equals("NULL")) //sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where time >=" + min_time + " and time <=" + max_time;
        {
            sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time," + min_time + "," + max_time + ")";
        } else {
            sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time," + min_time + ",NULL)";
        }
    } else if (!max_time.equals("NULL")) {
        sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time,NULL," + max_time + ")";
    } else {
        sql_stmt = "SELECT " + col_list + " FROM " + table_name;
    } 

    System.out.println("Running: " + sql_stmt);
  
    ResultSet res = stmt.executeQuery(sql_stmt);
    
    // get the number of columns being passed by the user
    int col_count = col_list.length() - col_list.replace(",", "").length()+ 1;
    
    //create a file when the user wants to output to a CSV file
    if (output.equals("csv"))
    {
        TreasureDataClient client = new TreasureDataClient();

        Job job = new Job(new Database("sample_datasets"), sql_stmt);
        
        // set the value of job type to either presto or hive based on the parameter        
        if (query_engine.equals("presto"))
            job.setType(Job.Type.PRESTO);
        else
            job.setType(Job.Type.HIVE);
        
        client.submitJob(job);
        String jobID = job.getJobID();
        System.out.println(jobID);

        while (true) {
            JobSummary.Status stat = client.showJobStatus(job);
            if (stat == JobSummary.Status.SUCCESS) {
                break;
            } else if (stat == JobSummary.Status.ERROR) {
                String msg = String.format("Job '%s' failed: got Job status 'error'", jobID);
                JobSummary js = client.showJob(job);
                if (js.getDebug() != null) {
                    System.out.println("cmdout:");
                    System.out.println(js.getDebug().getCmdout());
                    System.out.println("stderr:");
                    System.out.println(js.getDebug().getStderr());
                }
                throw new ClientException(msg);
            } else if (stat == JobSummary.Status.KILLED) {
                String msg = String.format("Job '%s' failed: got Job status 'killed'", jobID);
                throw new ClientException(msg);
            }
        }

        JobResult jobResult = client.getJobResult(job);
        Unpacker unpacker = jobResult.getResult(); // Unpacker class is MessagePack's deserializer
        UnpackerIterator iter = unpacker.iterator();
        
        // for CSV output, create a file called output.csv
        PrintWriter writer = new PrintWriter("output.csv", "UTF-8");

        while (iter.hasNext()) {
            ArrayValue row = iter.next().asArrayValue();
            for (Value elm : row) {
              //  System.out.print(elm + ",");
                writer.print(elm + ",");
            }
            writer.println();
        }
        writer.close();      
    }
    // if the user chose the tabular format, display it to the screen
    else
    {
        while (res.next()) {  
            for (int i=1;i<=col_count;i++){
          System.out.format("%-30s", String.valueOf(res.getObject(i)));
        }
        System.out.println();
        }
    }
}
}
