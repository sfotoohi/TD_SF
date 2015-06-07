import java.io.IOException;
import java.util.Properties;
import com.treasure_data.jdbc.TreasureDataDriver;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;
import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSchema;
import com.treasure_data.model.TableSummary;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;

public class TD_App {
  public static void loadSystemProperties() throws IOException {
    Properties props = System.getProperties();
    props.load(TreasureDataDriver.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
  }

// this method writes the result of the query to a CSV file
private static void writeToFile(UnpackerIterator iter) throws FileNotFoundException, UnsupportedEncodingException
{
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
    System.out.println("The output was successfully written to the file: output.csv");
}

// this method displays the data in a tabular format
private static void displayTabular(String c1,UnpackerIterator i2,UnpackerIterator i3)
{
    String col_list = c1;
    UnpackerIterator iter = i2;
    UnpackerIterator iter2 = i3;
    
    int count = 0;
    int cnt = 0;
    int col_count = col_list.length() - col_list.replace(",", "").length()+ 1;

    String[] tokens = col_list.split(",", -1);

    // placeholder for longest value for each column for formatting purpuses
    Integer[] max_len = new Integer[col_count];
    
    // initialize to 0
    for (int j=0; j<col_count; j++)
        max_len[j] = 0;

    System.out.println();
    // figure out what the longest value for each column is
    while (iter.hasNext()) {  
        ArrayValue row = iter.next().asArrayValue(); 
        for (Value elm : row) {                 
            if ( elm.toString().length() > max_len[cnt])
                max_len[cnt] = elm.toString().replace("\"","").length()+2;
            cnt++;
        }
        cnt = 0;
        count++;      
    }
    
    // begin displaying the table by first showing the column heading based on the longest value being returned for each column
    for (int w=0; w < col_count; w++)
        System.out.format("%-"+max_len[w]+"s", tokens[w]);
    System.out.println();
    
    //create the line between column heading and the actual data based on length of the data
    for (int w=0; w < col_count; w++){
        String str = "";
        for (int a=0 ; a<max_len[w];a++)
            str = str + "-";
        System.out.format("%-"+max_len[w]+"s", str);
    }
    
    System.out.println();
     cnt =0;
    //start displaying the actual data following the same format as the header
    while (iter2.hasNext()) {  
        ArrayValue row = iter2.next().asArrayValue(); 
        for (Value elm : row) {
                System.out.format("%-"+max_len[cnt]+"s", elm.toString().replace("\"",""));                    
            cnt++;
        }
        cnt = 0;
        System.out.println();
    }
    
    //create the line between last row of data and total number of rows returned
    for (int w=0; w < col_count; w++) {
        String str = "";
        for (int a=0 ; a<max_len[w];a++)
            str = str + "-";
        System.out.format("%-"+max_len[w]+"s", str);
    }
    System.out.println();
    System.out.println(count + " rows returned.");
}

// this method check to make sure the parameters that are being sent to the program are well-formed and adhere to the specification
private static boolean checkParam(String[] params)
{
    // check to see if there are correct number of parameters are being passed
    if ( params.length == 7 ) {
        TreasureDataClient client = new TreasureDataClient();
        boolean table_found = false; 
      
        // verify the database nad table provided by the user actualy exists
        try {
            List<DatabaseSummary> databases = client.listDatabases();
            for (DatabaseSummary database : databases) {
                String databaseName = database.getName();
                List<TableSummary> tables = client.listTables(databaseName);
                if (!databaseName.equals(params[0])) {
                        System.out.println("Database name does not exist!");
                        System.exit(1);
                }
                else
                {
                    while (!table_found)
                    {
                        for (TableSummary table : tables) {
                            if (table.getName().equals(params[1])) 
                            {     
                                table_found = true;
                                break;
                            }
                        }
                         break;
                    }
                
                    if (!table_found)
                    {
                        System.out.println("Table does not exist");
                        System.exit(1);
                    }
                    
                    // check to see if the user provided the correct output format
                    if (!params[6].equals("csv"))
                        if (!params[6].equals("tabular")) {
                            System.out.println("You have provided unsupported output format. Supported formats are csv or tabular.");
                            System.exit(1);
                        }
                    
                    // check to see if the user provided the correct query engine
                    if (!params[5].equals("presto"))
                        if (!params[5].equals("hive")) {
                            System.out.println("You have provided unsupported query engine. Supported query engines are csv or tabular.");
                            System.exit(1);
                        }
                    // if both start time and end time are unix timestamps, make sure that start time is less than end time 
                    if (!params[4].equals("NULL") && !params[3].equals("NULL"))
                    {
                        int x = Integer.parseInt(params[3]);
                        int y = Integer.parseInt(params[4]);
                        if (x >= y){
                            System.out.println("Start date is larger than end date.");
                            System.exit(1);
                        }
                    }
                }
                            
            }        
        }catch (Exception e)
        {
            System.out.println("Unable to connect to the server");        
            System.exit(1);
        }
    return false;
    }
    else {    
        System.out.println("Please provide the right number of parameters for your query!");
        System.out.println("Accepted format: <database name> <table name> <col1,col2,...coln> <min time in unix timestamp> <max timestamp in unix timestamp> <query engine> <output format>");
        System.exit(1);
        return false;
    }
}

public static void main(String[] args) throws Exception {
   loadSystemProperties();
    try {
      Class.forName("com.treasure_data.jdbc.TreasureDataDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    
    //check to make sure that the user has provided the correct parameters
    checkParam(args);
    
    // Assign each of the parameters from the user to variables
    String db_name = args[0];
    String table_name = args[1];
    String col_list = args[2];
    String min_time = args[3];
    String max_time = args[4];
    String query_engine = args[5];
    String output = args[6];
      
    String sql_stmt;
    
    // Construct the SQL statement based on provided start and end time by the 
    if (!min_time.equals("NULL")) {
        if (!max_time.equals("NULL"))  {
            sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time," + min_time + "," + max_time + ")";
        } else {
            sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time," + min_time + ",NULL)";
        }
    } else if (!max_time.equals("NULL")) {
        sql_stmt = "SELECT " + col_list + " FROM " + table_name + " where TD_TIME_RANGE(time,NULL," + max_time + ")";
    } else {
        sql_stmt = "SELECT " + col_list + " FROM " + table_name;
    } 
    
    //show the SQL command that is being run to the user
    System.out.println("Running: " + sql_stmt);

    TreasureDataClient client = new TreasureDataClient();
    
    Job job = new Job(new Database(db_name), sql_stmt);

    // set the value of job type to either presto or hive based on the parameter        
    if (query_engine.equals("presto"))
        job.setType(Job.Type.PRESTO);
    else
        job.setType(Job.Type.HIVE);

    client.submitJob(job);
    String jobID = job.getJobID();
    System.out.println("Job ID: " + jobID);

    while (true) {
        JobSummary.Status stat = client.showJobStatus(job);
        if (stat == JobSummary.Status.SUCCESS) {
            break;
        } else if (stat == JobSummary.Status.ERROR) {
            System.out.println("The columns you have provided are not define in " + table_name);
            System.exit(1);
        }
         else if (stat == JobSummary.Status.KILLED) {
            String msg = String.format("Job '%s' failed: got Job status 'killed'", jobID);
            throw new ClientException(msg);
         }
    }
    
    //  go through the result set - created two for formatting purposes. 
    //  I looked for other means (out of the box functionality) to have the nicely foprmatted tabular output
    //  but I could not find any therefore, I created my own method for tabular format layout
    JobResult jobResult = client.getJobResult(job);
    JobResult jr = client.getJobResult(job);
    Unpacker unpacker = jobResult.getResult(); // Unpacker class is MessagePack's deserializer
    Unpacker up = jr.getResult();
    UnpackerIterator iter = unpacker.iterator();
    UnpackerIterator iter2 = up.iterator();

    // for CSV output, create a file called output.csv
    if (output.equals("csv"))
        writeToFile (iter);
     //if the user chose the tabular format, display it to the screen
    else
        displayTabular(col_list,iter,iter2);
    }
}
