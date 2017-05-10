/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vinodkumar
 */

import java.io.*;
import java.sql.*;
import static java.lang.Character.UnicodeBlock.of;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JProgressBar;

/**
 *
 * @author vkumar
 */
public class CSVTranslate  extends JFrame {
    
    ;// package access
    JLabel label;
    JButton button;
    JProgressBar progressBar;
    private long startTime = 0;
    private long endTime = 0;
    private int totalLines = 0;
    private int tick = 0;
    private String outputPath = "/";
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private String qry = "";
    
   public CSVTranslate(JButton button, JLabel label, JProgressBar progressBar){
       this.label = label;
       this.button = button;
       this.progressBar = progressBar;
   }
    
    public void process(File[] selectedFiles){
        
        int totalFiles = selectedFiles.length;
        
        int currentFileIndex = 1;
        
        
        
        for (File file : selectedFiles) {
            
            this.startTime = System.currentTimeMillis();
            this.progressBar.setValue(0);
            
            label.setText("Processing file "+currentFileIndex+" of "+totalFiles+": '"+ file + "'");
            
            //Count file lines so we can use for progress bar
            try {
                this.totalLines = countLines(file.getAbsolutePath());
            } catch (IOException ex) {
                Logger.getLogger(CSVTranslate.class.getName()).log(Level.SEVERE, null, ex);
            }

            this.tick = Math.round(this.totalLines/50);
        
            this.setupDatabase();
            this.importCSVToDb(file);
            this.outputFiles(file);
            
            
            this.endTime = System.currentTimeMillis();
           
            System.out.println("Finished for file : "+ file);
            System.out.println("Total execution time: " + (this.endTime - this.startTime) + "ms");
            this.progressBar.setValue(0);
            currentFileIndex++;
        }
        
        
        //reset label and button
        label.setText("Processing completed.");
        button.setEnabled(true);
        button.setText("Upload Files");
        
    }    
        
    public void setupDatabase(){
        
        try {
          Class.forName("org.sqlite.JDBC");
          this.conn = DriverManager.getConnection("jdbc:sqlite:test.db");
          
          this.progressBar.setValue(3);
          // Drop table
          this.stmt = this.conn.createStatement();
          this.qry = "DROP TABLE IF EXISTS BATCHPROCESS ";
          this.stmt.executeUpdate(this.qry);
          this.stmt.close();
          
          this.progressBar.setValue(5);
          //Create new table for data
          this.stmt = this.conn.createStatement();
          this.qry = "CREATE TABLE BATCHPROCESS " +
                        "(COL1   varchar(150), " + 
                        " COL2   varchar(150), " + 
                        " COL3   varchar(150), " + 
                        " COL4   varchar(150), " + 
                        " COL5   varchar(150), " + 
                        " COL6   varchar(150), " + 
                        " COL7   varchar(150), " + 
                        " COL8   varchar(150), " + 
                        " COL9   varchar(150), " + 
                        " COL10   varchar(150), " + 
                        " COL11   varchar(150), " + 
                        " COL12   varchar(150), " + 
                        " COL13   varchar(150), " + 
                        " COL14   varchar(150), " + 
                        " COL15   varchar(150), " + 
                        " COL16   varchar(150), " + 
                        " COL17   varchar(150))";  
          this.stmt.executeUpdate(this.qry);
          this.stmt.close();
          
          //c.close();
        } catch ( Exception e ) {
          System.err.println( e.getMessage() );
        }
        
        System.out.println("Table created successfully");
        
        this.progressBar.setValue(10);
             
    }
    
    public void importCSVToDb(File file){
            
         
        BufferedReader br = null;
        String line = "";
        
        try {
            br = new BufferedReader(new FileReader(file));
            
            //System.out.println("File rows are: " +br.length() );
            
            int rowCounter = 1;
            
            while ((line = br.readLine()) != null) {
               
                String[] data = line.split(",");
                
                
                    
                if(rowCounter % this.tick == 0){
                    int cpv =  this.progressBar.getValue();
                    cpv++;
                    this.progressBar.setValue(cpv); 
                }
                
                //remove header from input csv file
                if(rowCounter == 1){
                    rowCounter++;
                    continue;
                }
                
                try {
                    this.conn.setAutoCommit(false);
                    this.qry = "INSERT INTO BATCHPROCESS values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                    PreparedStatement ps = this.conn.prepareStatement(this.qry);
                    ps.setString(1,data[0]);
                    ps.setString(2,data[1]);
                    ps.setString(3,data[2]);
                    ps.setString(4,data[3]);
                    ps.setString(5,data[4]);
                    ps.setString(6,data[5]);
                    ps.setString(7,data[6]);
                    ps.setString(8,data[7]);
                    ps.setString(9,data[8]);
                    ps.setString(10,data[9]);
                    ps.setString(11,data[10]);
                    ps.setString(12,data[11]);
                    ps.setString(13,data[12]);
                    ps.setString(14,data[13]);
                    ps.setString(15,data[14]);
                    ps.setString(16,data[15]);
                    ps.setString(17,data[16]);
                    ps.executeUpdate();
                    
                    
                    //System.out.println(rowCounter);
                    
                    rowCounter++;
                    //TODO: need to close ps somewhere here
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
                
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
   
         
        try {
            this.conn.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(CSVTranslate.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        //this.progressBar.setValue(50);
        
        System.out.println("File imported successfully");
    }
    
    public void outputFiles(File file){
        
        String csvFileName = file.getName();
        
        
        int pos = csvFileName.lastIndexOf(".");
        String txtFileName = csvFileName.substring(0, pos);
       
        
        File output_directory = new File("output");
        this.deleteDir(output_directory);
        
        if (!output_directory.exists()) {
            output_directory.mkdir();
        }
        
        try {
            
          FileWriter fw_csv = new FileWriter(output_directory+"/"+csvFileName); 
          FileWriter fw_txt = new FileWriter(output_directory+"/"+txtFileName+".txt"); 
          // Query table for desired result
          this.stmt = this.conn.createStatement();
          this.qry = "SELECT * FROM BATCHPROCESS ORDER BY COL8";
          this.rs = this.stmt.executeQuery(this.qry);
            
          int rowCounter = 1;
          int long_col_num = 1;
          int slice_num = 1;
          
          String newCol = null;
       
          
            while(this.rs.next()){
                
                if(rowCounter % this.tick == 0){
                    int cpv =  this.progressBar.getValue();
                    cpv++;
                    
                    System.out.println(cpv);
                    
                    if(cpv <= 100){
                        this.progressBar.setValue(cpv); 
                    }
                    
                }
                
                if(newCol!= null && !newCol.equals(rs.getString("COL6")) ){
                    long_col_num++;
                    slice_num = 1;
                }
                
                newCol = rs.getString("COL6");
                
                int col2 = Math.round(Float.parseFloat(this.rs.getString("COL2")));
                int col11 = Math.round(Float.parseFloat(this.rs.getString("COL11")));
                
                //System.out.println(col11);
                //tabbed seperated txt file line data
                fw_txt.append(Integer.toString(rowCounter));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(long_col_num));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(slice_num));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(col11));
                fw_txt.append('\n');
               
                //comma seperated csv file line data
                fw_csv.append(Integer.toString(rowCounter));
                fw_csv.append(',');
                fw_csv.append(this.rs.getString("COL8"));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(long_col_num));
                fw_csv.append(',');
                fw_csv.append(this.rs.getString("COL7"));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(slice_num));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(col2));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(col11));
                fw_csv.append('\n');
                
               rowCounter++;
               slice_num++;
               
            }
          
            fw_txt.flush();
            fw_txt.close();
            
            fw_csv.flush();
            fw_csv.close();
          
          
          this.stmt.close();
        } catch ( Exception e ) {
          System.err.println(  e.getMessage() );
        } finally{
            try { this.stmt.close(); } catch (Exception e) { /* ignored */ }
            try { this.rs.close(); } catch (Exception e) { /* ignored */ }
            try { this.conn.close(); } catch (Exception e) { /* ignored */ }
        }
        
        this.progressBar.setValue(100);
        System.out.println("Output file");
    }
    
    public void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }
    
    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
    
}
