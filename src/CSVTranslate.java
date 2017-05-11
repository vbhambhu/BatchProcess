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
            
            label.setText("Processing file "+currentFileIndex+" of "+totalFiles+": '"+ file.getName() + "'");
            
            //Count file lines so we can use for progress bar
            try {
                this.totalLines = countLines(file.getAbsolutePath());
            } catch (IOException ex) {
                Logger.getLogger(CSVTranslate.class.getName()).log(Level.SEVERE, null, ex);
            }

            this.tick = Math.round(this.totalLines/50);
        
            this.setupDatabase();
            System.out.println("DB setup done");
            this.importCSVToDb(file);
            System.out.println("DB import done");
            this.outputFiles(file);
            
            
            this.endTime = System.currentTimeMillis();
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
                        "(TRACK_ID   INT(15), " + 
                        " TRACK_TIME   INT(15), " + 
                        " POS_X_VALUE   INT(15), " +
                        " POS_Y_VALUE   INT(15) )";
          this.stmt.executeUpdate(this.qry);
          this.stmt.close();
          
          //c.close();
        } catch ( Exception e ) {
          System.err.println( e.getMessage() );
        }
        
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
                    this.qry = "INSERT INTO BATCHPROCESS values(?,?,?,?)";
                    PreparedStatement ps = this.conn.prepareStatement(this.qry);
                    
                    String track_id_str = data[7].trim();
                    String track_time_str = data[6].trim();
                    String pos_x_str = data[1].trim();
                    String pos_y_str = data[10].trim();
                    
                    //If any col blank or without data skip it
                    if(track_id_str.length() == 0 || track_time_str.length() == 0 || pos_x_str.length() == 0 || pos_y_str.length() == 0){
                      System.out.println("Converting problem " + rowCounter + " posx " + pos_x_str + " posy " + pos_y_str);
                      continue;
                    }
                    
                
                    ps.setInt(1,Integer.parseInt(track_id_str));
                    ps.setInt(2,Integer.parseInt(track_time_str));
                    ps.setInt(3,Math.round(Float.parseFloat(pos_x_str)));
                    ps.setInt(4,Math.round(Float.parseFloat(pos_y_str)));
                    ps.executeUpdate();
                    
                    
                    rowCounter++;
                    //TODO: need to close ps somewhere here
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                    System.out.println(ex.getStackTrace());
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
        
    }
    
    public void outputFiles(File file){
        
        String csvFileName = file.getName();
        
        
        int pos = csvFileName.lastIndexOf(".");
        String txtFileName = csvFileName.substring(0, pos);
       
        
       
        
        File output_directory = new File("output");
         
        if (!output_directory.exists()) {
            output_directory.mkdir();
        }
        
        try {
            
          FileWriter fw_csv = new FileWriter(output_directory+"/"+csvFileName); 
          FileWriter fw_txt = new FileWriter(output_directory+"/"+txtFileName+".txt"); 
          // Query table for desired result
          this.stmt = this.conn.createStatement();
          this.qry = "SELECT * FROM BATCHPROCESS ORDER BY TRACK_ID, TRACK_TIME";
          this.rs = this.stmt.executeQuery(this.qry);
            
          int rowCounter = 1;
          int long_col_num = 1;
          int slice_num = 1;
          
          int newCol = 999999999;
          
                // Add header to top of txt file.
                fw_txt.append('\n');
                
                //Add csv header
                fw_csv.append("ROW ID");
                fw_csv.append(',');
                fw_csv.append("TRACK ID");
                fw_csv.append(',');
                fw_csv.append("TRACK ID GROUP");
                fw_csv.append(',');
                fw_csv.append("ORIGINAL TRACK TIME");
                fw_csv.append(',');
                fw_csv.append("CORRECTED TRACK TIME");
                fw_csv.append(',');
                fw_csv.append("POS X");
                fw_csv.append(',');
                fw_csv.append("POS Y");
                fw_csv.append('\n');
                
       
          
            while(this.rs.next()){
                
                if(rowCounter % this.tick == 0){
                    int cpv =  this.progressBar.getValue();
                    cpv++;
  
                    if(cpv <= 100){
                        this.progressBar.setValue(cpv); 
                    }
                    
                }
                
                if(newCol != 999999999 && newCol != rs.getInt("TRACK_ID") ){
                    long_col_num++;
                    slice_num = 1;
                }
                
                newCol = rs.getInt("TRACK_ID");
                
                
                //tabbed seperated txt file line data
                fw_txt.append(Integer.toString(rowCounter));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(long_col_num));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(slice_num));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(rs.getInt("POS_X_VALUE")));
                fw_txt.append('\t');
                fw_txt.append(Integer.toString(rs.getInt("POS_Y_VALUE")));
                fw_txt.append('\n');
               
                
                //comma seperated csv file line data
                fw_csv.append(Integer.toString(rowCounter));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(rs.getInt("TRACK_ID")));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(long_col_num));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(rs.getInt("TRACK_TIME")));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(slice_num));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(rs.getInt("POS_X_VALUE")));
                fw_csv.append(',');
                fw_csv.append(Integer.toString(rs.getInt("POS_Y_VALUE")));
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
