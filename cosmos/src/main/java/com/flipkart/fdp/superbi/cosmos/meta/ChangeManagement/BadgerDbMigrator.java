package com.flipkart.fdp.superbi.cosmos.meta.ChangeManagement;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jalaj.kumar on 26/02/15.
 */
public class BadgerDbMigrator {


    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/b1_badger";

    private static final String USER = "root";//fk-bigfoot-read
    private static final String PASS = "";//readall
    private static final String UNDERSCORE = "_";

    private static final Logger LOGGER = LoggerFactory.getLogger(CosmosDBMigrator.class);

    private static HashMap<String,String> deserialisedMap = new HashMap<String, String>();


    private static void deserializeMap()
    {
        try {
            FileInputStream fis = new FileInputStream("mappings.ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            deserialisedMap = (HashMap<String, String>) ois.readObject();
            ois.close();
        }catch(Exception e)
        {
            LOGGER.error("Exception in loading deserializing changemanagement map",e);
        }
    }

    public static int nthOccurrence(String str, char c, int n) {
        int pos = str.indexOf(c, 0);
        while (n-- > 0 && pos != -1)
            pos = str.indexOf(c, pos + 1);
        return pos;
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            System.out.println("Creating statement...");
            stmt = conn.createStatement();


            deserializeMap();


//            for(Map.Entry<String,String> entry:deserialisedMap.entrySet())
//            {
//
//                String mapKey = entry.getKey();
//                String value = entry.getValue();
//                String major = value.split("\\.")[0];
//                String[] name = mapKey.split("_@_");
//                String tab = "dart_"+name[0];
//
//                String tableName = tab+"_"+name[1].replace(".","_");
//                System.out.println(tableName + " " + major);
//
//
//            }
//            System.exit(0);
            HashMap<String, String> idToProcessStringMap = new HashMap<String, String>();
            String sql;
            sql = "select id,process_data from process_data where active=1";
            ResultSet rs = stmt.executeQuery(sql);
            int count=0;
            int tot = 0;
            while(rs.next())
            {
                String id = rs.getString("id");
                String process_data = rs.getString("process_data");
                if(process_data==null) {
                    System.out.println("process data is null for id " + id);
                    continue;
                }
                boolean found = false;
                for(Map.Entry<String,String> entry:deserialisedMap.entrySet())
                {

                    String mapKey = entry.getKey();
                    String value = entry.getValue();
                    String major = value.split("\\.")[0];
                    String[] name = mapKey.split("_@_");
                    String tab = "dart_"+name[0];

                    String tableName = tab+"_"+name[1].replace(".","_");
                    if(process_data.toLowerCase().contains(tableName.toLowerCase())) {
                        //System.out.println(tableName);
                        found=true;
                        process_data = process_data.replaceAll("(?i)"+tableName,tab+"_"+major);

                    }

                }
//                if(!found){
//                    System.out.println("no match found for id "+id);
//                }
               // System.out.println();
                tot++;
                if(found) {
                    count++;
                    String ssql = "update process_data set process_data='" + process_data.replace("'","''") + "' where id=" + id;
                    stmt.addBatch(ssql);
                    System.out.println(ssql);
                }

            }
            stmt.executeBatch();
            System.out.println("Total process data replaced " + count+" in "+tot);
            rs.close();


        }catch(Exception e)
        {
            System.out.println("Exception happened");
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try

        }





    }
}
