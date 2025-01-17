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
 * Created by jalaj.kumar on 23/02/15.
 * This is a utility migrator Class ot migrate Dart.ingestion_def table
 * according to new schema versions generated by ChangeManagementUtils class .
 *
 */
public class DartDBMigrator {


    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/dart";

    private static final String USER = "root";//fk-bigfoot-read
    private static final String PASS = "";//readall

    private static final Logger LOGGER = LoggerFactory.getLogger(DartDBMigrator.class);

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

    public static void main(String[] args) {

        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            System.out.println("Creating statement...");
            stmt = conn.createStatement();

            String sql;
            sql = "select company,org,namespace,name ,id from ingestion_entity";
            ResultSet rs = stmt.executeQuery(sql);
            HashMap<String,String> entityToIdMap = new HashMap<String, String>();
            while(rs.next()){
                String company  = rs.getString("company");
                String org = rs.getString("org");
                String namespace = rs.getString("namespace");
                String name = rs.getString("name");
                String id = rs.getString("id");
                entityToIdMap.put(company+"_"+org+"_"+namespace+"_"+name,id);
            }
            System.out.println(entityToIdMap);
            rs.close();


            String checkIfColumnAddedQuery = "select 1 from information_schema.columns where TABLE_NAME='ingestion_def' and TABLE_SCHEMA='dart' and COLUMN_NAME='old_version'";
            ResultSet res = stmt.executeQuery(checkIfColumnAddedQuery);
            boolean added = false;
            while(res.next())
            {
                added = true;
            }
            System.out.println(added);
            if(!added)
            {
                String addColumnQuery = "alter table ingestion_def add column old_version varchar(63) default null ;";
                stmt.executeUpdate(addColumnQuery);
                System.out.println("Column older_version added");
            }else {
                System.out.println("Column older_version already exists");
            }

            deserializeMap();
           // List<String> updateQueries = new ArrayList<String>();

            System.out.println(deserialisedMap);
            for(Map.Entry<String,String> entry : deserialisedMap.entrySet())
            {
                String key = entry.getKey();
                String c_o_nm_name = key.split("_@_")[0];
                String old_version= key.split("_@_")[1];
               // System.out.println(key);
                String id = entityToIdMap.get(c_o_nm_name);
                if(id==null)
                {
                    System.out.println(c_o_nm_name+ " this entity is not found in dart db but is present in cosmos with valid dart_version "+ old_version);
                    continue;
                }
                //System.out.println(id);
                String query = "update ingestion_def set old_version='"+entry.getValue()+"' where ingestion_id="+id+" and version='"+old_version+"'";
               // updateQueries.add(query);
                System.out.println(key+" "+query);
                stmt.addBatch(query);
            }
            stmt.executeBatch();

            //Add code to swap columns  - better do it manually after verifying.

            stmt.close();
            conn.close();


        }catch(Throwable e)
        {
            System.out.println("Some exception happened");
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
        }//e







    }
}
