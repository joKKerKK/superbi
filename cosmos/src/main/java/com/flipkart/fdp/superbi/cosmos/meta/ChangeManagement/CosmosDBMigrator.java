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
 * Created by jalaj.kumar on 24/02/15.
 */
public class CosmosDBMigrator {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/cosmos";

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
            sql = "select seraph_schema.id,org.name,company.name,namespace.name,seraph_schema.name,seraph_schema.dart_version from seraph_schema join company on seraph_schema.company_id=company.id  join namespace on seraph_schema.namespace_id=namespace.id join org on namespace.org_id=org.id where seraph_schema.dart_entity_id is not null and seraph_schema.dart_version is not null group by seraph_schema.name,seraph_schema.dart_entity_id,seraph_schema.dart_version order by seraph_schema.dart_entity_id,seraph_schema.dart_version ;";
            ResultSet rs = stmt.executeQuery(sql);
            Map<String,String> desMapKeyToSeraphIdMap = new HashMap<String, String>();
            while(rs.next())
            {
                String seraph_id  = rs.getString("seraph_schema.id");

                //c_o_nspc_name
                String hashMapKey = rs.getString("company.name")+UNDERSCORE+rs.getString("org.name")+UNDERSCORE+
                        rs.getString("namespace.name")+UNDERSCORE+rs.getString("seraph_schema.name")+"_@_"+rs.getString("seraph_schema.dart_version");
                desMapKeyToSeraphIdMap.put(hashMapKey,seraph_id);

            }
            deserializeMap();
            System.out.println(deserialisedMap);
            for(Map.Entry<String,String> entry : deserialisedMap.entrySet())
            {
                String key = entry.getKey();

                String id = desMapKeyToSeraphIdMap.get(key);
                if(id==null)
                {
                    System.out.println(key+ " this entity is not found in cosmos db but is present in dart");
                    continue;
                }
                String query = "update seraph_schema set version='"+entry.getValue()+"' where id="+id;
                System.out.println(query);
                stmt.addBatch(query);
            }

            stmt.executeBatch();

            stmt.close();
            conn.close();

        }catch(Exception e)
        {
           System.out.println("Exception occured");
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
