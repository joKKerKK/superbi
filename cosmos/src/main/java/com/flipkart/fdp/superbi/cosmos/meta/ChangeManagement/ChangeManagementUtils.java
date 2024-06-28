package com.flipkart.fdp.superbi.cosmos.meta.ChangeManagement;

import com.flipkart.fdp.superbi.cosmos.meta.BigzillaConf;
import com.flipkart.fdp.superbi.cosmos.meta.HttpClientInitializer;
import com.flipkart.fdp.superbi.cosmos.meta.api.DartEntityResource;
import com.flipkart.fdp.superbi.cosmos.meta.db.DatabaseConfiguration;
import com.flipkart.fdp.superbi.cosmos.meta.db.SessionFactoryInitializer;
import com.google.common.collect.Maps;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jalaj.kumar on 16/02/15.
 * This is just a utility class to get existing map of major minor versions
 * This is supposed to be used as a standalone entity.
 *
 * Test class to get mapping of major minor version from production DB
 */
public class ChangeManagementUtils {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    //static final String DB_URL = "jdbc:mysql://bigfoot-db.ch.flipkart.com/cosmos";//bigfoot-db.ch.flipkart.com
    static final String DB_URL = "jdbc:mysql://localhost/cosmos";


   // static final String USER = "fk-bigfoot-read";//fk-bigfoot-read
    //static final String PASS = "readall";//readall

    static final String USER = "root";//fk-bigfoot-read
    static final String PASS = "";//readall

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagementUtils.class);

    public static void detectMajorMinorInDB()
    {



    }



    public static void createSerialisedMap(HashMap<String,String> map) {
        try
        {
            FileOutputStream fos = new FileOutputStream("mappings.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            oos.close();
            fos.close();
            System.out.printf("Serialized HashMap data is saved in mappings.ser");


            HashMap<String,String> changeManagementMap = new HashMap<String, String>();
            FileInputStream fis = new FileInputStream("mappings.ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            changeManagementMap = (HashMap<String, String>) ois.readObject();
            ois.close();
            System.out.println(changeManagementMap);

        }catch(Exception ioe)
        {
            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{

            Class.forName("com.mysql.jdbc.Driver");


            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);


            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "select org.name as orgName ,company.name as companyName,namespace.name as namespaceName,seraph_schema.name as entName,seraph_schema.dart_entity_id as dart_entity_id,seraph_schema.dart_version as dart_version from seraph_schema join company on seraph_schema.company_id=company.id  join namespace on seraph_schema.namespace_id=namespace.id join org on namespace.org_id=org.id where seraph_schema.dart_entity_id is not null and seraph_schema.dart_version is not null group by seraph_schema.name,seraph_schema.dart_entity_id,seraph_schema.dart_version order by seraph_schema.dart_entity_id,seraph_schema.dart_version ";
            ResultSet rs = stmt.executeQuery(sql);

            Map<String,Set<String>> entityToVersionMap = new HashMap<String, Set<String>>();
            while(rs.next()){

                String name  = rs.getString("entName");
                String dart_entity_id = rs.getString("dart_entity_id");
                String dart_version = rs.getString("dart_version");
                if(!entityToVersionMap.containsKey(dart_entity_id+":"+rs.getString("entName")+":"+rs.getString("orgName")+":"+rs.getString("namespaceName")
                        +":"+rs.getString("companyName")))
                {
                    Set<String> list = new HashSet<String>();
                    list.add(dart_version);
                    entityToVersionMap.put(dart_entity_id+":"+rs.getString("entName")+":"+rs.getString("orgName")+":"+rs.getString("namespaceName")
                            +":"+rs.getString("companyName")
                            ,list);
                }else{
                    entityToVersionMap.get(dart_entity_id+":"+rs.getString("entName")+":"+rs.getString("orgName")+":"+rs.getString("namespaceName")
                            +":"+rs.getString("companyName")).add(dart_version);
                }
            }
            rs.close();
            stmt.close();
            conn.close();
            System.out.println(entityToVersionMap);

           // System.exit(0);
            DatabaseConfiguration configuration = new DatabaseConfiguration();
            configuration.setDriverClass("com.mysql.jdbc.Driver");
//            configuration.setUrl("jdbc:mysql://bigfoot-db.ch.flipkart.com:3306/cosmos");
//            configuration.setUser("fk-bigfoot-read");
//            configuration.setPassword("readall");


            configuration.setUrl("jdbc:mysql://localhost:3306/cosmos");
            configuration.setUser("root");
            configuration.setPassword("");


            Map<String,String> properties = Maps.newHashMap();
            properties.put("dialect","org.hibernate.dialect.HSQLDialect");
            properties.put("hibernate.show_sql","true");
            properties.put("hibernate.hbm2ddl.auto","update");
            configuration.setProperties(properties);
            SessionFactoryInitializer initializer = new SessionFactoryInitializer(configuration);
            SessionFactory sessionFactory = initializer.getSessionFactory();

            final BigzillaConf bigzilla_conf= new BigzillaConf();
            bigzilla_conf.setBigzillaHost(new BigzillaConf.BigzillaHost("bigzilla.ch.flipkart.com", 28231));
            bigzilla_conf.setDefaultRuleRollingWindowMins(5);
            bigzilla_conf.setDefaultRuleThreshold(2);
            bigzilla_conf.selfHost = "localhost";
            bigzilla_conf.selfPort = 9000;

            DartEntityResource dartEntityResource = new DartEntityResource(
                    sessionFactory, HttpClientInitializer.getInstance(), bigzilla_conf, "stage-pf2.stage.ch.flipkart.com", 28223);

            //String eee = dartEntityResource.getEntityChangeTypeBetweenVersions("fkint","cp","user","Address","5.0","5.1");
            //System.out.println(eee);
           // Address:cp:user:fkint=[3.0@4.0, 1.0, 5.0, 2.0, 5.1]
           // System.exit(0);

            HashMap<String,String> finalAns  = new HashMap<String, String>();
            Map<String,List<String>> ans= new HashMap<String,List<String>>();
            int major=0;
            int minor=0;
            Writer writer = null;
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("filename.txt"), "utf-8"));


            Writer writer2 = null;
            writer2 = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("filename2.txt"), "utf-8"));

            Writer writer3 = null;
            writer3 = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("filename3.txt"), "utf-8"));

            Map<String,String> majorVersionEntities= new HashMap<String,String>();
            for(Map.Entry<String,Set<String>> e :entityToVersionMap.entrySet() ) {

                String dart_id = e.getKey();
                final Set<String> dartVersionss = e.getValue();
                final List<String> dartVersions = new ArrayList<String>();
                dartVersions.addAll(dartVersionss);
                Collections.sort(dartVersions, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                       int part1 = Integer.parseInt(o1.split("\\.")[1]);
                        int part1_0 = Integer.parseInt(o1.split("\\.")[0]);
                        int part2 = Integer.parseInt(o2.split("\\.")[1]);
                        int part2_0 = Integer.parseInt(o2.split("\\.")[0]);
                        if(part1_0>part2_0)
                        {
                            return 1;
                        }
                        else if(part1_0<part2_0){
                            return -1;
                        }
                        else if(part1<part2){
                            return -1;
                        }else if(part1>part2)
                        {
                            return 1;
                        }
                        return 0;
                    }
                });

                ans.put(dart_id, new ArrayList<String>() {{
                    add(dartVersions.get(0));}});
                int size = dartVersions.size();
                for(int i=1;i<size;i++)
                {
                    System.out.println("calling for " +dart_id.split(":")[0] +" "+ dart_id.split(":")[4] + " " + dart_id.split(":")[2] + " " + dart_id.split(":")[3] + " " + dart_id.split(":")[1] + " " + dartVersions.get(i - 1) + " " + dartVersions.get(i));
                    if(dart_id.split(":")[0].equals("4"))
                    {
                        System.out.println("Skipping");
                        continue;
                    }
                    String ee = dartEntityResource.getEntityChangeTypeBetweenVersions(dart_id.split(":")[4], dart_id.split(":")[2], dart_id.split(":")[3], dart_id.split(":")[1], dartVersions.get(i-1), dartVersions.get(i));
                    if(ee.compareToIgnoreCase("minor")==0)
                    {
                        String versions = ans.get(dart_id).get(ans.get(dart_id).size()-1)+"@"+dartVersions.get(i);
                        List<String> str = ans.get(dart_id);
                        str.set(str.size()-1,versions);
                        minor++;
                    }
                    else
                    {
                        major++;
                        ans.get(dart_id).add(dartVersions.get(i));
                    }
                }
               // System.out.println(ans);

                String[] keyArray = dart_id.split(":");
                String hashMapKey = keyArray[4]+"_"+keyArray[2]+"_"+keyArray[3]+"_"+keyArray[1]+"_@_";
                if(ans.get(dart_id).size()>1) {
                    if(!majorVersionEntities.containsKey(keyArray[4]+"_"+keyArray[2]+"_"+keyArray[3]+"_"+keyArray[1])) {
                        writer3.write(keyArray[4] + "_" + keyArray[2] + "_" + keyArray[3] + "_" + keyArray[1]);
                        writer3.write("\n");
                        majorVersionEntities.put(keyArray[4] + "_" + keyArray[2] + "_" + keyArray[3] + "_" + keyArray[1],"put");
                    }
                }
                Double newerVersionForEntity = Double.parseDouble(ans.get(dart_id).get(ans.get(dart_id).size() - 1).split("@")[ans.get(dart_id).get(ans.get(dart_id).size() - 1).split("@").length - 1]);
                Double vv = newerVersionForEntity+1;;
                Integer versionStart = vv.intValue();
                Integer precision = 0;
                List<String> list =  ans.get(dart_id);
                Integer vvv = versionStart;
                for(String s: list)
                {
                    String[] ss = s.split("@");
                    for(String sss:ss)
                    {
                        finalAns.put(hashMapKey+sss,String.valueOf(vvv+"."+precision));
                        System.out.println("New line is " + hashMapKey + sss + " " + String.valueOf(vvv + precision));
                        writer2.write(hashMapKey + sss + " " + String.valueOf(vvv + "."+precision));
                        writer2.write("\n");
                        //if(precision<)
                        precision+=1;
                    }
                    precision=0;
                    vvv+=1;
                }


                writer.write(dart_id + " " + ans.get(dart_id).toString());
                writer.write("\n");

            }
            System.out.println(ans);
            System.out.println(major+" "+minor);
            createSerialisedMap(finalAns);
            writer.close();
            writer2.close();
            writer3.close();

        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
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
        }//end try
        System.out.println("Completed");
    }
}
