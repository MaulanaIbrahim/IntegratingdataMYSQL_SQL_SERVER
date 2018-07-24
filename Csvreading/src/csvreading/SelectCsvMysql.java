/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package csvreading;


import com.mysql.jdbc.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Maulana
 */
public class SelectCsvMysql {
    private static DbConnection connect = new DbConnection(); 
    private static String driver = "com.mysql.jdbc.Driver";
    private static String koneksi = "jdbc:mysql://10.2.124.195/STYLO_E_2GR?user=dbstylo&password=stylo2018#";
    public static MssqlLoader mssql = new MssqlLoader();

    public static void main(String[] args) throws FileNotFoundException{
            
            ArrayList<String> valueCsvCol = new ArrayList<String>();
            ArrayList<String> typeCol = new ArrayList<String>();
            int i;
            Scanner scanner = new Scanner(new File("G:\\tugas kuliah\\tingkat 2\\Semester 4\\CV\\kerja_praktek\\csv_sample_data.csv"));
            //scanner.useDelimiter(",");
            while(scanner.hasNext()){    
                    valueCsvCol.add(scanner.nextLine());
             }
                
            for(i = 0;i<valueCsvCol.size();i++){
                String[] Hasil = valueCsvCol.get(i).split(",");
                String dbName = Hasil[1];
                String tabelName = Hasil[5];
                typeCol = getTypeColumn(tabelName,dbName);
                String typecolumn = String.join(",",typeCol);
                try {
                    mssql.createtable(tabelName+"_"+tabelName, typecolumn);
                } catch (SQLException ex) {
                    Logger.getLogger(SelectCsvMysql.class.getName()).log(Level.SEVERE, null, ex);
                }
                
            }

        }
        
        
        public static ArrayList<String> getTypeColumn(String tabelName,String dbName){
            ArrayList<String> typeColumn = new ArrayList<String>();  
            String dataType;
            try{
                
                    Statement statement = connect.getConnection(koneksi,driver).createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT COLUMN_NAME,COLUMN_TYPE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION "
                                        + "FROM INFORMATION_SCHEMA.COLUMNS"
                                        + "where table_name='"+tabelName+"' and TABLE_SCHEMA='"+dbName+"'");
                    while(resultSet.next()){ 
                        if(resultSet.getString("DATA_TYPE").equals("int") || resultSet.getString("DATA_TYPE").equals("INT") ){
                            dataType = "BIGINT";
                        }else if(resultSet.getInt("CHARACTER_MAXIMUM_LENGTH")>8000){
                            dataType = "VARCHAR (MAX)";
                        }else if(resultSet.getString("DATA_TYPE").equals("varchar") || resultSet.getString("DATA_TYPE").equals("VARCHAR") ){
                            dataType = "VARCHAR ("+resultSet.getString("CHARACTER_MAXIMUM_LENGTH")+")";
                        }else if(resultSet.getString("DATA_TYPE").equals("datetime") || resultSet.getString("DATA_TYPE").equals("DATETIME") ){
                            dataType = "datetime2";
                        }else if(resultSet.getString("DATA_TYPE").equals("timestamp") || resultSet.getString("DATA_TYPE").equals("TIMESTAMP") ){
                            dataType = "smalldatetime";
                        }else{
                            dataType = resultSet.getString("DATA_TYPE");
                        }
                        typeColumn.add(resultSet.getString("COLUMN_NAME")+" "+dataType);
                    }
                    
                }catch(Exception e){
                   
                }
            return typeColumn;
        }
}
