/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package csvreading;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Maulana
 */
public class MssqlLoader {
    private static DbConnection connect = new DbConnection();
    private static Statement statement;
    private static Connection con;
    private static String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static String koneksi = "jdbc:sqlserver://DESKTOP-00U9NGH\\SQLEXPRESS;" +
            "databaseName=NPT2;user=sa;password=maulana;";
        
     String createtable(String tabel,String namaColumn) throws SQLException{
            String query = "CREATE TABLE [NPT2].[dbo].["+tabel+"] ("+namaColumn+")";
            Statement stmt = null;
            con = connect.getConnection(koneksi, driver);
            stmt = con.createStatement();
            stmt.executeUpdate(query);
            return "sukses";
        }
    
    String InsertionMSSQL(String tabel,String values,String namacolumn) throws SQLException{ 
            PreparedStatement preparedStatement = connect.getConnection(koneksi, driver).prepareStatement("INSERT INTO [NPT2].[dbo].["+tabel+"] ("+namacolumn+") VALUES('"+values+"');");
            if (preparedStatement.executeUpdate() != 1){
                System.out.println("gagal insert ke tabel");
            } else {
                System.out.println("sukses insert ke tabel");
            }
            preparedStatement.close();            
            connect.closeConnection();
            return "sukses";
    }
    
    String tabelExist(String tabel) throws SQLException{
        Statement smt = connect.getConnection(koneksi,driver).createStatement();
        ResultSet resultSet = smt.executeQuery("SELECT COUNT(*) AS jmlhrow FROM NPT2.INFORMATION_SCHEMA.TABLES where TABLE_NAME = '"+tabel+"'");
        String x = null;
        while(resultSet.next()){
        x = resultSet.getString("jmlhrow");
        }
        resultSet.close();
        smt.close();
        return x ;
    }
 
       
}
