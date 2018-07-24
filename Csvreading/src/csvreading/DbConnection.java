/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package csvreading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author Maulana
 */
public class DbConnection {
    Connection connection = null;
 
    public Connection getConnection(String koneksi,String driver) throws SQLException{
        try {
            Class.forName(driver);
            if(connection == null)
                connection = (Connection) DriverManager.getConnection(koneksi);
 
        } catch (ClassNotFoundException e) {
 
            e.printStackTrace();
             
        } catch (SQLException e) {
             
            e.printStackTrace();
             
        }
        return connection;
    }
    
    public void closeConnection(){
        try {
              if (connection != null) {
                  connection.close();
              }
            } catch (Exception e) { 
                //do nothing
            }
    }       
}
