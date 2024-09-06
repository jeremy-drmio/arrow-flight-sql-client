package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

public class Main {
  public static void main(String[] args) throws Exception {

    Properties props = new Properties();
    props.setProperty("user", "dremio");
    props.setProperty("password", "dremio123");

    String connectionStr = "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false";
    String query = "select * from Samples.\"samples.dremio.com\".\"zips.json\" limit 5";

    try (Connection con = DriverManager.getConnection(connectionStr, props);
        PreparedStatement ps = con.prepareStatement(query);
        ResultSet rs = ps.executeQuery()) {

      System.out.println(getResultsAsJson(rs));
    }
  }

  private static String getResultsAsJson(ResultSet rs) throws Exception {
    ResultSetMetaData md = rs.getMetaData();
    int numCols = md.getColumnCount();
    List<String> colNames =
        IntStream.range(0, numCols)
            .mapToObj(
                i -> {
                  try {
                    return md.getColumnName(i + 1);
                  } catch (SQLException e) {
                    e.printStackTrace();
                    return "?";
                  }
                })
            .toList();

    JsonArray result = new JsonArray();
    while (rs.next()) {
      JsonObject row = new JsonObject();
      colNames.forEach(
          cn -> {
            try {
              row.addProperty(cn, rs.getObject(cn).toString());
            } catch (SQLException e) {
              e.printStackTrace();
            }
          });
      result.add(row);
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(result);
  }
}
