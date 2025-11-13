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
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

public class Main {
  public static void main(String[] args) throws Exception {
    arrowFlightPath();
  }

  private static void arrowFlightPath() throws Exception {
    String connection = "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false";
    String query = "select 2";
    runQuery(connection, query);
  }

  private static void runQuery(String connection, String query, String... params) throws Exception {
    String user = "";
    String token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjljMzY5MDdiODY0YTE2MGI4YjlmNmE2YmFkODQ5Y2Y2NWI4Njk0ZTEiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiJhYmMwMWZlNy1lNThmLTQwYTMtOTEyNS0yMGRkZDQyMTk1OGUiLCJhdWQiOiJjOTZlMjk4NS02NmIxLTRmNDUtYjc1MC0zMTJmMzJhOWQ1MWQiLCJpc3MiOiJodHRwczovL2FwcC5kcmVtaW8uY29tIiwidHlwZSI6InVpIiwiZXhwIjoxNzYzMDEwMTY4LCJpYXQiOjE3NjI5ODEzNjgsImp0aSI6ImU1NTdjMmY0LTExZTAtNDQxMS1hMDU4LWMyYTNhNDBkOTM2MyIsInVzZXJuYW1lIjoiZGNzdGVzdEBkcmVtaW8uY29tIiwic2lkIjoiOTU3YjY3NjQtNmQzZS00NGIyLTg4MGMtZWJkMmRiZDk3NWY4In0.kuPvu1D-D4oPaY1P54Nh9LvszA2aKLKpkRN4sYJVjjzggBL5t2lbg2MGjY4HGBjIM37vL5jXqbQ5Za61_N9m2Wg9KORpKl1_Ugqvio8-_M_aUmQuots0PC6uuqli5c5Qk3Zs4giAP7UYh1yKGlsufO1dNRwL6uUayweguYeuOBR3UZFQXaEmEYQx_sOf3KFE_jN19lV9dp47LyKqlWnC5DRFdJrN8FvNZiv4qTdQQVxH7w9qAd5EGQlNHZvlUhQcA1z_aOht_294jxNwGZ1ueyLGoT4E0BBl4174EvC-5TbRnUiJrbeEJ8cpvgG7y0fpRutmlhiAm2_145L5g_n99Q";

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("token", token);

    try (Connection con = DriverManager.getConnection(connection, props);
         Statement s = con.createStatement()) {

      try (ResultSet rs = s.executeQuery(query)) {
        System.out.println(getResultsAsJson(rs));
      }
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
