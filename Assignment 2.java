import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.*;
import java.util.*;
import java.math.*;

class LimAssignment2 {

    // create connection object to access db
    static Connection conn = null;

    public static void main(String[] args) throws Exception {
        // Get connection properties
        // parameters needed to create connection
        String paramsFile = "ConnectionParameters.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }
        // store connection properties
        Properties connectprops = new Properties();
        // initializes properties from file
        connectprops.load(new FileInputStream(paramsFile));

        try {
            // Get connection
            // loads jdbc driver class to access db from java
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            // request connection object from driver manager
            // login taken from properties
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n", dburl, username);

            showCompanies();

            // Enter Ticker and TransDate, Fetch data for that ticker and date
            Scanner in = new Scanner(System.in);
            while (true) {
                System.out.print("Enter ticker and start date (YYYY.MM.DD) and end date (YYYY.MM.DD): ");
                String[] data = in.nextLine().trim().split("\\s+");
                if (data.length < 1 || data.length == 2){
                  break;
                }
                if(data.length == 1){
                  showTickerDay(data[0], null, null);
                }
                else if(data.length == 3){
                  showTickerDay(data[0], data[1], data[2]);
                }
            }

            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }

    static void showCompanies() throws SQLException {
        // Create and execute a query
        // request statement object from connnection object
        Statement stmt = conn.createStatement();
        // execute sql query
        // goes thru string
        ResultSet results = stmt.executeQuery("select Ticker, Name from Company");

        // Show results
        while (results.next()) {
            System.out.printf("%5s %s%n", results.getString("Ticker"), results.getString("Name"));
        }
        stmt.close();
    }

    static void showTickerDay(String ticker, String startDate, String endDate) throws SQLException {

        // get name
        PreparedStatement company = conn.prepareStatement(
          "select Name from Company where Ticker = ?"
        );
        company.setString(1, ticker);
        ResultSet c = company.executeQuery();
        if(c.next()){
          System.out.printf("%s\n", c.getString(1));
        }
        else{
          System.out.printf("%s not found in database.\n\n", ticker);
          return;
        }
        c.close();

        // get info from price volume
        ResultSet pv;
        if(startDate != null){
          PreparedStatement priceVolume = conn.prepareStatement(
            "select * from PriceVolume where Ticker = ? and TransDate >= ? and TransDate <= ? order by TransDate DESC"
          );
          priceVolume.setString(1, ticker);
          priceVolume.setString(2, startDate);
          priceVolume.setString(3, endDate);
          pv = priceVolume.executeQuery();
        }
        else{
          PreparedStatement priceVolume = conn.prepareStatement(
            "select * from PriceVolume where Ticker = ? order by TransDate DESC"
          );
          priceVolume.setString(1, ticker);
          pv = priceVolume.executeQuery();
        }

        // read and store data
        ArrayList<String[]> values = new ArrayList<String[]>();
        while(pv.next()){
          String[] val = new String[8];

          val[0] = pv.getString(1);
          val[1] = pv.getString(2);
          val[2] = Double.toString(pv.getDouble(3));
          val[3] = Double.toString(pv.getDouble(4));
          val[4] = Double.toString(pv.getDouble(5));
          val[5] = Double.toString(pv.getDouble(6));
          val[6] = Integer.toString(pv.getInt(7));
          val[7] = Double.toString(pv.getDouble(8));

          values.add(val);
        }
        pv.close();

        // find stock splits
        double div = 1.0; // adjusted divisor
        int totalSplits = 0; // total splits for ticker
        double nextOP = Double.parseDouble(values.get(0)[2]); // next open price

        int len = values.size();
        int i = 1;

        while(i < len){
          String[] array = values.get(i);
          double closePrice = Double.parseDouble(array[5]);
          double split = closePrice / nextOP;
          String date = array[1];

          // 2:1 split
          if(Math.abs(split - 2.0) < 0.20){
            System.out.print("2:1 split on " + date + " ");
            System.out.printf("%.2f --> %.2f\n", closePrice, nextOP);
            div *= 2.0;
            totalSplits++;
          }
          // 3:1 split
          else if(Math.abs(split - 3.0) < 0.30){
            System.out.print("3:1 split on " + date + " ");
            System.out.printf("%.2f --> %.2f\n", closePrice, nextOP);
            div *= 3.0;
            totalSplits++;
          }
          // 3:1 split
          else if(Math.abs(split - 1.5) < 0.15){
            System.out.print("3:2 split on " + date + " ");
            System.out.printf("%.2f --> %.2f\n", closePrice, nextOP);
            div *= 1.5;
            totalSplits++;
          }

          // set next open price
          nextOP = Double.parseDouble(array[2]);

          // price data divided
          array[2] = Double.toString(Double.parseDouble(array[2]) / div);
          array[3] = Double.toString(Double.parseDouble(array[3]) / div);
          array[4] = Double.toString(Double.parseDouble(array[4]) / div);
          array[5] = Double.toString(Double.parseDouble(array[5]) / div);

          i++;
        }
        System.out.printf("%d splits in %d trading days\n\n", totalSplits, len);

        int shares = 0;
        double fiftySum = 0.0;
        double profit = 0.0;
        double trans = 0;
        String[] firstArr = values.get(0);

        // for 50 days or more
        if(len > 50){

          i = len - 50;

          // calculate fifty sum
          while(i < len){
            String[] array = values.get(i);
            double closePrice = Double.parseDouble(array[5]);

            fiftySum += closePrice;
            i++;
          }

          // trading transactions
          for(int j = len - 51; j > 0; j--){

            String[] array = values.get(j);
            String[] nextArr = values.get(j+1);
            String[] lastArr = values.get(j-1);
            String[] fiftyArr = values.get(j+50);

            double fiftyMA = fiftySum / 50.0; // moving average

            double lastOpen = Double.parseDouble(lastArr[2]);
            double openPrice = Double.parseDouble(array[2]);
            double closePrice = Double.parseDouble(array[5]);
            double nextClose = Double.parseDouble(nextArr[5]);

            double closeOpen = closePrice / openPrice;
            double nextOC = openPrice / nextClose;

            // buy criterion
            if(fiftyMA > closePrice && closeOpen < 0.97000001){
              shares += 100;
              profit -= 100 * lastOpen;
              profit -= 8.00;
              trans++;
            }
            // sell criterion
            else if(shares >= 100 && fiftyMA < openPrice && nextOC > 1.00999999){
              shares -= 100;
              profit += 100 * ((openPrice + closePrice) / 2);
              profit -= 8.00;
              trans++;
            }
            fiftySum += closePrice;
            fiftySum -= Double.parseDouble(fiftyArr[5]);
          }
          profit += shares * Double.parseDouble(firstArr[2]);
        }
        System.out.println("Executing Investment Strategy");
        System.out.printf("Transactions executed: %d\n", Math.round(trans));
        System.out.printf("Net Cash: %.2f\n\n", profit);
    }
}
