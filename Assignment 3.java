import java.util.*;
import java.sql.*;
import java.io.*;

class LimAssignment3 {
  // create reader and writer object to access db
  static Connection r = null;
  static Connection w = null;

  public static void main(String[] args) throws Exception{
    // long startTime = System.nanoTime();
    // get connection properties
    // parameters needed to create connection
    String readParameter = "readerparams.txt";
    String writeParameter = "writerparams.txt";
    if(args.length >= 2){
      readParameter = args[0];
      writeParameter = args[1];
    }
    // store connection properties
    Properties rprops = new Properties();
    Properties wprops = new Properties();
    // initializes properties from file
    rprops.load(new FileInputStream(readParameter));
    wprops.load(new FileInputStream(writeParameter));

    try{
      // Get connection
      // loads jdbc driver class to access db from java
      Class.forName("com.mysql.jdbc.Driver");

      String dburlr = rprops.getProperty("dburl");
      String dburlw = wprops.getProperty("dburl");

      String readUsername = rprops.getProperty("user");
      String writeUsername = wprops.getProperty("user");

      r = DriverManager.getConnection(dburlr, rprops);
      w = DriverManager.getConnection(dburlw, wprops);

      System.out.printf("Reader connection %s %s established.%n", dburlr, readUsername);
      System.out.printf("Writer connection %s %s established.%n", dburlw, writeUsername);

      Scanner in = new Scanner(System.in);

      //////////////////////////////////////////////////////////////////////////
      // GET INDUSTRIES //
      //////////////////////////////////////////////////////////////////////////

      Statement s = r.createStatement();
      ResultSet count = s.executeQuery("Select count(distinct industry) from company;");
      count.next();
      String[] industries = new String[count.getInt(1)];
      System.out.println(industries.length + " industries found.");
      count.close();

      String industryInfo = "SELECT Industry, count(distinct Ticker) as TickerCnt FROM Company NATURAL JOIN PriceVolume GROUP BY Industry ORDER BY Industry, TickerCnt DESC";
      PreparedStatement query = r.prepareStatement(industryInfo);
      ResultSet r1 = query.executeQuery();

      int i = 0;
      while(r1.next()){
        industries[i] = r1.getString(1);
        System.out.printf("%s \n", industries[i]);
        i++;
      }
      System.out.println();
      r1.close();

      //////////////////////////////////////////////////////////////////////////
      // SET UP TABLE //
      //////////////////////////////////////////////////////////////////////////

      PreparedStatement clear = w.prepareStatement("DROP TABLE IF EXISTS Performance;");
      clear.executeUpdate();
      System.out.println("Performance Table Cleared...");

      String table = "CREATE TABLE Performance ("
                        + "Industry CHAR(30), "
                        + "Ticker CHAR(6), "
                        + "StartDate CHAR(10), "
                        + "EndDate CHAR(10), "
                        + "tickerReturn CHAR(12), "
                        + "IndustryReturn CHAR(12)"
                        + ");";
      try{
        PreparedStatement check = w.prepareStatement(table);
        check.executeUpdate();
        System.out.println("Table Setup Complete: Success");
      }
      catch(SQLException ex){
        System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
      }

      System.out.println();

      //////////////////////////////////////////////////////////////////////////
      // PROCESS INDUSTRY //
      //////////////////////////////////////////////////////////////////////////

      i = 0;
      while(i < industries.length){
        System.out.printf("Processing %s\n", industries[i]);
        compareStocks(industries[i]);
        System.out.println();
        i++;
      }

      //////////////////////////////////////////////////////////////////////////
      // END //
      //////////////////////////////////////////////////////////////////////////

      r.close();
      w.close();
      System.out.println("Database Connection Closed.");
      // long endTime = System.nanoTime();
      // System.out.println();
      // System.out.println(((endTime - startTime)/1000000000));
    }
    catch(SQLException ex){
      System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
    }
    return;
  }

  //////////////////////////////////////////////////////////////////////////
  // COMPARE INDUSTRY STOCKS //
  //////////////////////////////////////////////////////////////////////////
  public static void compareStocks(String industry) throws Exception{
    int tickerNum = 0; // number of tickers accepted
    String startDate = "";
    String endDate = "";
    int cd = 0;

    // get first and last date of data from each symbol ticker
    String[] statement = {"select Ticker, min(TransDate) AS start, max(TransDate) AS end, count(distinct TransDate) as TradingDays "
                       + "from Company natural join PriceVolume "
                       + "where Industry = ? ", "group by Ticker "
                       + "having TradingDays >= 150 "
                       + "order by Ticker"};
    PreparedStatement obtaincount = r.prepareStatement(statement[0] + statement[1] + ";");
    obtaincount.setString(1, industry);
    ResultSet countIndustryTickers = obtaincount.executeQuery();

    // get first and last dates that are aligned
    String query1 = "select max(start), min(end) from (" + statement[0] + statement[1] + ") as IndustryInfo;";
    String query2 = "";
    PreparedStatement pstmnt = r.prepareStatement(query1);

    pstmnt.setString(1, industry);

    ResultSet r1 = pstmnt.executeQuery();
    while(r1.next()){
      startDate = r1.getString(1);
      endDate = r1.getString(2);
    }
    r1.close();

    query2 = "and TransDate >= '" + startDate + "' and TransDate <= '" + endDate + "' ";

    // get from database with different restrictions then compare stocks
    PreparedStatement psmnt = r.prepareStatement(statement[0] + query2 + statement[1] + ";");
    psmnt.setString(1, industry);
    ResultSet r2 = psmnt.executeQuery();

    int count = 0;
    int first = 1;
    ArrayList<String> symbols = new ArrayList<String>();
    ArrayList<String> intervalStart = new ArrayList<String>();
    ArrayList<String> intervalEnd = new ArrayList<String>();

    while(r2.next()){
      if(first == 1){
        intervalStart = getIntervalDates(r2.getString(1), r2.getString(2), r2.getString(3), 1);
        intervalEnd = getIntervalDates(r2.getString(1), r2.getString(2), r2.getString(3), 0);
        cd = r2.getInt(4);
        first = 0;
      }
      symbols.add(r2.getString(1));
      count++;
    }
    tickerNum = count;
    r2.close();

    // compare each ticker to rest of industry
    double tickerReturnSum = 0.0;
    int i = 0;
    int j = 0;
    double openPrice;
    double closePrice;
    double[] tickerReturns = new double[tickerNum];
    ArrayList<String> tupleValues = new ArrayList<String>();
    while(i < intervalStart.size() - 1){
      while(j < symbols.size()){
        openPrice = 0.0;
        closePrice = 0.0;
        PreparedStatement query3 = r.prepareStatement("select P.TransDate, P.openPrice, P.closePrice "
                                                          + "from PriceVolume P "
                                                          + "where Ticker = ? and TransDate >= ? "
                                                          + "and TransDate < ?;");

        query3.setString(1, symbols.get(j));
        query3.setString(2, intervalStart.get(i));
        query3.setString(3, intervalStart.get(i+1));

        ResultSet r3 = query3.executeQuery();
        r3.next();
        openPrice = r3.getDouble(2);
        closePrice = r3.getDouble(3);

        while(r3.next()){
          closePrice = r3.getDouble(3);
        }
        r3.close();

        tickerReturns[j] = (closePrice / openPrice) - 1;

        tickerReturnSum += tickerReturns[j];
        j++;
      }

      j = 0;
      double totalReturn;
      while(j < symbols.size()){
        totalReturn = (tickerReturnSum - tickerReturns[j]) / ((double) tickerNum - 1);

        String query4 = "INSERT into Performance(Industry, Ticker, StartDate, EndDate, tickerReturn, IndustryReturn)"
                     + " values (?,?,?,?,?,?);";
        PreparedStatement insert = w.prepareStatement(query4);
        insert.setString(1, industry);
        insert.setString(2, symbols.get(j));
        insert.setString(3, intervalStart.get(i));
        insert.setString(4, intervalEnd.get(i));
        insert.setString(5, String.format("%10.7f", tickerReturns[j]));
        insert.setString(6, String.format("%10.7f", totalReturn));
        insert.execute();
        insert.close();
        j++;
      }

      j = 0;
      i++;
      tickerReturnSum = 0.0;
    }
    if(cd == 0){
      System.out.printf("Insufficient data for %s => no analysis\n", industry);
    }
    else{
      System.out.printf("%d accepted tickers for %s (%s - %s), %d common dates\n",
                             tickerNum,
                             industry,
                             startDate,
                             endDate,
                             cd);
    }
    return;
  }

  //////////////////////////////////////////////////////////////////////////
  // GET INTERVAL DATES //
  //////////////////////////////////////////////////////////////////////////
  public static ArrayList<String> getIntervalDates(String ticker, String startDate, String endDate, int start) throws Exception{
    ArrayList<String> ret = new ArrayList<String>();
    PreparedStatement query = r.prepareStatement("SELECT P.TransDate, P.openPrice, P.closePrice "
                                                      + "From PriceVolume P "
                                                      + "WHERE Ticker = ? AND TransDate >= ? AND TransDate < ?;");
    query.setString(1, ticker);
    query.setString(2, startDate);
    query.setString(3, endDate);

    ResultSet results = query.executeQuery();

    int counter = 0;
    int remainder = (start == 1) ? 0 : 59;

    while(results.next()){
      if(counter % 60 == remainder){
        ret.add(results.getString(1));
      }
      counter++;
    }
    results.close();
    return ret;
  }
}
