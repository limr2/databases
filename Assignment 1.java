import java.io.*;
import java.util.*;

public class LimAssignment1 {

  public static ArrayList<String[]> crazyDays = new ArrayList<String[]>();
  public static ArrayList<String[]> stockSplits = new ArrayList<String[]>();

  // finds all crazy days and stock splits
  public static void calculate(String date, String highPrice, String lowPrice, String openPrice, String closePrice){

    String[] crazyDayInfo = new String[2];
    String[] stockSplitInfo = new String[4];

    float high = Float.parseFloat(highPrice);
    float low = Float.parseFloat(lowPrice);
    float percent = (high - low) / high;

    float open = Float.parseFloat(openPrice);
    float close = Float.parseFloat(closePrice);
    float ratio = close / open;

    // if crazy day, add to arraylist
    if(percent >= 0.15){
      crazyDayInfo[0] = date;
      percent = percent * 100;
      percent = Math.round(percent * 100f) / 100f;
      crazyDayInfo[1] = Float.toString(percent);
      crazyDays.add(crazyDayInfo);
    }

    // if stock split, add to arraylist
    if(Math.abs(ratio - 2) < 0.05){
      stockSplitInfo[0] = "2:1";
      stockSplitInfo[1] = date;
      stockSplitInfo[2] = closePrice;
      stockSplitInfo[3] = openPrice;
      stockSplits.add(stockSplitInfo);
    }
    else if(Math.abs(ratio - 3) < 0.05){
      stockSplitInfo[0] = "3:1";
      stockSplitInfo[1] = date;
      stockSplitInfo[2] = closePrice;
      stockSplitInfo[3] = openPrice;
      stockSplits.add(stockSplitInfo);
    }
    else if(Math.abs(ratio - 1.5) < 0.05){
      stockSplitInfo[0] = "3:2";
      stockSplitInfo[1] = date;
      stockSplitInfo[2] = closePrice;
      stockSplitInfo[3] = openPrice;
      stockSplits.add(stockSplitInfo);
    }
    return;
  }

  public static void printInfos(){

    // if arraylist is not empty, print info
    if(!(crazyDays.isEmpty())){

      // print each crazy day
      crazyDays.forEach((i) -> System.out.println("Crazy day: " + i[0] + "  " + i[1]));

      System.out.println("Total crazy days = " + Integer.toString(crazyDays.size()));

      // if more than one crazy day, find max
      if(crazyDays.size() > 1){
        // calculate craziest day
        String craziestDay = "";
        float craziestPer = 15;
        for(int i = 0; i < crazyDays.size(); i++){
          String tempString = crazyDays.get(i)[0];
          Float tempFloat = Float.parseFloat(crazyDays.get(i)[1]);
          if(tempFloat > craziestPer){
            craziestDay = tempString;
            craziestPer = tempFloat;
          }
        }
        System.out.println("The craziest day: " + craziestDay + "  " + Float.toString(craziestPer));
      }

      System.out.println("");

      // clear arraylists
      crazyDays.clear();
    }
    else{
      System.out.println("Total crazy days = 0");
      System.out.println("");
    }
    if(!(stockSplits.isEmpty())){

      // print each stock split
      stockSplits.forEach((i) -> System.out.println(i[0] + " split on " + i[1] + "  " + i[2] + " --> " + i[3]));

      System.out.println("Total number of splits: " + Integer.toString(stockSplits.size()));
      System.out.println("");

      // clear arraylists
      stockSplits.clear();
    }
    else{
      System.out.println("Total number of splits: 0");
      System.out.println("");
    }
    return;
  }

  public static void main(String args[]){

    String fileName = "Stockmarket-1990-2015.txt";
    String line = null;
    String[] parts;
    String lastCompany = "x";

    try{

      FileReader fileReader = new FileReader(fileName);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String lastOpenPrice = "";
      int check = 0;

      while((line = bufferedReader.readLine()) != null){
        parts = line.split("\t");

        // if new company, initialize
        if(!(parts[0].equals(lastCompany))){

          // initialize
          lastCompany = parts[0];
          // if first one, just compare to itself
          lastOpenPrice = parts[5];

          // print info for last company
          if(check == 1){
            printInfos();
          }

          // print new company info
          System.out.println("Processing " + lastCompany);
          System.out.println("======================");
        }

        // do calculations
        calculate(parts[1], parts[3], parts[4], lastOpenPrice, parts[5]);
        lastOpenPrice = parts[2];
        check = 1;
      }
      bufferedReader.close();

      // print for final company
      printInfos();

    }
    catch(FileNotFoundException ex){
      System.out.println("Unable to open file '" + fileName + "'");
    }
    catch(IOException ex){
      System.out.println("Error reading file '" + fileName + "'");
    }
  }
}
