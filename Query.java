import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.*;

/**
 * Runs queries against a back-end database
 */
public class Query
{
  private String configFilename;
  private Properties configProps = new Properties();

  private String jSQLDriver;
  private String jSQLUrl;
  private String jSQLUser;
  private String jSQLPassword;

  // DB Connection
  private Connection conn;

  // Logged In User
  private String username; // customer username is unique
  private int reservationId = 1;
  // List of Itinaries
  // Canned queries

  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  private static final String LOGIN_SEARCH_SQL = "SELECT username, password FROM Users WHERE username=? AND password=?";
  private PreparedStatement loginSearchStatement;

  private static final String CREATE_SEARCH_SQL = "SELECT * FROM Users WHERE username=?";
  private PreparedStatement createSearchStatement;

  private static final String INSERT_USER_SQL = "INSERT INTO Users VALUES(?,?,?)";
  private PreparedStatement insertUserStatement;
  
  private static final String DIRECT_SEARCH_SQL = "SELECT TOP (?) fid,day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price FROM Flights WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND actual_time > 0 ORDER BY actual_time ASC,fid";
  private PreparedStatement directSearchStatement;                                                  

  private static final String INDIRECT_SEARCH_SQL = "SELECT TOP (?) F1.fid AS id1, F2.fid AS id2, F1.actual_time AS t1, F2.actual_time AS t2, F1.day_of_month AS day_of_month, F1.carrier_id AS carrier_id1, F2.carrier_id AS carrier_id2, F1.flight_num AS flight_num1, F2.flight_num AS flight_num2, F1.origin_city AS origin_city1, F2.origin_city AS origin_city2, F1.dest_city AS dest_city1, F2.dest_city AS dest_city2, F1.capacity AS capacity1, F2.capacity AS capacity2, F1.price AS price1, F2.price AS price2 FROM Flights AS F1, Flights AS F2 WHERE F1.origin_city = ? AND F1.dest_city = f2.origin_city AND F2.dest_city = ? AND F1.day_of_month = F2.day_of_month AND F1.day_of_month = ? AND F1.actual_time > 0 AND F2.actual_time > 0 ORDER BY (F1.actual_time + F2.actual_time)";
  private PreparedStatement indirectSearchStatement;

  private static final String RESERVATION_INSERT_SQL = "INSERT INTO Reservations VALUES(?,?,?,?,?,?,?)";
  private PreparedStatement reservationInsertStatement;

  private static final String RESERVATION_CHECK_SQL = "SELECT day,rev_id,it_id,paid,fid1,fid2 FROM Reservations WHERE username = ?";
  private PreparedStatement reservationCheckStatement;

  private static final String RESERVATION_GET_ID = "SELECT TOP 1 rev_id FROM Reservations ORDER BY rev_id DESC";
  private PreparedStatement reservationGetIDStatement;

  private static final String CAPACITY_INSERT_SQL = "INSERT INTO Capacity VALUES(?,?)";
  private PreparedStatement capacityInsertStatement;

  private static final String CAPACITY_CHECK_SQL = "SELECT capacity FROM Capacity WHERE fid = ?";
  private PreparedStatement capacityCheckStatement;

  private static final String CAPACITY_UPDATE_SQL = "UPDATE Capacity SET capacity = ? WHERE fid = ?";
  private PreparedStatement capacityUpdateStatement;

  private static final String FLIGHT_INFO_SQL = "SELECT fid,carrier_id,flight_num,origin_city,dest_city,actual_time as time,capacity, price FROM Flights  WHERE fid = ?";
  private PreparedStatement flightInfoStatement;

  private static final String FLIGHT_PRICE_SQL = "SELECT price FROM Flights WHERE fid = ?";
  private PreparedStatement flightPricStatement;

  private static final String GET_USER_MONEY_SQL = "SELECT balance FROM Users WHERE username = ?";
  private PreparedStatement getUserMoneyStatement;

  private static final String UPDATE_RESERVATION_PAID_SQL = "UPDATE Reservations SET paid = 1 WHERE rev_id = ?";
  private PreparedStatement updateReservationPaidStatement;

  private static final String UPDATE_USER_MONEY_SQL = "UPDATE Users SET balance = ? WHERE username = ?";
  private PreparedStatement updateUserMoneyStatement;

  private static final String CANCEL_RESERVATIONS_SQL = "DELETE FROM Reservations WHERE rev_id = ?";
  private PreparedStatement cancelReservatioStatement;

  private static final String DELETE_USERS_SQL = "DELETE Users";
  private PreparedStatement deleteUsersStatement;

  private static final String DELETE_RESERVATIONS_SQL = "DELETE Reservations";
  private PreparedStatement deleteReservatioStatement;

  private static final String DELETE_CAPACITY_SQL = "DELETE CAPACITY";
  private PreparedStatement deleteCapacityStatement;
  
  // data structures for storing search results
  private TreeMap<Integer, ArrayList<Flight>> sortedFlights = new TreeMap<>();
  private HashMap<Integer, ArrayList<Flight>> searchedFlights = new HashMap<>();

  // transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  private PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  private PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  private PreparedStatement rollbackTransactionStatement;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public Query(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /* Connection code to SQL Azure.  */
  public void openConnection() throws Exception
  {
    configProps.load(new FileInputStream(configFilename));

    jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    jSQLUrl = configProps.getProperty("flightservice.url");
    jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement

    /* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created. Do not drop any tables and do not
   * clear the flights table. You should clear any tables you use to store reservations
   * and reset the next reservation ID to be 1.
   */
  public void clearTables ()
  {
    // your code here
    try {
      deleteUsersStatement.executeUpdate();
      deleteReservatioStatement.executeUpdate();
      deleteCapacityStatement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
    commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
    rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    loginSearchStatement = conn.prepareStatement(LOGIN_SEARCH_SQL);
    createSearchStatement = conn.prepareStatement(CREATE_SEARCH_SQL);
    insertUserStatement = conn.prepareStatement(INSERT_USER_SQL);
    directSearchStatement = conn.prepareStatement(DIRECT_SEARCH_SQL);
    indirectSearchStatement = conn.prepareStatement(INDIRECT_SEARCH_SQL);
    reservationInsertStatement = conn.prepareStatement(RESERVATION_INSERT_SQL);
    reservationCheckStatement = conn.prepareStatement(RESERVATION_CHECK_SQL);
    reservationGetIDStatement = conn.prepareStatement(RESERVATION_GET_ID);
    capacityInsertStatement = conn.prepareStatement(CAPACITY_INSERT_SQL);
    capacityCheckStatement = conn.prepareStatement(CAPACITY_CHECK_SQL);
    capacityUpdateStatement = conn.prepareStatement(CAPACITY_UPDATE_SQL);
    flightInfoStatement = conn.prepareStatement(FLIGHT_INFO_SQL);
    flightPricStatement = conn.prepareStatement(FLIGHT_PRICE_SQL);
    getUserMoneyStatement = conn.prepareStatement(GET_USER_MONEY_SQL);
    updateReservationPaidStatement = conn.prepareStatement(UPDATE_RESERVATION_PAID_SQL);
    updateUserMoneyStatement = conn.prepareStatement(UPDATE_USER_MONEY_SQL);
    deleteUsersStatement = conn.prepareStatement(DELETE_USERS_SQL);
    deleteReservatioStatement = conn.prepareStatement(DELETE_RESERVATIONS_SQL);
    deleteCapacityStatement = conn.prepareStatement(DELETE_CAPACITY_SQL);
    cancelReservatioStatement = conn.prepareStatement(CANCEL_RESERVATIONS_SQL);
    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username
   * @param password
   *
   * @return If someone has already logged in, then return "User already logged in\n"
   * For all other errors, return "Login failed\n".
   *
   * Otherwise, return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password)
  {
    return transaction_loginHelper(username, password);
  }

  private String transaction_loginHelper(String username, String password){
    try{
      if (this.username == null) {
        loginSearchStatement.clearParameters();
        loginSearchStatement.setString(1, username);
        loginSearchStatement.setString(2, password);
        ResultSet rs = loginSearchStatement.executeQuery();
        int count = 0;
        while(rs.next()){
          count++;
        }
        if(count == 1) {
          this.username = username;
          return ("Logged in as " + username + "\n");
        } else {
          return "Login failed\n";
        }
      } else {
        return "User already logged in\n";
      }
    } catch(SQLException e) {
      e.printStackTrace();
      return "Login failed\n";
    }
  }

  /**
   * Implement the create user function.
   *
   * @param username new user's username. User names are unique the system.
   * @param password new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
   */
  public String transaction_createCustomer (String username, String password, int initAmount){
    return transaction_createCustomerHelper(username, password, initAmount);
  }

  private String transaction_createCustomerHelper(String username, String password, int initAmount){
    if (initAmount < 0) {
      return "Failed to create user\n";
    }
    try{
      beginTransaction();
      createSearchStatement.clearParameters();
      createSearchStatement.setString(1, username);
      ResultSet rs = createSearchStatement.executeQuery();
      int count = 0;
      while(rs.next()){
        count++;
      }
      if (count == 1) {
        rollbackTransaction();
        return "Failed to create user\n";
      } else {
        insertUserStatement.clearParameters();
        insertUserStatement.setString(1, username);
        insertUserStatement.setString(2, password);
        insertUserStatement.setInt(3, initAmount);
        insertUserStatement.executeUpdate();
        commitTransaction();
        return ("Created user " + username + "\n");
      }
    } catch(SQLException e) {
      e.printStackTrace();
      return "Failed to create user\n";
    }
  }
  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    return transaction_searchHelper(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_searchHelper(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    String output;
    int directCounter = 0;
    try {
      directSearchStatement.clearParameters();
      indirectSearchStatement.clearParameters();
      // clearing data structures
      sortedFlights.clear();
      searchedFlights.clear();

      // setting parameters for directSearchStatement
      directSearchStatement.setInt(1, numberOfItineraries);
      directSearchStatement.setString(2, originCity);
      directSearchStatement.setString(3, destinationCity);
      directSearchStatement.setInt(4, dayOfMonth);

      ResultSet rs = directSearchStatement.executeQuery();
      while (rs.next()) {
        Flight directF = new Flight();
        directF.fid = rs.getInt("fid");
        directF.dayOfMonth = rs.getInt("day_of_month");
        directF.carrierId = rs.getString("carrier_id");
        directF.flightNum = rs.getString("flight_num");
        directF.originCity = rs.getString("origin_city");
        directF.destCity = rs.getString("dest_city");
        directF.time = rs.getInt("actual_time");
        directF.capacity = rs.getInt("capacity");
        directF.price = rs.getInt("price");

        if (sortedFlights.containsKey(directF.time)) {
          sortedFlights.get(directF.time).add(directF);
        } else {
          ArrayList<Flight> flights = new ArrayList<>();
          flights.add(directF);
          sortedFlights.put(directF.time,flights);
        }
        directCounter++;
      }
      int remainingFlight = numberOfItineraries - directCounter;
      if (!directFlight && remainingFlight > 0) {
        indirectSearchStatement.setInt(1, remainingFlight);
        indirectSearchStatement.setString(2, originCity);
        indirectSearchStatement.setString(3, destinationCity);
        indirectSearchStatement.setInt(4, dayOfMonth);
        ResultSet rs1 = indirectSearchStatement.executeQuery();
        while (rs1.next()) {
          Flight indirectF1 = new Flight();
          Flight indirectF2 = new Flight();
          indirectF1.fid = rs1.getInt("id1");
          indirectF1.dayOfMonth = rs1.getInt("day_of_month");
          indirectF1.carrierId = rs1.getString("carrier_id1");
          indirectF1.flightNum = rs1.getString("flight_num1");
          indirectF1.originCity = rs1.getString("origin_city1");
          indirectF1.destCity = rs1.getString("dest_city1");
          indirectF1.time = rs1.getInt("t1");
          indirectF1.capacity = rs1.getInt("capacity1");
          indirectF1.price = rs1.getInt("price1");
          
          indirectF2.fid = rs1.getInt("id2");
          indirectF2.dayOfMonth = indirectF1.dayOfMonth;
          indirectF2.carrierId = rs1.getString("carrier_id2");
          indirectF2.flightNum = rs1.getString("flight_num2");
          indirectF2.originCity = rs1.getString("origin_city2");
          indirectF2.destCity = rs1.getString("dest_city2");
          indirectF2.time = rs1.getInt("t2");
          indirectF2.capacity = rs1.getInt("capacity2");
          indirectF2.price = rs1.getInt("price2");

          int totalTime = indirectF1.time + indirectF2.time;

          if (sortedFlights.containsKey(totalTime)) {
            sortedFlights.get(totalTime).add(indirectF1);
            sortedFlights.get(totalTime).add(indirectF2);
          } else {
            ArrayList<Flight> indirectFlights = new ArrayList<>();
            indirectFlights.add(indirectF1);
            indirectFlights.add(indirectF2);
            sortedFlights.put(totalTime,indirectFlights);
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    output = "";
    int itID = 0;
    for (Map.Entry<Integer, ArrayList<Flight>> entrySet : sortedFlights.entrySet()) {
      ArrayList<Flight> allFlights = entrySet.getValue();
      ArrayList<Flight> groupedFlights = new ArrayList<>();
      for (Flight f : allFlights) {
        groupedFlights.add(f);
        if (!f.destCity.equals(destinationCity)) {
          continue;
        } else {
          int numOfFlgihts = groupedFlights.size();
          output += "Itinerary " + itID + ": " + numOfFlgihts + " flight(s), " +  entrySet.getKey() + " minutes\n";
          for (Flight f1 : groupedFlights) {
            output += f1.toString() + "\n";
          }
          searchedFlights.put(itID,new ArrayList<Flight>(groupedFlights));
          itID++;
          groupedFlights.clear();
        }
      }
    }
    return output;
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
   * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
   * If the user already has a reservation on the same day as the one that they are trying to book now, then return
   * "You cannot book two flights in the same day\n".
   * For all other errors, return "Booking failed\n".
   *
   * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
   * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
   * successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId)
  {
    try {
      
      reservationInsertStatement.clearParameters();
      reservationCheckStatement.clearParameters();
      reservationGetIDStatement.clearParameters();

      if (this.username == null) {
        return "Cannot book reservations, not logged in\n";
      } else if (!searchedFlights.containsKey(itineraryId)) {
        return "No such itinerary " + itineraryId + "\n";
      } else {
        reservationCheckStatement.setString(1, username);
        ResultSet sameDayCheck = reservationCheckStatement.executeQuery();
        if (sameDayCheck.next()) {
          int day = sameDayCheck.getInt("day");
          if (day == searchedFlights.get(itineraryId).get(0).dayOfMonth) {
            return "You cannot book two flights in the same day\n";
          }
        }
        beginTransaction();
        ResultSet getID = reservationGetIDStatement.executeQuery();
        if (getID.next()) {
          reservationId = getID.getInt("rev_id") + 1;
        }
        reservationInsertStatement.setInt(1, reservationId);
        reservationInsertStatement.setInt(2, itineraryId);
        reservationInsertStatement.setString(3, username);
        reservationInsertStatement.setInt(4, 0);
        reservationInsertStatement.setInt(5, searchedFlights.get(itineraryId).get(0).fid);
        if (searchedFlights.get(itineraryId).size() == 1) {
          reservationInsertStatement.setInt(6, -1);
        } else {
          reservationInsertStatement.setInt(6, searchedFlights.get(itineraryId).get(1).fid);
        }
        reservationInsertStatement.setInt(7, searchedFlights.get(itineraryId).get(0).dayOfMonth);
        reservationInsertStatement.executeUpdate();

        for (Flight f : searchedFlights.get(itineraryId)) {
          capacityInsertStatement.clearParameters();
          capacityCheckStatement.clearParameters();
          capacityUpdateStatement.clearParameters();
          
          capacityInsertStatement.setInt(1, f.fid);
          capacityInsertStatement.setInt(2, f.capacity);
          capacityInsertStatement.executeUpdate();
          
          capacityCheckStatement.setInt(1, f.fid);
          ResultSet capCheck = capacityCheckStatement.executeQuery();
          if (capCheck.next()) {
            int capacity = capCheck.getInt("capacity");
            if (capacity - 1 < 0) {
              rollbackTransaction();
              return "Booking failed\n";
            } else {
              capacityUpdateStatement.setInt(1, capacity - 1);
              capacityUpdateStatement.setInt(2, f.fid);
              capacityUpdateStatement.executeUpdate();
            }
          }
        }
        commitTransaction();
        return "Booked flight(s), reservation ID: " + reservationId + "\n";
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Booking failed\n";
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
   * If the user has no reservations, then return "No reservations found\n"
   * For all other errors, return "Failed to retrieve reservations\n"
   *
   * Otherwise return the reservations in the following format:
   *
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * Reservation [reservation ID] paid: [true or false]:\n"
   * [flight 1 under the reservation]
   * [flight 2 under the reservation]
   * ...
   *
   * Each flight should be printed using the same format as in the {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations()
  {
    try {
      reservationCheckStatement.clearParameters();
      String output = "";
      int payment;
      boolean paid = false;
      if (this.username == null) {
        return "Cannot view reservations, not logged in\n";
      } 
      reservationCheckStatement.setString(1, username);
      ResultSet rs = reservationCheckStatement.executeQuery();
      int numberOfReservations = 0;
      while (rs.next()) {
        int rev_id = rs.getInt("rev_id");
        payment = rs.getInt("paid");
        int it_id = rs.getInt("it_id");
        int fid1 = rs.getInt("fid1");
        int fid2 =  rs.getInt("fid2");
        int day = rs.getInt("day");
        ArrayList<Integer> flightIDS = new ArrayList<>();
        flightIDS.add(fid1);
        flightIDS.add(fid2);
        if (payment == 1) {
          paid = true;
        }
        output += "Reservation " + rev_id + " paid: " + paid + ":\n";
        for (int fid : flightIDS) {
          flightInfoStatement.clearParameters();
          flightInfoStatement.setInt(1, fid);
          ResultSet rs1 = flightInfoStatement.executeQuery();
          if (rs1.next()) {
            Flight f = new Flight();
            f.fid = fid;
            f.dayOfMonth = day;
            f.carrierId = rs1.getString("carrier_id");
            f.flightNum = rs1.getString("flight_num");
            f.originCity = rs1.getString("origin_city");
            f.destCity = rs1.getString("dest_city");
            f.time = rs1.getInt("time");
            f.capacity = rs1.getInt("capacity");
            f.price = rs1.getInt("price");
            output += f.toString() + "\n";
          }
        }
        numberOfReservations++;
      }
      if (numberOfReservations == 0) {
        return "No reservations found\n";
      }
      return output;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Failed to retrieve reservations\n";
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
   * For all other errors, return "Failed to cancel reservation [reservationId]"
   *
   * If successful, return "Canceled reservation [reservationId]"
   *
   * Even though a reservation has been canceled, its ID should not be reused by the system.
   */
  public String transaction_cancel(int reservationId)
  {
    // only implement this if you are interested in earning extra credit for the HW!
    return "Failed to cancel reservation " + reservationId + "\n";
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   * If the reservation is not found / not under the logged in user's name, then return
   * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
   * If the user does not have enough money in their account, then return
   * "User has only [balance] in account but itinerary costs [cost]\n"
   * For all other errors, return "Failed to pay for reservation [reservationId]\n"
   *
   * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
   * where [balance] is the remaining balance in the user's account.
   */
  public String transaction_pay (int reservationId)
  {
    try {
      reservationCheckStatement.clearParameters();
      getUserMoneyStatement.clearParameters();
      updateReservationPaidStatement.clearParameters();
      updateUserMoneyStatement.clearParameters();
      if (this.username == null) {
        return "Cannot pay, not logged in\n";
      } else  {
        reservationCheckStatement.setString(1,username);
        ResultSet check = reservationCheckStatement.executeQuery();
        if (!check.next()) {
          return "Cannot find unpaid reservation " + reservationId +" under user: " + username+ "\n";
        } else {
          int rev_id = check.getInt("rev_id");
          int paid = check.getInt("paid");
          if (rev_id != reservationId || paid == 1) {
            return "Cannot find unpaid reservation " + reservationId +" under user: " + username+ "\n";
          }
        }
      }
      getUserMoneyStatement.setString(1, username);
      ResultSet userMoney = getUserMoneyStatement.executeQuery();
      int userBalance = 0;
      if (userMoney.next()) {
        userBalance = userMoney.getInt("balance");
      }
      reservationCheckStatement.clearParameters();
      reservationCheckStatement.setString(1,username);
      ResultSet reservationDetails = reservationCheckStatement.executeQuery();
      int fid1 = -1;
      int fid2 = -1;
      int totalFlightCost = 0;
      if (reservationDetails.next()) {
        fid1 = reservationDetails.getInt("fid1");
        fid2 = reservationDetails.getInt("fid2");
      }
      ArrayList<Integer> flightIDS = new ArrayList<>();
      flightIDS.add(fid1);
      flightIDS.add(fid2);
      for (int fid : flightIDS) {
        flightPricStatement.clearParameters();
        flightPricStatement.setInt(1,fid);
        ResultSet flightPrice = flightPricStatement.executeQuery();
        if (flightPrice.next()) {
          totalFlightCost += flightPrice.getInt("price");
        }
      }
      beginTransaction();
      if (userBalance >= totalFlightCost) {
        int remainingBalance = userBalance - totalFlightCost;
        updateUserMoneyStatement.setInt(1,remainingBalance);
        updateUserMoneyStatement.setString(2,username);
        updateReservationPaidStatement.setInt(1,reservationId);
        updateUserMoneyStatement.executeUpdate();
        updateReservationPaidStatement.executeUpdate();
        commitTransaction();
        return "Paid reservation: " + reservationId + " remaining balance: " + remainingBalance + "\n";
      } else {
        rollbackTransaction();
        return "User has only " + userBalance + " in account but itinerary costs " + totalFlightCost + "\n";
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
      return "Failed to pay for reservation " + reservationId + "\n";
  }

  /* some utility functions below */

  public void beginTransaction() throws SQLException
  {
    conn.setAutoCommit(false);
    beginTransactionStatement.executeUpdate();
  }

  public void commitTransaction() throws SQLException
  {
    commitTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  public void rollbackTransaction() throws SQLException
  {
    rollbackTransactionStatement.executeUpdate();
    conn.setAutoCommit(true);
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments. You don't need to
   * use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
}
