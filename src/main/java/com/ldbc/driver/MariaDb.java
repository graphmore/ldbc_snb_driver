// Author: Charlie Davidson
// Research Advisor: Dr. Reilly
// Database Connector for MariaDb using LDBC SNB Driver
// Documentation: https://github.com/ldbc/ldbc_snb_driver/wiki

package com.ldbc.driver;

// import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.LoggingService;


// import com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.sql.*;
import java.io.*;
import java.util.*;

public class MariaDb extends Db {

  /**
  * Connection to the LDBC Databse in MariaDB.
  */
  static class MariaDbClient {

    /** java.sql.Connection to MariaDb */
    Connection dbConnect = null;

    /**
    * Connect to the LDBC Database in MariaDb.
    *
    * @param connectionURL Provides the entire connection string for MariaDB,
    *        including password and database. Should be of the form:
    *        "jdbc:mariadb://localhost:3306/dbName?user=dbUser&password=dbPassword"
    */
    MariaDbClient(String connectionUrl) {
      try {
        // Call the java.sql.DriverManager.getConnection method to establish the
        // database connection.
        System.out.println("***** Connecting to MariaDb *****");
        dbConnect = DriverManager.getConnection(connectionUrl);
      } catch(SQLException e) {
        // If the getConnection method throws an SQLException, print information
        // and end the program.
        e.printStackTrace();
        System.out.println("Cannot connect to MariaDb using connection url: " + connectionUrl);
        System.exit(1);
      }
    }

    public void close() {
      if (dbConnect != null) {
        try {
          // Close the database connection
          dbConnect.close();
        } catch(SQLException e) {
          // If the close method throws an SQLException, print information
          // and end the program.
          e.printStackTrace();
          System.out.println("Error when closing connection to MariaDB");
          System.exit(1);
        }
      }
    }

/*
    Object execute(String queryString, Map<Integer,Object> queryParams) {
      // PreparedStatement psQuery = dbConnect.prepareStatement(queryString);
      // int i=0;
      // // May need to convert i to Integer object
      // while (queryParams.get(i)!=null){
      //   if (queryParams.get(i).getClass()==String.class){
      //     psQuery.setString(i, queryParams.get(i))
      //   }
      //   else if (queryParams.get(i).getClass()==Integer.class){
      //     psQuery.setInt(i, queryParams.get(i))
      //   }
      //   else if (queryParams.get(i).getClass()==Double.class){
      //     psQuery.setDouble(i, queryParams.get(i))
      //   }
      //   i++;
      // }
      // ResultSet rsQuery = psQuery.executeQuery();
      // return rsQuery;
    }
*/
  } // end of MariaDbClient class

  static class MariaDbConnectionState extends DbConnectionState {

    private final MariaDbClient mariaDbClient;

    private MariaDbConnectionState(String connectionUrl) {
      mariaDbClient = new MariaDbClient(connectionUrl);
    }

    public MariaDbClient client() {
      return mariaDbClient;
    }

    public void close() {
      mariaDbClient.close();
    }

  } // end of MariaDbConnectionState class

  private MariaDbConnectionState connectionState = null;

  @Override
  public void onInit(Map<String,String> properties, LoggingService loggingService) throws DbException {

    String connectionUrl = properties.get("url");

    connectionState = new MariaDbConnectionState(connectionUrl);

/*
    registerOperationHandler(LdbcQuery1.class,LdbcQuery1Handler.class);
    registerOperationHandler(LdbcQuery2.class,LdbcQuery2Handler.class);
    registerOperationHandler(LdbcQuery3.class,LdbcQuery3Handler.class);
    registerOperationHandler(LdbcQuery4.class,LdbcQuery4Handler.class);
    registerOperationHandler(LdbcQuery5.class,LdbcQuery5Handler.class);
    registerOperationHandler(LdbcQuery6.class,LdbcQuery6Handler.class);
    registerOperationHandler(LdbcQuery7.class,LdbcQuery7Handler.class);
    registerOperationHandler(LdbcQuery8.class,LdbcQuery8Handler.class);
    registerOperationHandler(LdbcQuery9.class,LdbcQuery9Handler.class);
    registerOperationHandler(LdbcQuery10.class,LdbcQuery10Handler.class);
    registerOperationHandler(LdbcQuery11.class,LdbcQuery11Handler.class);
    registerOperationHandler(LdbcQuery12.class,LdbcQuery12Handler.class);
    registerOperationHandler(LdbcQuery13.class,LdbcQuery13Handler.class);
    registerOperationHandler(LdbcQuery14.class,LdbcQuery14Handler.class);
  */
    // registerOperationHandler(LdbcShortQuery1PersonProfile.class,LdbcShortQuery1PersonProfileHandler.class)
    // registerOperationHandler(LdbcShortQuery2PersonPosts.class,LdbcShortQuery2PersonPostsHandler.class)
    // registerOperationHandler(LdbcShortQuery3PersonFriends.class,LdbcShortQuery3PersonFriendsHandler.class)
    // registerOperationHandler(LdbcShortQuery4MessageContent.class,LdbcShortQuery4MessageContentHandler.class)
    // registerOperationHandler(LdbcShortQuery5MessageCreator.class,LdbcShortQuery5MessageCreatorHandler.class)
    // registerOperationHandler(LdbcShortQuery6MessageForum.class,LdbcShortQuery6MessageForumHandler.class)
    // registerOperationHandler(LdbcShortQuery7MessageReplies.class,LdbcShortQuery7MessageRepliesHandler.class)
    // registerOperationHandler(LdbcUpdate1AddPerson.class,LdbcUpdate1AddPersonHandler.class)
    // registerOperationHandler(LdbcUpdate2AddPostLike.class,LdbcUpdate2AddPostLikeHandler.class)
    // registerOperationHandler(LdbcUpdate3AddCommentLike.class,LdbcUpdate3AddCommentLikeHandler.class)
    // registerOperationHandler(LdbcUpdate4AddForum.class,LdbcUpdate4AddForumHandler.class)
    // registerOperationHandler(LdbcUpdate5AddForumMembership.class,LdbcUpdate5AddForumMembershipHandler.class)
    // registerOperationHandler(LdbcUpdate6AddPost.class,LdbcUpdate6AddPostHandler.class)
    // registerOperationHandler(LdbcUpdate7AddComment.class,LdbcUpdate7AddCommentHandler.class)
    // registerOperationHandler(LdbcUpdate8AddFriendship.class,LdbcUpdate8AddFriendshipHandler.class)
  }

  @Override
  public void onClose() throws IOException {
    connectionState.close();
  }

/*
  // Documentation uses extends
  //public static class LdbcQuery1Handler extends OperationHandler<LdbcQuery1> {
  public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      // Map<String, Object> queryParams = new HashMap<>();
      // queryParams.put("personid",operation.personId());
      // queryParams.put("firstname",operation.firstName());
      // queryParams.put("limit",operation.limit());
      //
      // // TODO replace with actual query string
      // String queryString = null;
      //
      // BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      // LdbcQuery1Result result = client.execute(queryString, queryParams);
      //
      // return operation.buildResult(0, result);



    }
  }

  public static class LdbcQuery2Handler extends OperationHandler<LdbcQuery2> {

    private static final String dbPassFile = "/path/to/file";

    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      // Map<String, Object> queryParams = new HashMap<>();
      // queryParams.put(1,operation.personId());
      // queryParams.put(2,operation.maxDate());
      // queryParams.put(3,operation.personId());
      // queryParams.put(4,operation.maxDate());
      // queryParams.put(5,operation.limit());




      // Get database account info from file
  		Scanner inFile = new Scanner(new File(dbPassFile));
  		String dbUser = inFile.next();
  		String dbPass = inFile.next();
  		inFile.close();

      // Connect to the database
  		Connection dbConnect = null;
  		dbConnect = DriverManager.getConnection(
  			"jdbc:mariadb://localhost:3306/" + boatRentalDB + "?user="
  			+ dbUser + "&password=" + dbPass);

      // Does this approach work?
      String queryString = "SELECT P2.id, P2.firstName, P2.lastName, Po.id, Po.content, Po.imageFile, " +
      "Po.creationDate " +
      "FROM Person P2, Post Po, KnowsPerson K " +
      "WHERE K.id1 = ? AND K.id2 = P2.id AND Po.creatorId = P2.id " +
      "AND Po.creationDate > ? "+
      "UNION " +
      "SELECT P2.id, P2.firstName, P2.lastName, Po.id, Po.content, NULL, Po.creationDate " +
      "FROM Person P2, Comment Po, KnowsPerson K " +
      "WHERE K.id1 = ? AND K.id2 = P2.id AND Po.creatorId = P2.id " +
      "AND Po.creationDate > ?";

      PreparedStatement psQuery = dbConnect.prepareStatement(queryString);

      psQuery.setString(1, operation.personId());
      psQuery.setInt(2, operation.maxDate());
      psQuery.setString(3, operation.personId());
      psQuery.setInt(4, operation.maxDate());

      ResultSet rsQuery = psQuery.executeQuery();
      //BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      //LdbcQuery2Result result = client.execute(queryString, queryParams);

      long personId = rsQuery.getLong(1);
      String personFirstName = rsQuery.getString(2);
      String personLastName = rsQuery.getString(3);
      long messageId = rsQuery.getLong(4);
      String messageContent = rsQuery.getString(5);
      long messageCreationDate = rsQuery.getLong(6);

      LdbcQuery2Result result = new LdbcQuery2Result(personId, personFirstName, personLastName, messageId, messageContent, messageCreationDate);

      psQuery.close();
  		dbConnect.close();

      //return operation.buildResult(0, result);
      return result;
    }
  }

  public static class LdbcQuery3Handler extends OperationHandler<LdbcQuery3> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("countryxname",operation.countryXName());
      queryParams.put("countryyname",operation.countryYName());
      queryParams.put("startdate",operation.startDate());
      queryParams.put("durationdays",operation.durationDays());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery3Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }

  public static class LdbcQuery4Handler extends OperationHandler<LdbcQuery4> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("startdate",operation.startDate());
      queryParams.put("durationdays",operation.durationDays());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery4Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery5Handler extends OperationHandler<LdbcQuery5> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("mindate",operation.minDate());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery5Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery6Handler extends OperationHandler<LdbcQuery6> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("tagname",operation.tagName());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery6Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery7Handler extends OperationHandler<LdbcQuery7> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery7Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery8Handler extends OperationHandler<LdbcQuery8> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery8Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery9Handler extends OperationHandler<LdbcQuery9> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("maxdate",operation.maxDate());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery9Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery10Handler extends OperationHandler<LdbcQuery10> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("month",operation.month());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery10Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery11Handler extends OperationHandler<LdbcQuery11> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("countryname",operation.countryName());
      queryParams.put("workfromyear",operation.workFromYear());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery11Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery12Handler extends OperationHandler<LdbcQuery12> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("personid",operation.personId());
      queryParams.put("tagclassname",operation.tagClassName());
      queryParams.put("limit",operation.limit());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery12Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery13Handler extends OperationHandler<LdbcQuery13> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("person1id",operation.person1Id());
      queryParams.put("person2id",operation.person2Id());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery13Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
  public static class LdbcQuery14Handler extends OperationHandler<LdbcQuery14> {
    @Override
    public OperationResultReport executeOperation(ReadOperation operation) {
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("person1id",operation.person1Id());
      queryParams.put("person2id",operation.person2Id());

      // TODO replace with actual query string
      String queryString = null;

      BasicClient client = ((BasicDbConnectionState) dbConnectionState()).client();
      LdbcQuery14Result result = client.execute(queryString, queryParams);

      return operation.buildResult(0, result);
    }
  }
*/
  @Override
  protected DbConnectionState getConnectionState() throws DbException {
    return connectionState;
  }
}
