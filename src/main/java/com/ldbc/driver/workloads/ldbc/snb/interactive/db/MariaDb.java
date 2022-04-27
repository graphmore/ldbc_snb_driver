// Author: Charlie Davidson
// Research Advisor: Dr. Reilly
// Last Modified: March 2022
// Database Connector for MariaDb using LDBC SNB Driver
// Documentation: https://github.com/ldbc/ldbc_snb_driver/wiki

// NOTE: The documentation for implementing a database connector is outdated
// Instead follow the format of SimpleDb.java, located in src/main/java/com/ldbc/driver/workloads/simple/db
// Or follow the dummy files located in the same directory as this file: src/main/java/com/ldbc/driver/workloads/ldbc/snb/interactive/db

package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MariaDb extends Db {

    // Connection to MariaDb
    public static Connection dbConnect;

    // Query Strings
    public static String ldbcQuery1String = null;
    public static String ldbcQuery2String = "SELECT P2.id, P2.firstName, P2.lastName, Po.id, Po.content, Po.imageFile, "
        + "Po.creationDate "
        + "FROM Person P2, Post Po, KnowsPerson K "
        + "WHERE K.id1 = ? AND K.id2 = P2.id AND Po.creatorId = P2.id "
        + "AND Po.creationDate > ? "
        + "UNION "
        + "SELECT P2.id, P2.firstName, P2.lastName, Po.id, Po.content, NULL, Po.creationDate "
        + "FROM Person P2, Comment Po, KnowsPerson K "
        + "WHERE K.id1 = ? AND K.id2 = P2.id AND Po.creatorId = P2.id "
        + "AND Po.creationDate > ?;";
    public static String ldbcQuery3String = null;
    public static String ldbcQuery4String = null;
    public static String ldbcQuery5String = null;
    public static String ldbcQuery6String = null;
    public static String ldbcQuery7String = null;
    public static String ldbcQuery8String = null;
    public static String ldbcQuery9String = null;
    public static String ldbcQuery10String = null;
    public static String ldbcQuery11String = null;
    public static String ldbcQuery12String = null;
    public static String ldbcQuery13String = null;
    public static String ldbcQuery14String = null;

    public static String ldbcShortQuery1String = "SELECT P.firstName, P.lastName, P.birthday, P.locationIP, P.browserUsed, P.location, P.gender, P.creationDate "
        + "FROM Person P "
        + "WHERE P.id = ?;";
    public static String ldbcShortQuery2String = "WITH RECURSIVE ParentChain(parentId, commentId, origCommentId, creatorId) AS ( "
        + "(SELECT COALESCE(C.replyOfPost, C.replyOfComment), C.id, C.id, C.creatorId "
        + "FROM Comment C "
        + "WHERE C.creatorId = ? "
        + "ORDER BY C.creationDate DESC LIMIT 10) "
        + "UNION"
        + "(SELECT COALESCE(C2.replyOfPost, C2.replyOfComment), C2.id, PC.origCommentId, C2.creatorId "
        + "FROM Comment C2, ParentChain PC "
        + "WHERE C2.id = PC.parentId) "
        + "), OriginalPost(origCommentId, originalPostId) AS ( "
        + "SELECT PC.origCommentId, MIN(PC.parentId) "
        + "FROM ParentChain PC "
        + "GROUP BY PC.origCommentId "
        + "), Messages(messageId, messageContent, messageCreationDate, originalPostId, originalPostAuthorId, originalPostAuthorFirstName, originalPostAuthorLastName) AS ( "
        + "(SELECT P.id, COALESCE(P.imageFile, P.content), P.creationDate, P.id, P.creatorId, U.firstName, U.lastName "
        + "FROM Post P, Person U "
        + "WHERE P.creatorId = U.id and P.creatorId = ? "
        + "ORDER BY P.creationDate DESC LIMIT 10) "
        + "UNION "
        + "(SELECT C2.id, C2.content, C2.creationDate, O.originalPostId, P2.creatorId, U2.firstName, U2.lastName "
        + "FROM Comment C2, OriginalPost O, Post P2, Person U2 "
        + "WHERE C2.id = O.origCommentId "
        + "AND O.originalPostId = P2.id AND P2.creatorId = U2.id) "
        + ") "
        + "SELECT * FROM Messages "
        + "ORDER BY messageCreationDate DESC LIMIT 10;";
    public static String ldbcShortQuery3String = "SELECT P.id, P.firstName, P.lastName, K.creationDate "
        + "FROM KnowsPerson K, Person P "
        + "WHERE K.id2 = P.id AND K.id1 = ? "
        + "ORDER BY K.creationDate DESC, P.id ASC;";
    public static String ldbcShortQuery4String = "SELECT P.creationDate, COALESCE(P.imageFile, P.content, '') "
        + "FROM Post P "
        + "WHERE P.id = ? "
        + "UNION "
        + "SELECT C.creationDate, C.content "
        + "FROM Comment C "
        + "WHERE C.id = ?;";
    public static String ldbcShortQuery5String = "SELECT P.id, P.firstName, P.lastName "
        + "FROM Person P, Comment C "
        + "WHERE C.creatorId = P.id AND C.id = ? "
        + "UNION "
        + "SELECT P.id, P.firstName, P.lastName "
        + "FROM Person P, Post C "
        + "WHERE C.creatorId = P.id AND C.id = ?;";
    public static String ldbcShortQuery6String = "WITH RECURSIVE chain(parent, child) AS ( "
        + "SELECT COALESCE(C.replyOfPost, C.replyOfComment), C.id "
        + "FROM Comment C "
        + "WHERE C.id = ? "
        + "UNION "
        + "SELECT COALESCE(C2.replyOfPost, C2.replyOfComment), C2.id "
        + "FROM Comment C2, chain "
        + "WHERE C2.id = chain.parent "
        + ") "
        + "SELECT F.id, F.title, P.id, P.firstName, P.lastName "
        + "FROM Forum F, Person P, Post T "
        + "WHERE T.id = ? AND F.id = T.inForum AND F.moderator = P.id "
        + "UNION "
        + "SELECT F2.id, F2.title, P2.id, P2.firstName, P2.lastName "
        + "FROM Forum F2, Person P2, Post T2 "
        + "WHERE T2.id = (SELECT MIN(parent) from chain) "
        + "AND F2.id = T2.inForum AND F2.moderator = P2.id;";
    public static String ldbcShortQuery7String = null;

    public static String ldbcUpdate1String = null;
    public static String ldbcUpdate2String = null;
    public static String ldbcUpdate3String = null;
    public static String ldbcUpdate4String = null;
    public static String ldbcUpdate5String = null;
    public static String ldbcUpdate6String = null;
    public static String ldbcUpdate7String = null;
    public static String ldbcUpdate8String = null;

    // Prepared Statements
    public static PreparedStatement psLdbcQuery1;
    public static PreparedStatement psLdbcQuery2;
    public static PreparedStatement psLdbcQuery3;
    public static PreparedStatement psLdbcQuery4;
    public static PreparedStatement psLdbcQuery5;
    public static PreparedStatement psLdbcQuery6;
    public static PreparedStatement psLdbcQuery7;
    public static PreparedStatement psLdbcQuery8;
    public static PreparedStatement psLdbcQuery9;
    public static PreparedStatement psLdbcQuery10;
    public static PreparedStatement psLdbcQuery11;
    public static PreparedStatement psLdbcQuery12;
    public static PreparedStatement psLdbcQuery13;
    public static PreparedStatement psLdbcQuery14;

    public static PreparedStatement psLdbcShortQuery1;
    public static PreparedStatement psLdbcShortQuery2;
    public static PreparedStatement psLdbcShortQuery3;
    public static PreparedStatement psLdbcShortQuery4;
    public static PreparedStatement psLdbcShortQuery5;
    public static PreparedStatement psLdbcShortQuery6;
    public static PreparedStatement psLdbcShortQuery7;

    public static PreparedStatement psLdbcUpdate1;
    public static PreparedStatement psLdbcUpdate2;
    public static PreparedStatement psLdbcUpdate3;
    public static PreparedStatement psLdbcUpdate4;
    public static PreparedStatement psLdbcUpdate5;
    public static PreparedStatement psLdbcUpdate6;
    public static PreparedStatement psLdbcUpdate7;
    public static PreparedStatement psLdbcUpdate8;

    static class MariaDbClient {
        MariaDbClient(String connectionUrl) {
            try {
                System.out.println("***** Connecting to MariaDb *****");
                dbConnect = DriverManager.getConnection(connectionUrl);

                // Create prepared statements
                // psLdbcQuery1 = dbConnect.prepareStatement(ldbcQuery1String);
                psLdbcQuery2 = dbConnect.prepareStatement(ldbcQuery2String);
                // psLdbcQuery3 = dbConnect.prepareStatement(ldbcQuery3String);
                // psLdbcQuery4 = dbConnect.prepareStatement(ldbcQuery4String);
                // psLdbcQuery5 = dbConnect.prepareStatement(ldbcQuery5String);
                // psLdbcQuery6 = dbConnect.prepareStatement(ldbcQuery6String);
                // psLdbcQuery7 = dbConnect.prepareStatement(ldbcQuery7String);
                // psLdbcQuery8 = dbConnect.prepareStatement(ldbcQuery8String);
                // psLdbcQuery9 = dbConnect.prepareStatement(ldbcQuery9String);
                // psLdbcQuery10 = dbConnect.prepareStatement(ldbcQuery10String);
                // psLdbcQuery11 = dbConnect.prepareStatement(ldbcQuery11String);
                // psLdbcQuery12 = dbConnect.prepareStatement(ldbcQuery12String);
                // psLdbcQuery13 = dbConnect.prepareStatement(ldbcQuery13String);
                // psLdbcQuery14 = dbConnect.prepareStatement(ldbcQuery14String);

                psLdbcShortQuery1 = dbConnect.prepareStatement(ldbcShortQuery1String);
                psLdbcShortQuery2 = dbConnect.prepareStatement(ldbcShortQuery2String);
                psLdbcShortQuery3 = dbConnect.prepareStatement(ldbcShortQuery3String);
                psLdbcShortQuery4 = dbConnect.prepareStatement(ldbcShortQuery4String);
                psLdbcShortQuery5 = dbConnect.prepareStatement(ldbcShortQuery5String);
                psLdbcShortQuery6 = dbConnect.prepareStatement(ldbcShortQuery6String);
                // psLdbcShortQuery7 = dbConnect.prepareStatement(ldbcShortQuery7String);

                // psLdbcUpdate1 = dbConnect.prepareStatement(ldbcUpdate1String);
                // psLdbcUpdate2 = dbConnect.prepareStatement(ldbcUpdate2String);
                // psLdbcUpdate3 = dbConnect.prepareStatement(ldbcUpdate3String);
                // psLdbcUpdate4 = dbConnect.prepareStatement(ldbcUpdate4String);
                // psLdbcUpdate5 = dbConnect.prepareStatement(ldbcUpdate5String);
                // psLdbcUpdate6 = dbConnect.prepareStatement(ldbcUpdate6String);
                // psLdbcUpdate7 = dbConnect.prepareStatement(ldbcUpdate7String);
                // psLdbcUpdate8 = dbConnect.prepareStatement(ldbcUpdate8String);

            }
            catch(SQLException e) {
                e.printStackTrace();
                System.out.println("Cannot connect to MariaDb using connection url: " + connectionUrl);
                System.exit(1);
            }
        }

        public void close() {
            if (dbConnect != null) {
                try {
                    // Close prepared statements
                    // psLdbcQuery1.close();
                    psLdbcQuery2.close();
                    // psLdbcQuery3.close();
                    // psLdbcQuery4.close();
                    // psLdbcQuery5.close();
                    // psLdbcQuery6.close();
                    // psLdbcQuery7.close();
                    // psLdbcQuery8.close();
                    // psLdbcQuery9.close();
                    // psLdbcQuery10.close();
                    // psLdbcQuery11.close();
                    // psLdbcQuery12.close();
                    // psLdbcQuery13.close();
                    // psLdbcQuery14.close();

                    psLdbcShortQuery1.close();
                    psLdbcShortQuery2.close();
                    psLdbcShortQuery3.close();
                    psLdbcShortQuery4.close();
                    psLdbcShortQuery5.close();
                    psLdbcShortQuery6.close();
                    // psLdbcShortQuery7.close();

                    // psLdbcUpdate1.close();
                    // psLdbcUpdate2.close();
                    // psLdbcUpdate3.close();
                    // psLdbcUpdate4.close();
                    // psLdbcUpdate5.close();
                    // psLdbcUpdate6.close();
                    // psLdbcUpdate7.close();
                    // psLdbcUpdate8.close();

                    // Close the database connection
                    dbConnect.close();
                }
                catch(SQLException e) {
                    e.printStackTrace();
                    System.out.println("Error when closing connection to MariaDB");
                    System.exit(1);
                }
            }
        }
    }

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

    }

    private MariaDbConnectionState connectionState = null;

    @Override
    public void onInit(Map<String,String> properties, LoggingService loggingService) throws DbException {

        String connectionUrl = properties.get("url");

        connectionState = new MariaDbConnectionState(connectionUrl);

        // Long Reads
        // registerOperationHandler(LdbcQuery1.class,LdbcQuery1Handler.class);
        registerOperationHandler(LdbcQuery2.class,LdbcQuery2Handler.class);
        // registerOperationHandler(LdbcQuery3.class,LdbcQuery3Handler.class);
        // registerOperationHandler(LdbcQuery4.class,LdbcQuery4Handler.class);
        // registerOperationHandler(LdbcQuery5.class,LdbcQuery5Handler.class);
        // registerOperationHandler(LdbcQuery6.class,LdbcQuery6Handler.class);
        // registerOperationHandler(LdbcQuery7.class,LdbcQuery7Handler.class);
        // registerOperationHandler(LdbcQuery8.class,LdbcQuery8Handler.class);
        // registerOperationHandler(LdbcQuery9.class,LdbcQuery9Handler.class);
        // registerOperationHandler(LdbcQuery10.class,LdbcQuery10Handler.class);
        // registerOperationHandler(LdbcQuery11.class,LdbcQuery11Handler.class);
        // registerOperationHandler(LdbcQuery12.class,LdbcQuery12Handler.class);
        // registerOperationHandler(LdbcQuery13.class,LdbcQuery13Handler.class);
        // registerOperationHandler(LdbcQuery14.class,LdbcQuery14Handler.class);

        // Short Reads
        registerOperationHandler(LdbcShortQuery1PersonProfile.class,LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class,LdbcShortQuery2PersonPostsHandler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class,LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class,LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class,LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class,LdbcShortQuery6MessageForumHandler.class);
        // registerOperationHandler(LdbcShortQuery7MessageReplies.class,LdbcShortQuery7MessageRepliesHandler.class);

        // Updates
        // registerOperationHandler(LdbcUpdate1AddPerson.class,LdbcUpdate1AddPersonHandler.class);
        // registerOperationHandler(LdbcUpdate2AddPostLike.class,LdbcUpdate2AddPostLikeHandler.class);
        // registerOperationHandler(LdbcUpdate3AddCommentLike.class,LdbcUpdate3AddCommentLikeHandler.class);
        // registerOperationHandler(LdbcUpdate4AddForum.class,LdbcUpdate4AddForumHandler.class);
        // registerOperationHandler(LdbcUpdate5AddForumMembership.class,LdbcUpdate5AddForumMembershipHandler.class);
        // registerOperationHandler(LdbcUpdate6AddPost.class,LdbcUpdate6AddPostHandler.class);
        // registerOperationHandler(LdbcUpdate7AddComment.class,LdbcUpdate7AddCommentHandler.class);
        // registerOperationHandler(LdbcUpdate8AddFriendship.class,LdbcUpdate8AddFriendshipHandler.class);
    }

    @Override
    public void onClose() throws IOException {
        connectionState.close();
    }

    /*
    public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery1 operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // TODO Populate the prepared statement
                psLdbcQuery1;

                // Execute the query
                ResultSet rsQuery = psLdbcQuery1.executeQuery();

                // TODO Process result set
                List<LdbcQuery1Result> result;

                // Report the results to the result reporter
                // result needs to be of type List<LdbcQuery1Result>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    */

    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2,MariaDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // Convert from java.util.Date to java.sql.Date
                java.sql.Date sqlDate = new java.sql.Date(operation.maxDate().getTime());

                // Populate the prepared statement
                psLdbcQuery2.setLong(1, operation.personId());
                psLdbcQuery2.setDate(2, sqlDate);
                psLdbcQuery2.setLong(3, operation.personId());
                psLdbcQuery2.setDate(4, sqlDate);

                // Execute the query
                ResultSet rsQuery = psLdbcQuery2.executeQuery();

                // Process result set
                List<LdbcQuery2Result> result = new ArrayList<LdbcQuery2Result>();

                while(rsQuery.next()) {
                    long personId = rsQuery.getLong(1);
                    String personFirstName = rsQuery.getString(2);
                    String personLastName = rsQuery.getString(3);
                    long messageId = rsQuery.getLong(4);
                    String messageContent = rsQuery.getString(5);
                    long messageCreationDate = rsQuery.getDate(7).getTime();

                    result.add(new LdbcQuery2Result(personId, personFirstName, personLastName, messageId, messageContent, messageCreationDate));
                }

                // Report the results to the result reporter
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    /*
    public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery3 operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {

                // TODO Populate the prepared statement
                psLdbcQuery3;

                // Execute the query
                ResultSet rsQuery = psLdbcQuery3.executeQuery();

                // TODO Process result set
                List<LdbcQuery3Result> result;

                // Report the results to the result reporter
                // result needs to be of type List<LdbcQuery3Result>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery4 operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {

                // TODO Populate the prepared statement
                psLdbcQuery4;

                // Execute the query
                ResultSet rsQuery = psLdbcQuery4.executeQuery();

                // TODO Process result set
                List<LdbcQuery4Result> result;

                // Report the results to the result reporter
                // result needs to be of type List<LdbcQuery4Result>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery5 operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {

                // TODO Populate the prepared statement
                psLdbcQuery5;

                // Execute the query
                ResultSet rsQuery = psLdbcQuery5.executeQuery();

                // TODO Process result set
                List<LdbcQuery5Result> result;

                // Report the results to the result reporter
                // result needs to be of type List<LdbcQuery5Result>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery6 operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            try {

                // TODO Populate the prepared statement
                psLdbcQuery6;

                // Execute the query
                ResultSet rsQuery = psLdbcQuery6.executeQuery();

                // TODO Process result set
                List<LdbcQuery6Result> result;

                // Report the results to the result reporter
                // result needs to be of type List<LdbcQuery6Result>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    // TODO implement queries 7 - 14

    */

    public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile,MariaDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // Populate the prepared statement
                psLdbcShortQuery1.setLong(1, operation.personId());

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery1.executeQuery();

                // Process result set
                rsQuery.next();

                String firstName = rsQuery.getString(1);
                String lastName = rsQuery.getString(2);
                long birthday = rsQuery.getDate(3).getTime();
                String locationIp = rsQuery.getString(4);
                String browserUsed = rsQuery.getString(5);
                long cityId = rsQuery.getLong(6);
                String gender = rsQuery.getString(7);
                long creationDate = rsQuery.getDate(8).getTime();

                LdbcShortQuery1PersonProfileResult result = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, locationIp, browserUsed, cityId, gender, creationDate);

                // Report the results to the result reporter
                // result needs to be of type LdbcShortQuery1PersonProfileResult
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // Populate the prepared statement
                psLdbcShortQuery2.setLong(1,operation.personId());
                psLdbcShortQuery2.setLong(2,operation.personId());

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery2.executeQuery();

                // Process result set
                List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<LdbcShortQuery2PersonPostsResult>();

                while(rsQuery.next()) {

                    long messageId = rsQuery.getLong(1);
                    String messageContent = rsQuery.getString(2);
                    long messageCreationDate = rsQuery.getDate(3).getTime();
                    long originalPostId = rsQuery.getLong(4);
                    long originalPostAuthorId = rsQuery.getLong(5);
                    String originalPostAuthorFirstName = rsQuery.getString(6);
                    String originalPostAuthorLastName = rsQuery.getString(7);

                    result.add(new LdbcShortQuery2PersonPostsResult(messageId, messageContent, messageCreationDate, originalPostId, originalPostAuthorId, originalPostAuthorFirstName, originalPostAuthorLastName));
                }

                // Report the results to the result reporter
                // result needs to be of type List<LdbcShortQuery2PersonPostsResult>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends,MariaDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // Populate the prepared statement
                psLdbcShortQuery3.setLong(1, operation.personId());

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery3.executeQuery();

                // Process result set
                List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<LdbcShortQuery3PersonFriendsResult>();

                while(rsQuery.next()) {
                    long friendId = rsQuery.getLong(1);
                    String firstName = rsQuery.getString(2);
                    String lastName = rsQuery.getString(3);
                    long friendshipCreationDate = rsQuery.getDate(4).getTime();

                    result.add(new LdbcShortQuery3PersonFriendsResult(friendId, firstName, lastName, friendshipCreationDate));
                }

                // Report the results to the result reporter
                // result needs to be of type List<LdbcShortQuery3PersonFriendsResult>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // Populate the prepared statement
                psLdbcShortQuery4.setLong(1, operation.messageId());
                psLdbcShortQuery4.setLong(2, operation.messageId());

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery4.executeQuery();

                // Process result set
                rsQuery.next();

                long messageCreationDate = rsQuery.getDate(1).getTime();
                String messageContent = rsQuery.getString(2);

                LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(messageContent, messageCreationDate);

                // Report the results to the result reporter
                // result needs to be of type LdbcShortQuery4MessageContentResult
                resultReporter.report( 0, result, operation );
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator,MariaDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // Populate the prepared statement
                psLdbcShortQuery5.setLong(1, operation.messageId());
                psLdbcShortQuery5.setLong(2, operation.messageId());

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery5.executeQuery();

                // Process result set
                rsQuery.next();

                long personId = rsQuery.getLong(1);
                String firstName = rsQuery.getString(2);
                String lastName = rsQuery.getString(3);

                LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(personId, firstName, lastName);

                // Report the results to the result reporter
                // result needs to be of type LdbcShortQuery5MessageCreatorResult
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // TODO Populate the prepared statement
                psLdbcShortQuery6.setLong(1,operation.messageId());
                psLdbcShortQuery6.setLong(2,operation.messageId());

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery6.executeQuery();

                // TODO Process result set
                rsQuery.next();

                long forumId = rsQuery.getLong(1);
                String forumTitle = rsQuery.getString(2);
                long moderatorId = rsQuery.getLong(3);
                String moderatorFirstName = rsQuery.getString(4);
                String moderatorLastName = rsQuery.getString(5);

                LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(forumId, forumTitle, moderatorId, moderatorFirstName, moderatorLastName);

                // Report the results to the result reporter
                // result needs to be of type LdbcShortQuery6MessageForumResult
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    /*
    public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies,MariaDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation, MariaDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

            try {

                // TODO Populate the prepared statement
                psLdbcShortQuery7;

                // Execute the query
                ResultSet rsQuery = psLdbcShortQuery7.executeQuery();

                // TODO Process result set
                List<LdbcShortQuery7MessageRepliesResult> result;

                // Report the results to the result reporter
                // result needs to be of type List<LdbcShortQuery7MessageRepliesResult>
                resultReporter.report( 0, result, operation );

            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
     */

    // TODO implement update queries 1 - 8

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
