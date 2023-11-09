package eu.more2020.visual.middleware.datasource.QueryExecutor;

import cfjd.com.fasterxml.jackson.annotation.JsonIgnore;
import eu.more2020.visual.middleware.datasource.DataSourceQuery;
import eu.more2020.visual.middleware.datasource.ModelarDBQuery;
import eu.more2020.visual.middleware.domain.Dataset.AbstractDataset;
import eu.more2020.visual.middleware.domain.Query.QueryMethod;
import eu.more2020.visual.middleware.domain.QueryResults;
import eu.more2020.visual.middleware.domain.UnivariateDataPoint;
import eu.more2020.visual.middleware.experiments.util.PrepareSQLStatement;
import cfjd.org.apache.arrow.flight.FlightClient;
import cfjd.org.apache.arrow.flight.FlightStream;
import cfjd.org.apache.arrow.flight.Ticket;
import cfjd.org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class ModelarDBQueryExecutor implements QueryExecutor, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ModelarDBQueryExecutor.class);
    @JsonIgnore
    FlightClient flightClient;
    AbstractDataset dataset;

    public ModelarDBQueryExecutor(FlightClient flightClient, AbstractDataset dataset) {
        this.flightClient = flightClient;
        this.dataset = dataset;
        LOG.info("Created Executor {}, ", this);
    }


    public ModelarDBQueryExecutor(FlightClient flightClient) {
        this.flightClient = flightClient;
        LOG.info("Created Executor {}, ", this);
    }

    @Override
    public QueryResults execute(DataSourceQuery q, QueryMethod method) throws SQLException {
        switch (method) {
            case M4:
                return executeM4Query(q);
            case M4_MULTI:
                return executeM4MultiQuery(q);
            case M4OLAP:
                return executeM4OLAPQuery(q);
            case RAW:
                return executeRawQuery(q);
            case MIN_MAX:
                return executeMinMaxQuery(q);
            default:
                throw new UnsupportedOperationException("Unsupported Query Method");
        }
    }

    @Override
    public QueryResults executeM4Query(DataSourceQuery q) throws SQLException {
        return collect(executeM4ModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public QueryResults executeM4MultiQuery(DataSourceQuery q) throws SQLException {
        return collect(executeM4MultiModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public QueryResults executeM4LikeMultiQuery(DataSourceQuery q) throws SQLException {
        return collect(executeM4MultiModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public QueryResults executeM4OLAPQuery(DataSourceQuery q) throws SQLException {
        return collect(executeM4OLAPModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public QueryResults executeRawQuery(DataSourceQuery q) throws SQLException {
        return collect(executeRawModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public QueryResults executeRawMultiQuery(DataSourceQuery q) {
        return null;
    }

    @Override
    public QueryResults executeMinMaxQuery(DataSourceQuery q) throws SQLException {
        return collect(executeMinMaxModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public void initialize(String path) throws SQLException {
    }

    @Override
    public void drop() throws SQLException {

    }

    Comparator<UnivariateDataPoint> compareLists = new Comparator<UnivariateDataPoint>() {
        @Override
        public int compare(UnivariateDataPoint s1, UnivariateDataPoint s2) {
            if (s1==null && s2==null) return 0; //swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };

    public FlightStream executeM4OLAPModelarDBQuery(ModelarDBQuery q) throws SQLException {
        String sql = q.m4WithOLAPQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setInt("width", q.getNumberOfGroups());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());

        String query = preparedStatement.getSql();

        return execute(query);
    }

    public FlightStream executeRawModelarDBQuery(ModelarDBQuery q) throws SQLException{
        String sql = q.rawQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();

        return execute(query);
    }

    public FlightStream executeRawMultiModelarDBQuery(ModelarDBQuery q) throws SQLException{
        LOG.debug("Executing {} with {}, ", q, dataset);
        String sql = q.rawMultiQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();
        return execute(query);
    }

    public FlightStream executeM4ModelarDBQuery(ModelarDBQuery q) throws SQLException {
        String sql = q.m4QuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setInt("width", q.getNumberOfGroups());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();

        return execute(query);
    }

    public FlightStream executeM4MultiModelarDBQuery(ModelarDBQuery q) throws SQLException {
        String sql = q.m4MultiQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setInt("width", q.getNumberOfGroups());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();

        return execute(query);
    }

    public FlightStream executeMinMaxModelarDBQuery(ModelarDBQuery q) throws SQLException {
        LOG.debug("Executing {} with {}, ", q, dataset);

        String sql = q.minMaxQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setInt("width", q.getNumberOfGroups());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();
        return execute(query);
    }

    private QueryResults collect(FlightStream flightStream)  {
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<UnivariateDataPoint>> data = new HashMap<>();
        while (flightStream.next()) {
            VectorSchemaRoot vsr = flightStream.getRoot();
            int rowCount = vsr.getRowCount();
        }
        try {
            flightStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        queryResults.setData(data);
        return queryResults;
    }

    public FlightStream execute(String query) throws SQLException {
        LOG.info("Executing Query: \n" + query);
        Ticket ticket = new Ticket(query.getBytes());
        return flightClient.getStream(ticket);
    }

    public AbstractDataset getDataset() {
        return dataset;
    }

    public Comparator<UnivariateDataPoint> getCompareLists() {
        return compareLists;
    }

    public void setDataset(AbstractDataset dataset) {
        this.dataset = dataset;
    }

    @Override
    public String toString() {
        return "ModelarDBQueryExecutor{" +
                ", dataset=" + dataset +
                ", compareLists=" + compareLists +
                '}';
    }

    @Override
    public ArrayList<String> getDbTables() {
        return null;
    }

}
