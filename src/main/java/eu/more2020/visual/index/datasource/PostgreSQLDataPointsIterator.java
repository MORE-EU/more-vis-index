package eu.more2020.visual.index.datasource;

import eu.more2020.visual.index.domain.DataPoint;
import eu.more2020.visual.index.domain.ImmutableDataPoint;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class PostgreSQLDataPointsIterator implements Iterator<DataPoint> {

    private final ResultSet resultSet;
    private final List<Integer> measures;

    public PostgreSQLDataPointsIterator(List<Integer> measures, ResultSet resultSet){
        this.measures = measures;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return !(resultSet.isAfterLast() || resultSet.isLast());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public ImmutableDataPoint next() {
        double[] values = new double[measures.size()];
        long datetime = 0L;
        try {
            int i = 0;
            while (i < measures.size() && resultSet.next()) {
                datetime = resultSet.getLong(2);
                Double val = resultSet.getObject(3) == null ? null : resultSet.getDouble(3);
                if(val == null) {
                    i++;
                    continue;
                }
                values[i] = val;
                i ++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new ImmutableDataPoint(datetime, values);
    }
}