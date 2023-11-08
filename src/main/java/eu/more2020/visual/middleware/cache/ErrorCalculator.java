package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.MaxErrorEvaluator;
import eu.more2020.visual.middleware.domain.PixelColumn;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.domain.TimeInterval;
import eu.more2020.visual.middleware.domain.ViewPort;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.apache.arrow.flatbuf.Int;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErrorCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private MaxErrorEvaluator maxErrorEvaluator;
    private boolean hasError = true;
    private long pixelColumnInterval;

    public Map<Integer, Double> calculateValidColumnsErrors(Query query, List<PixelColumn> pixelColumns, ViewPort viewPort, long pixelColumnInterval) {
        // Calculate errors using processed data
        List<Integer> measures = query.getMeasures();
        maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        List<List<Integer>> pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        // Find the part of the query interval that is not covered by the spans in the interval tree.
        Map<Integer, Double> error = new HashMap<>();
        for (int m : measures) error.put(m, 0.0);
        int validColumns = 0;
        for (List<Integer> pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError == null) continue;
            int i = 0;
            for (int m : measures) {
                final Double data = error.get(m);
                final Integer val =  pixelColumnError.get(i);
                if(val == null) break;// no measure data
                error.put(m, data + val);
                i++;
            }
            if(i == measures.size()) validColumns ++; // Column is valid if it has data for all measures
        }
        LOG.debug("Valid columns: {}", validColumns);
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * validColumns);
            LOG.info("Measure has error: {}", measureError);
            hasError = measureError > 1 - query.getAccuracy();
            error.put(m, measureError);
        }
        return error;
    }


    public Map<Integer, Double> calculateTotalError(Query query, List<PixelColumn> pixelColumns, ViewPort viewPort, long pixelColumnInterval) {
        // Calculate errors using processed data
        List<Integer> measures = query.getMeasures();
        maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        List<List<Integer>> pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        // Find the part of the query interval that is not covered by the spans in the interval tree.
        Map<Integer, Double> error = new HashMap<>();
        for (int m : measures) error.put(m, 0.0);
        for (List<Integer> pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError == null) continue;
            int i = 0;
            for (int m : measures) {
                final Double data = error.get(m);
                final Integer val = pixelColumnError.get(i);
                if(val == null) break;// no measure data
                error.put(m, data + val);
                i++;
            }
        }
        hasError = true;
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * viewPort.getWidth());
            LOG.info("Measure has error: {}", measureError);
            hasError = measureError > 1 - query.getAccuracy();
            error.put(m, measureError);
        }
        return error;
    }

    public List<TimeInterval> getMissingIntervals() {
        List<TimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.groupIntervals(pixelColumnInterval, missingIntervals);
        LOG.info("Unable to Determine Errors: " + missingIntervals);
        return missingIntervals;
    }


    public boolean hasError(){
        return hasError;
    }
}
