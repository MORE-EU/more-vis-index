package eu.more2020.visual.middleware.cache;

import eu.more2020.visual.middleware.domain.*;
import eu.more2020.visual.middleware.domain.Query.Query;
import eu.more2020.visual.middleware.util.DateTimeUtil;
import org.apache.arrow.flatbuf.Int;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ErrorCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private MaxErrorEvaluator maxErrorEvaluator;
    private boolean hasError = true;
    private long pixelColumnInterval;
    private List<List<Integer>> pixelColumnErrors;
    private Map<Integer, Double> error;
    private List<Integer> measuresWithError;

    public Map<Integer, Double> calculateValidColumnsErrors(Query query, List<PixelColumn> pixelColumns, ViewPort viewPort, long pixelColumnInterval) {
        // Calculate errors using processed data
        List<Integer> measures = query.getMeasures();
        maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        // Find the part of the query interval that is not covered by the spans in the interval tree.
        error = new HashMap<>();
        int[] validColumns = new int[measures.size()];
        int i = 0;
        for (int m : measures) {
            error.put(m, 0.0);
            validColumns[i] = 0;
            i ++;
        }

        for (List<Integer> pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError == null) continue;
            i = 0;
            for (int m : measures) {
                final Double data = error.get(m);
                final Integer val =  pixelColumnError.get(i);
                if(val == null) break; // no measure data
                error.put(m, data + val);
                validColumns[i] += 1;
                i++;
            }
        }
        measuresWithError = new ArrayList<>();
        LOG.debug("Valid columns: {}", validColumns);
        i = 0;
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * validColumns[i]);
            LOG.info("Measure has error: {}", measureError);
            hasError = measureError > 1 - query.getAccuracy();
            if(hasError) measuresWithError.add(m);
            error.put(m, measureError);
            i ++;
        }
        return error;
    }


    public Map<Integer, Double> calculateTotalError(Query query, List<PixelColumn> pixelColumns, ViewPort viewPort, long pixelColumnInterval) {
        // Calculate errors using processed data
        List<Integer> measures = query.getMeasures();
        maxErrorEvaluator = new MaxErrorEvaluator(measures, viewPort, pixelColumns);
        this.pixelColumnInterval = pixelColumnInterval;
        pixelColumnErrors = maxErrorEvaluator.computeMaxPixelErrorsPerColumnAndMeasure();
        error = new HashMap<>();
        for (int m : measures) error.put(m, 0.0);
        for (List<Integer> pixelColumnError : pixelColumnErrors) {
            if(pixelColumnError == null) continue; // No data was found for this pixel column
            int i = 0;
            for (int m : measures) {
                final Double data = error.get(m);
                final Integer val = pixelColumnError.get(i);
                error.put(m, data + val);
                i++;
            }
        }
        hasError = true;
        measuresWithError = new ArrayList<>();
        for (int m : measures) {
            double measureError = error.get(m) / (viewPort.getHeight() * viewPort.getWidth());
            LOG.info("Measure has error: {}", measureError);
            hasError = measureError > 1 - query.getAccuracy();
            if(hasError) measuresWithError.add(m);
            error.put(m, measureError);
        }
        return error;
    }

    public List<MultivariateTimeInterval> getMissingIntervals() {
        List<MultivariateTimeInterval> missingIntervals = maxErrorEvaluator.getMissingRanges();
        missingIntervals = DateTimeUtil.groupMultiIntervals(pixelColumnInterval, missingIntervals);
        LOG.info("Unable to Determine Errors: " + missingIntervals);
        return missingIntervals;
    }


    public boolean hasError(){
        return hasError;
    }

    public List<Integer> getMeasuresWithError() {
        return measuresWithError;
    }
}
