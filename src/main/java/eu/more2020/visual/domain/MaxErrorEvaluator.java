package eu.more2020.visual.domain;
import com.google.common.collect.*;

import java.util.List;

/**
 * Class that computes the maximum number of pixel errors.
 */
public class MaxErrorEvaluator {

    private final List<Integer> measures;
    private final double[] error;
    private ViewPort viewPort;

    private List<PixelColumn> pixelColumns;



    public MaxErrorEvaluator(List<Integer> measures, ViewPort viewPort) {
        this.measures = measures;
        this.viewPort = viewPort;
        int length = measures.size();
        error = new double[length];
    }

    public void computeMaxError() {




    }






    private int getMeasureIndex(int m) {
        return measures.indexOf(m);
    }

    public double getError(int m){
        // Create the superset of missing + false
        return error[getMeasureIndex(m)] / (viewPort.getHeight() * viewPort.getWidth());
    }


}

