package eu.more2020.visual.repository;

import eu.more2020.visual.domain.*;
import eu.more2020.visual.domain.Detection.ChangepointDetection;
import eu.more2020.visual.domain.Detection.DeviationDetection;
import eu.more2020.visual.domain.Detection.RangeDetection;

import java.io.IOException;
import java.util.List;

/**
 * Repository for the Dataset entity.
 */
@SuppressWarnings("unused")
public interface ToolsRepository {

    List<Changepoint> getManualChangepoints(String id);

    List<DataPoint> forecasting(String id);

    List<Changepoint> changepointDetection(ChangepointDetection changepoints) throws IOException;

    List<DataPoint> soilingDetection(DeviationDetection deviationDetection);

    List<DataPoint> yawMisalignmentDetection(RangeDetection rangeDetection);

}
