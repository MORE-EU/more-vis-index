package eu.more2020.visual.experiments.util;

import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.ScatterTrace;
import tech.tablesaw.plotly.traces.Trace;

public class TimeSeriesPlot {

    public TimeSeriesPlot() {}

    public void build(String filePath) {
        Table timeSeries = Table.read().csv(filePath);
        Layout layout = Layout.builder()
                .title("Results")
                .height(600)
                .width(800)
                .build();

        ScatterTrace trace = ScatterTrace.builder(timeSeries.column("timestamp"), timeSeries.column("1"))
                .mode(ScatterTrace.Mode.LINE)
                .build();

        Plot.show(new Figure(layout, trace));
    }

}

