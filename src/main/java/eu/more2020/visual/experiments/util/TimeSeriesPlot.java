package eu.more2020.visual.experiments.util;

import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.components.Margin;
import tech.tablesaw.plotly.traces.ScatterTrace;
import tech.tablesaw.plotly.traces.Trace;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class TimeSeriesPlot {

    public TimeSeriesPlot() {}

    public void plot(String filePath){
        Table timeSeries = Table.read().csv(filePath);
        String colName  = timeSeries.column(1).name();
        Layout layout = Layout.builder()
                .title(filePath + ": " + colName)
                .height(600)
                .width(960)
                .build();

        ScatterTrace trace = ScatterTrace.builder(timeSeries.column(0), timeSeries.column(1))
                .mode(ScatterTrace.Mode.LINE)
                .fill(ScatterTrace.Fill.NONE)
                .build();

        Plot.show(new Figure(layout, trace));
    }

    public void build(String filePath) {
        if (new File(filePath).isDirectory()) buildDirectory(filePath);
        else plot(filePath);
    }

    public void buildDirectory(String filePath) {
        Path pathDir = new File(filePath).toPath();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pathDir)) {
            for (Path file : stream) {
                plot(file.toString());
            }
        } catch (DirectoryIteratorException | IOException x) {
            System.err.println(x);
        }
    }

}

