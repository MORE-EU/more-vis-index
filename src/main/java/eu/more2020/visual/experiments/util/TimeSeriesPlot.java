package eu.more2020.visual.experiments.util;

import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.*;
import tech.tablesaw.plotly.traces.ScatterTrace;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;

public class TimeSeriesPlot {

    private final String outFolder;
    public TimeSeriesPlot(String outFolder) {
        this.outFolder = outFolder;
    }

    public void plot(String filePath) throws IOException {
        Table timeSeries = Table.read().csv(filePath);
        String colName  = timeSeries.column(1).name();
        String name = filePath.replace(".csv", "").replace(outFolder, "");


        Axis xAxis = Axis.builder().showGrid(false).visible(false).build();
        Axis yAxis = Axis.builder().showGrid(false).visible(false).build();

        Layout layout = Layout.builder()
                .height(600)
                .width(960)
                .xAxis(xAxis)
                .plotBgColor("white")
                .yAxis(yAxis)
                .grid(Grid.builder().rows(1).columns(1).build())
                .build();

        ScatterTrace trace = ScatterTrace.builder(timeSeries.column(0), timeSeries.column(1))
                .mode(ScatterTrace.Mode.LINE)
                .fill(ScatterTrace.Fill.NONE)
                .showLegend(false)
                .marker(
                        Marker.builder()
                                .color("black")
                                .build())
                .build();

        String htmlFileName = Paths.get(outFolder, name + ".html").toString();
        Plot.show(new Figure(layout, trace), new File(htmlFileName));
    }

    public void build(String filePath) throws IOException {
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

