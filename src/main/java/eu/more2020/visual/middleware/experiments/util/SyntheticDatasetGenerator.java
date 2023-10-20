package eu.more2020.visual.middleware.experiments.util;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class SyntheticDatasetGenerator {

    private static final Logger LOG = LogManager.getLogger(SyntheticDatasetGenerator.class);

    private int rowCount;
    private int colCount;
    private int cardinality;
    private List<Integer> categoricalCols;
    private String file;
    private RandomDataGenerator randomDataGenerator;

    public SyntheticDatasetGenerator(int rowCount, int colCount, int cardinality, String file) {
        this.rowCount = rowCount;
        this.colCount = colCount;
        this.cardinality = cardinality;
        this.file = file;
        randomDataGenerator = new RandomDataGenerator();
        randomDataGenerator.reSeed(0);
    }


    public void generate() throws IOException {
        double generatedDouble = randomDataGenerator.nextUniform(0d, 1000d);
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(file), csvWriterSettings);
        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < colCount; col++) {
                csvWriter.addValue(randomDataGenerator.nextUniform(- cardinality, cardinality - 1));
            }
            csvWriter.writeValuesToRow();
        }
        csvWriter.close();
    }


}
