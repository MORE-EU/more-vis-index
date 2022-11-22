package eu.more2020.visual.domain.Dataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ParquetDataset extends AbstractDataset{

    private Map<String, Integer> fieldNameToId;

    public ParquetDataset(String path, String id, String name, String timeFormat) throws IOException {
       super(path, id, name, timeFormat);
    }

    public void convertMeasures(String parquetTimeCol, List<String> parquetMeasures) throws IOException {
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(getPath()), new Configuration()));
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        fieldNameToId = new HashMap<>();
        List<Type> fields = schema.getFields();
        String header[] = new String[schema.getFields().size()];
        int i = 0;
        for (Type field : fields){
            header[i] = field.getName();
            fieldNameToId.put(field.getName(), i ++);
        }
        this.setHeader(header);
        this.setTimeCol(fieldNameToId.get(parquetTimeCol));
        this.setMeasures(parquetMeasures.stream().map(fieldNameToId::get).collect(Collectors.toList()));
    }


}
