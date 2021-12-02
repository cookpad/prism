package com.cookpad.prism.merge;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import com.cookpad.prism.record.Record;
import com.cookpad.prism.record.RecordReaderFactory;
import com.cookpad.prism.record.RecordWriterFactory;
import com.cookpad.prism.record.Schema;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class ParquetFileMerger {
    private final RecordWriterFactory recordWriterFactory;
    private final RecordReaderFactory recordReaderFactory;

    public void merge(Schema schema, Path inputFilePathA, Path inputFilePathB, Path outputFilePath) throws IOException {
        try (ParquetWriter<Record> writer = recordWriterFactory.build(schema, outputFilePath)) {
            try (ParquetReader<Record> readerA = recordReaderFactory.build(schema, inputFilePathA);
                    ParquetReader<Record> readerB = recordReaderFactory.build(schema, inputFilePathB)) {
                new ParquetMerger(readerA, readerB, writer).merge();
            }
        }
    }
}
