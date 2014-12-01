package com.spotify.data.crunch;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ParquetTest {
  /**
   * $ hadoop jar parquet-test-1.0.0-SNAPSHOT.jar com.spotify.data.crunch.ParquetTest
   */
  public static void main(String args[]) throws Exception {
    cleanUp();

    setUpInput();
    writeParquet();
    checkOutput();
  }

  private static void cleanUp() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(new Path("/tmp/parquet_test_avro_input"), true);
    fs.delete(new Path("/tmp/parquet_test_output"), true);
  }

  private static void checkOutput() throws IOException {
    Path file = new Path("/tmp/parquet_test_output/part-m-00000.parquet");
    try (AvroParquetReader<GenericData.Record> reader = new AvroParquetReader<>(file)) {
      List<GenericData.Record> actualRecords = Lists.newArrayList();
      GenericData.Record record;
      while ((record = reader.read()) != null) {
        System.out.println("Record: " + record);
        actualRecords.add(record);
      }
      // Record: {"fieldA": "A2222", "fieldB": "B2222", "fieldC": 1111}
      // Record: {"fieldA": "A2222", "fieldB": "B2222", "fieldC": 2222}


      System.out.println("Expected: [{\"fieldA\": \"A1111\", \"fieldB\": \"B1111\", \"fieldC\": 1111}, {\"fieldA\": \"A2222\", \"fieldB\": \"B2222\", \"fieldC\": 2222}]");
      System.out.println("Actual:   " + actualRecords);
      // Actual:   [{"fieldA": "A2222", "fieldB": "B2222", "fieldC": 1111}, {"fieldA": "A2222", "fieldB": "B2222", "fieldC": 2222}]
    }
  }

  private static void writeParquet() {
    Pipeline pipeline = new MRPipeline(ParquetTest.class);

    Source<GenericData.Record> source = From.avroFile(new Path("/tmp/parquet_test_avro_input"));
    PCollection<GenericData.Record> collection = pipeline.read(source);

    collection.write(new AvroParquetFileTarget(new Path("/tmp/parquet_test_output")));

    pipeline.done();
  }

  private static void setUpInput() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    fs.mkdirs(new Path("/tmp/parquet_test_avro_input"));

    Path pt = new Path("/tmp/parquet_test_avro_input/data.avro");
    try (
        InputStream inputStream = ParquetTest.class.getResourceAsStream("/data.avro");
        FSDataOutputStream outputStream = fs.create(pt, true)
    ) {
      IOUtils.copy(inputStream, outputStream);
    }
  }
}
