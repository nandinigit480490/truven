package com.dbo.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws IOException {
		
		String Database = "DBName";
		String HeaderFile =  "Headers_TAB_SEP.csv";
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		Path currentRelativePath = Paths.get("");
		String absolutePath = currentRelativePath.toAbsolutePath().toString();
		System.out.println("Current relative path is: " + absolutePath);

		String csvFile = absolutePath+"/"+HeaderFile;
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\\t";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			br.readLine();
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] row = line.split(cvsSplitBy);
				String fileName = row[0];
				String headers = row[1];

				sparkReadTxtFile(absolutePath, fileName, headers,Database);

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void sparkReadTxtFile(String Basepath, String fileName, String headers,String Database) {

		String Headers[] = headers.split("\\,");

		List<StructField> dd = new ArrayList<StructField>();

		for (String h : Headers) {
			StructField structField = new StructField(h, DataTypes.StringType, true, Metadata.empty());
			dd.add(structField);
		}
		StructField[] structFields = dd.stream().toArray(StructField[]::new);

		StructType structType = new StructType(structFields);
		// TODO Auto-generated method stub
		String mst = "local[*]";
		// String mst ="spark://192.168.0.108:7077";
		SparkConf conf = new SparkConf().setAppName("cust data").setMaster(mst);
		SparkSession spark = SparkSession.builder().config(conf)
				.config("spark.mongodb.input.uri", "mongodb://192.168.0.21/F.coll")
				.config("spark.mongodb.output.uri","mongodb://192.168.0.21/F.coll")

				.getOrCreate();
 
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		System.out.println("---> reading file name " + fileName);

		Dataset<Row> d = spark.read().option("header", "false").option("delimiter", "|").schema(structType)
				.csv(Basepath + "/DATA/" + fileName.toLowerCase());

		Map<String, String> writeOverrides_AddressGeos_ERR = new HashMap<String, String>();
		writeOverrides_AddressGeos_ERR.put("database",Database);
		writeOverrides_AddressGeos_ERR.put("collection",fileName.split("\\.")[0]);
		writeOverrides_AddressGeos_ERR.put("writeConcern.w", "majority");
		
		WriteConfig writeConfig_AddressGeos_ERR = WriteConfig.create(jsc)
				.withOptions(writeOverrides_AddressGeos_ERR);
		
		MongoSpark.save(d,writeConfig_AddressGeos_ERR);

		
		d.show(false); 
	}
}
