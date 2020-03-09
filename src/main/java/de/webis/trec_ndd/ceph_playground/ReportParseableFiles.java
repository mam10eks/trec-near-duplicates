package de.webis.trec_ndd.ceph_playground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.archivespark.sparkling.warc.WarcLoader;
import org.archive.archivespark.sparkling.warc.WarcRecord;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import avro.shaded.com.google.common.collect.Iterators;
import de.webis.corpus_internet_archive.S3Files;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.collection.Iterator;

public class ReportParseableFiles {
	
	public static void main(String[] args) {
		Namespace parsedArgs = parseArguments(args);		
		S3Files s3Files = new S3Files(
			parsedArgs.getString("accessKey"),
			parsedArgs.getString("secretKey"),
			parsedArgs.getString("bucketName")
		);

		try(JavaSparkContext sc = context()) {
			sc.parallelize(s3Files.filesInBucket())
				.map(i -> new FileReport(i, s3Files))
				.map(i -> i.toString())
				.saveAsTextFile(parsedArgs.getString("outputFile"));
		}
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class FileReport {
		private String bucketName;
		private String key;
		private Boolean parseable;
		private String warcVersion;
		private Integer documentCount;
		
		public FileReport(S3ObjectSummary summary, S3Files files) {
			bucketName = summary.getBucketName();
			key = summary.getKey();
			parseable = false;
			documentCount = null;
			warcVersion = null;
			
			System.out.println("--> (" + summary.getBucketName() + ";" + summary.getKey() + ")");
			System.err.println("--> (" + summary.getBucketName() + ";" + summary.getKey() + ")");

			try {
				Iterator<WarcRecord> iter = WarcLoader.load(files.rawContent(summary));
				documentCount = 0;
				while(iter.hasNext()) {
					WarcRecord next = iter.next();
					if(next.isResponse()) {
						documentCount++;
					}
				}
				
				warcVersion = files.warcVersion(summary).name();
				parseable = true;
			} catch(Throwable e) {}
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("ReportParseableFiles");

		return new JavaSparkContext(conf);
	}
	
	private static Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("ReportParseableFiles")
			.build()
			.defaultHelp(true)
			.description("Report warc files in ceph that are parseable.");

		parser.addArgument("--bucketName")
			.required(Boolean.TRUE)
			.help("Specify the name of the s3-bucket where all warcs are stored.");
		parser.addArgument("--accessKey")
			.required(Boolean.TRUE)
			.help("Specify the s3 access key.");
		parser.addArgument("--secretKey")
			.required(Boolean.TRUE)
			.help("Specify the s3 secret key.");
		parser.addArgument("--outputFile")
			.required(Boolean.TRUE)
			.help("Specify the output directory.");
		
		return parser.parseArgsOrFail(args);
	}
}
