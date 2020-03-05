package de.webis.trec_ndd.ceph_playground;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import de.webis.corpus_internet_archive.S3Files;
import de.webis.corpus_internet_archive.WARCReader;
import de.webis.corpus_internet_archive.WarcRecord;
import de.webis.trec_ndd.similarity.TextProfileSignatureSimilarity;
import de.webis.trec_ndd.spark.DocumentGroup;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;

public class DeduplicateWebDocumentsInWarcFiles {
	public static void main(String[] args) {
		Namespace parsedArgs = parseArguments(args);
		S3Files s3Files = new S3Files(
			parsedArgs.getString("accessKey"),
			parsedArgs.getString("secretKey"),
			parsedArgs.getString("bucketName")
		);

		try(JavaSparkContext sc = context()) {
			JavaRDD<DocumentGroup> rdd = sc.parallelize(s3Files.filesInBucket().subList(0, 1000))
				.flatMap(i -> WARCReader.parse(s3Files.content(i)))
				.map(i -> new WebDocument(i))
				.groupBy(i -> i.getHash())
				.map(i -> docGroup(i))
				.filter(dg -> dg.ids.size() > 1);
			
			rdd.map(i -> i.toString())
				.saveAsTextFile(parsedArgs.getString("outputFile"));
		}
	}
	
	private static DocumentGroup docGroup(Tuple2<String, Iterable<WebDocument>> i) {
		DocumentGroup ret = new DocumentGroup();
		
		ret.setHash(i._1);
		List<String> ids = ImmutableList.copyOf(i._2).stream().map(j -> j.getId()).collect(Collectors.toList());
		ret.setIds(new ArrayList<String>(ids));
		
		return ret;
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("DedupWebDocsInCeph");

		return new JavaSparkContext(conf);
	}

	@Data
	@SuppressWarnings("serial")
	public static class WebDocument implements Serializable {
		private final String hash;
		private final String url;
		private final String crawlingTimestamp;
		
		public WebDocument(WarcRecord record) {
			String content = record.getContent();
			this.hash = TextProfileSignatureSimilarity.textProfileSignatureString(content);
			this.url = record.getHeader().getHeaderMetadataItem("WARC-Target-URI");
			this.crawlingTimestamp = record.getHeader().getHeaderMetadataItem("WARC-Date");
		}
		
		public String getId() {
			return url + ":::" + crawlingTimestamp;
		}
	}
	
	private static Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("DedupWebDocsInCeph")
			.build()
			.defaultHelp(true)
			.description("Deduplicate web documents listed in ceph.");

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
