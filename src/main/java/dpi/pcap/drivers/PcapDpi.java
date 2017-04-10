package dpi.pcap.drivers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dpi.pcap.mappers.IPMapper;
import dpi.pcap.reducers.IPReducer;

public class PcapDpi extends Configured implements Tool 
{
	public static void main (String[] args) throws Exception
	{
		int exitCode = ToolRunner.run (new PcapDpi(), args);
		System.exit (exitCode);
	}
	
	public int run (String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance (conf, "DPI analyzer.");
		
		Log log = LogFactory.getLog(getClass());
		log.info("DRIVER: Iniciando o driver.");
		
		
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		log.info("DRIVER: Arquivo(s) de entrada: "+args[0]);
		
		job.setMapperClass(IPMapper.class);
		log.info("DRIVER: Classe mapper adicionada: IPMapper.class");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(IPReducer.class);
		log.info("DRIVER: Classe reducer adicionada: IPReducer.class");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
}
