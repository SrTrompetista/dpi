package dpi.pcap.drivers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
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
				
		configuraFiltros (conf);
		
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
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	void configuraFiltros (Configuration conf)
	{
		conf.set("filtrosIP", "ip.addr;ip.bogus_ip_length;ip.bogus_ip_version;ip.checksum;ip.checksum.status;ip.checksum_bad.expert;ip.checksum_calculated;ip.cipso.categories;ip.cipso.doi;ip.cipso.malformed;ip.cipso.sensitivity_level;ip.cipso.tag_data;ip.cipso.tag_type;ip.cur_rt;ip.cur_rt_host;ip.dsfield;ip.dsfield.dscp;ip.dsfield.ecn;ip.dst;ip.dst_host;ip.empty_rt;ip.empty_rt_host;ip.evil_packet;ip.flags;ip.flags.df;ip.flags.mf;ip.flags.rb;ip.flags.sf;ip.frag_offset;ip.fragment;ip.fragment.count;ip.fragment.error;ip.fragment.multipletails;ip.fragment.overlap;ip.fragment.overlap.conflict;ip.fragment.toolongfragment;ip.fragments;ip.geoip.asnum;ip.geoip.city;ip.geoip.country;ip.geoip.dst_asnum;ip.geoip.dst_city;ip.geoip.dst_country;ip.geoip.dst_isp;ip.geoip.dst_lat;ip.geoip.dst_lon;ip.geoip.dst_org;ip.geoip.isp;ip.geoip.lat;ip.geoip.lon;ip.geoip.org;ip.geoip.src_asnum;ip.geoip.src_city;ip.geoip.src_country;ip.geoip.src_isp;ip.geoip.src_lat;ip.geoip.src_lon;ip.geoip.src_org;ip.hdr_len;ip.host;ip.id;ip.len;ip.nop;ip.opt.addr;ip.opt.ext_sec_add_sec_info;ip.opt.ext_sec_add_sec_info_format_code;ip.opt.flag;ip.opt.id_number;ip.opt.len;ip.opt.len.invalid;ip.opt.mtu;ip.opt.ohc;ip.opt.originator;ip.opt.overflow;ip.opt.padding;ip.opt.ptr;ip.opt.ptr.before_address;ip.opt.ptr.middle_address;ip.opt.qs_func;ip.opt.qs_nonce;ip.opt.qs_rate;ip.opt.qs_reserved;ip.opt.qs_ttl;ip.opt.qs_ttl_diff;ip.opt.qs_unused;ip.opt.ra;ip.opt.rhc;ip.opt.sec_cl;ip.opt.sec_prot_auth_doe;ip.opt.sec_prot_auth_flags;ip.opt.sec_prot_auth_fti;ip.opt.sec_prot_auth_genser;ip.opt.sec_prot_auth_nsa;ip.opt.sec_prot_auth_sci;ip.opt.sec_prot_auth_siop_esi;ip.opt.sec_prot_auth_unassigned;ip.opt.sec_rfc791_comp;ip.opt.sec_rfc791_hr;ip.opt.sec_rfc791_sec;ip.opt.sec_rfc791_tcc;ip.opt.sid;ip.opt.time_stamp;ip.opt.time_stamp_addr;ip.opt.type;ip.opt.type.class;ip.opt.type.copy;ip.opt.type.number;ip.proto;ip.reassembled.data;ip.reassembled.length;ip.reassembled_in;ip.rec_rt;ip.rec_rt_host;ip.src;ip.src_host;ip.src_rt;ip.src_rt_host;ip.subopt_too_long;ip.tos;ip.tos.cost;ip.tos.delay;ip.tos.precedence;ip.tos.reliability;ip.tos.throughput;ip.ttl;ip.ttl.lncb;ip.ttl.too_small;ip.version");
		String[] filtros = conf.get("filtrosIP").split(";");
		int i = 0;
		
		//Encontra qual a posição do campo "ip.src", pois ele é usado como chave.
		for (i = 0; i < filtros.length; i++)			
			if (filtros[i].equals("ip.src"))
				break;
		
		//Salva a posição do ip.src como um INTEIRO.
		conf.set("ip.src", Integer.toString(i));
	}
}
