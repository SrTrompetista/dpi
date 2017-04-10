package dpi.pcap.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class IPReducer extends Reducer<Text, Text, Text, Text>
{ 
	@Override
	public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException
	{
		Log log = LogFactory.getLog(IPReducer.class);
		
		String filtros = "ip.addr;ip.bogus_ip_length;ip.bogus_ip_version;ip.checksum;ip.checksum.status;ip.checksum_bad.expert;ip.checksum_calculated;ip.cipso.categories;ip.cipso.doi;ip.cipso.malformed;ip.cipso.sensitivity_level;ip.cipso.tag_data;ip.cipso.tag_type;ip.cur_rt;ip.cur_rt_host;ip.dsfield;ip.dsfield.dscp;ip.dsfield.ecn;ip.dst;ip.dst_host;ip.empty_rt;ip.empty_rt_host;ip.evil_packet;ip.flags;ip.flags.df;ip.flags.mf;ip.flags.rb;ip.flags.sf;ip.frag_offset;ip.fragment;ip.fragment.count;ip.fragment.error;ip.fragment.multipletails;ip.fragment.overlap;ip.fragment.overlap.conflict;ip.fragment.toolongfragment;ip.fragments;ip.geoip.asnum;ip.geoip.city;ip.geoip.country;ip.geoip.dst_asnum;ip.geoip.dst_city;ip.geoip.dst_country;ip.geoip.dst_isp;ip.geoip.dst_lat;ip.geoip.dst_lon;ip.geoip.dst_org;ip.geoip.isp;ip.geoip.lat;ip.geoip.lon;ip.geoip.org;ip.geoip.src_asnum;ip.geoip.src_city;ip.geoip.src_country;ip.geoip.src_isp;ip.geoip.src_lat;ip.geoip.src_lon;ip.geoip.src_org;ip.hdr_len;ip.host;ip.id;ip.len;ip.nop;ip.opt.addr;ip.opt.ext_sec_add_sec_info;ip.opt.ext_sec_add_sec_info_format_code;ip.opt.flag;ip.opt.id_number;ip.opt.len;ip.opt.len.invalid;ip.opt.mtu;ip.opt.ohc;ip.opt.originator;ip.opt.overflow;ip.opt.padding;ip.opt.ptr;ip.opt.ptr.before_address;ip.opt.ptr.middle_address;ip.opt.qs_func;ip.opt.qs_nonce;ip.opt.qs_rate;ip.opt.qs_reserved;ip.opt.qs_ttl;ip.opt.qs_ttl_diff;ip.opt.qs_unused;ip.opt.ra;ip.opt.rhc;ip.opt.sec_cl;ip.opt.sec_prot_auth_doe;ip.opt.sec_prot_auth_flags;ip.opt.sec_prot_auth_fti;ip.opt.sec_prot_auth_genser;ip.opt.sec_prot_auth_nsa;ip.opt.sec_prot_auth_sci;ip.opt.sec_prot_auth_siop_esi;ip.opt.sec_prot_auth_unassigned;ip.opt.sec_rfc791_comp;ip.opt.sec_rfc791_hr;ip.opt.sec_rfc791_sec;ip.opt.sec_rfc791_tcc;ip.opt.sid;ip.opt.time_stamp;ip.opt.time_stamp_addr;ip.opt.type;ip.opt.type.class;ip.opt.type.copy;ip.opt.type.number;ip.proto;ip.reassembled.data;ip.reassembled.length;ip.reassembled_in;ip.rec_rt;ip.rec_rt_host;ip.src;ip.src_host;ip.src_rt;ip.src_rt_host;ip.subopt_too_long;ip.tos;ip.tos.cost;ip.tos.delay;ip.tos.precedence;ip.tos.reliability;ip.tos.throughput;ip.ttl;ip.ttl.lncb;ip.ttl.too_small;ip.version";
		String[] valoresTotais = filtros.split(";");
		
		Map<String, String> myMap = new HashMap<String, String>();
		String valorExistente;
		for (int i = 0; i < valoresTotais.length; i ++)
		{
			myMap.put(valoresTotais[i], "");
		}
		
		for (Text record : records)
		{
			log.info("IPREDUCER: analisando a linha: "+record.toString());
			String[] valores = record.toString().split(";");
			
			for (int i = 0; i < valores.length; i++)
			{
				if (valores[i].isEmpty())
				{
					//log.info("IPMAPPER: Valores totais: "+valoresTotais[i]);
					//valoresTotais[i] = valoresTotais[i].concat("_null");
					log.info("IPMAPPER: Encontrou um valor vazio.");
					//log.info("IPMAPPER: Valores totais: "+valoresTotais[i]);
				}
				else if (valores[i].startsWith("\"") && valores[i].endsWith("\""))
				{
					valorExistente = myMap.get(valoresTotais[i]);
					myMap.put(valoresTotais[i], valorExistente == "" ? valores[i] : valorExistente + ", " + valores[i]);
				}
				
			}
		}
		
		JSONObject hue = new JSONObject(myMap);
		
		log.info("IPREDUCER: HASHMAP >>>>>>>> " + hue.toString());
		/*
		for (String name: myMap.keySet())
		{

            String chave =name.toString();
            String value = myMap.get(name).toString();  
           // System.out.println(chave + " " + value);  
            log.info("IPREDUCER: HASHMAP >>>>>>>> " + chave + " " + value);
		}
		*/
		
		/*
		ArrayList<String> list = new ArrayList<String>();
		list.add("john");
		list.add("mat");
		list.add("jason");
		list.add("matthew");
		
		JSONObject school = new JSONObject();

		
		try {
			school.put("class","4");
			school.put("name", new JSONArray(list));
			log.info("IPMAPPER: Valores totais: "+school.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for (int i = 0; i < valoresTotais.length; i ++)
		{
			valoresTotais[i] = valoresTotais[i].substring(0, valoresTotais[i].length()-1) + "],";
		}
		*/
		
		valoresTotais[valoresTotais.length-1] = valoresTotais[valoresTotais.length-1].substring(0, valoresTotais[valoresTotais.length-1].length()-1) + "}";
		
		context.write(new Text(key), new Text(Arrays.toString(valoresTotais)));
		
	}
}
