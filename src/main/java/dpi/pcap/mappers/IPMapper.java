package dpi.pcap.mappers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IPMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
	public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException 
	{
		Log log = LogFactory.getLog(IPMapper.class);
		
		Configuration conf = context.getConfiguration();
		
		String linha = record.toString();
		log.info("IPMAPPER: Leu a linha: "+linha);
		
		if (linha.startsWith("ip.addr;"))
		{
			log.info("IPMAPPER: A linha começou com ip.addr."); 
			
			conf.set("filtros", linha);
			conf.set("teste", "teste");
			log.info("IPMAPPER: Configurado o valor de filtros para :"+linha);
			
			String[] filtros = linha.split(";");
			int i = 0;
			
			for (i = 0; i < filtros.length; i++)
				if (filtros[i].equals("ip.src"))
					break;
			
			conf.set("ip.src", Integer.toString(i));
			log.info("IPMAPPER: Foi encontrado o campo ip.src no numero: "+i);
		}
		
		else
		{
			String[] valores = linha.split(";");
			String  ipAddress = conf.get("ip.src");

			log.info("IPMAPPER: Ip de origem: "+ valores[Integer.parseInt(ipAddress)]);
			
			context.write(new Text(valores[Integer.parseInt(ipAddress)]),  new Text(linha));
		}	
	}
}
