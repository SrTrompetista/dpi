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
			conf.set("filtros", linha);
			
			String[] filtros = linha.split(";");
			int i = 0;
			
			for (i = 0; i < filtros.length; i++)
				if (filtros[i].equals("ip.src"))
					break;
			
			conf.set("ip.src", Integer.toString(i));
		}
		
		else
		{
			String  ipAddress = conf.get("ip.src");
			String[] filtros = conf.get("filtros").split(";");
			String[] valores = linha.split(";");

			StringBuilder strBuilder = new StringBuilder();

			for (int i = 0; i < valores.length; i++)
			{
				if (valores[i].isEmpty())
				{
					strBuilder.append(filtros[i]+"=vazio;");
				} else
				{
					strBuilder.append(filtros[i]+"="+valores[i]+";");
				}

			}

			//map: <key, value> -> <ip.src, ip.addr=172.16...;ip.checksum=0x0002;...>
			log.info("IPMAPPER: Escreveu o par: <"+valores[Integer.parseInt(ipAddress)]+", "+strBuilder.toString()+">");
			context.write(new Text(valores[Integer.parseInt(ipAddress)]),  new Text(strBuilder.toString()));
		}	
	}
}
