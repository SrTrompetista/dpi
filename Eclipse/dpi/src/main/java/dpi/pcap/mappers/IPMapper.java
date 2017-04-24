package dpi.pcap.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class IPMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	@Override
	public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException 
	{	
		//Configuration nos permite usar o contexto para armazenar valores.
		Configuration conf = context.getConfiguration();	
		
		String linha = record.toString();	
		
		if (!linha.startsWith("ip.addr;"))	
		{
			//Restaura a posição que "ip.src" está presente
			String  ipAddress = conf.get("ip.src");		
			String[] filtros = conf.get("filtrosIP").split(";");
			String[] valores = linha.split(";");

			StringBuilder strBuilder = new StringBuilder();

			for (int i = 0; i < valores.length; i++)
			{
				if (valores[i].isEmpty())
				{
					//strBuilder.append(filtros[i]+"=vazio;");
				} else
				{
					strBuilder.append(filtros[i].replaceAll("\\.", "-")+"="+valores[i]+";");
				}

			}

			//map: <key, value> -> <ip.src, ip.addr=172.16...;ip.checksum=0x0002;...>
			context.write(new Text(valores[Integer.parseInt(ipAddress)]),  new Text(strBuilder.toString()));
		}	
	}
}
