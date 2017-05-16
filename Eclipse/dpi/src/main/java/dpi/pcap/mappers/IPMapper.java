package dpi.pcap.mappers;

import java.io.IOException;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
		
		Socket socket = new Socket ("172.16.20.39",4242);

		if (!linha.startsWith("ip.addr;"))	
		{
			//Restaura a posição que "ip.src" está presente
			String  ipAddress = conf.get("ip.src");	
			String  horaPacote = conf.get("frame.time_epoch");
			
			String[] filtros = conf.get("filtrosIP").split(";");
			String[] valores = linha.split(";");

			StringBuilder strBuilder = new StringBuilder();

			for (int i = 0; i < valores.length; i++)
			{
				if (valores[i].isEmpty() || valores[i].contains(","))
				{
					//isEmpty: nao e interessante para a aplicacao conter valores nulos
					//contains(","): openTSDB nao suporta strings com virgulas.
				} else
				{
					strBuilder.append(filtros[i]+"="+valores[i]+" ");
				}

			}

			String comandoOpenTSDB = "put ip " 
									 + valores[Integer.parseInt(horaPacote)].split("\\.")[0]
									 + " 1 " 
									 + strBuilder 
									 + "\n";
			
			Log log = LogFactory.getLog(getClass());
			log.info("Mapper: Comando -> " + comandoOpenTSDB);
			
			try
			{
				socket.getOutputStream().write(comandoOpenTSDB.getBytes());
			} catch (Exception e)
			{
				e.printStackTrace();
			}
			
			//map: <key, value> -> <ip.src, ip.addr=172.16...;ip.checksum=0x0002;...>
			context.write(new Text(valores[Integer.parseInt(ipAddress)]),  new Text(strBuilder.toString()));
		}	
	}
}
