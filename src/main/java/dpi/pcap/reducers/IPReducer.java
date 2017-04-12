package dpi.pcap.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class IPReducer extends Reducer<Text, Text, NullWritable, Text>
{ 
	@Override
	public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException
	{
		Log log = LogFactory.getLog(IPReducer.class);

		String chave, valor;
		String valorExistente;
		String[] valoresTotais, temp;
		JSONArray jsonExistente;

		
		Map<String, JSONArray> myMap2 = new HashMap<String, JSONArray>();

		for (Text record : records)
		{
			String[] parChaveValor = record.toString().split(";");

			for (int i = 0; i < parChaveValor.length; i++)
			{

				temp = parChaveValor[i].split("=");
				chave = temp[0];
				valor = temp[1];

				if (myMap2.containsKey(chave))
				{					
					jsonExistente = myMap2.get(chave);
					jsonExistente.put(valor);
					myMap2.put(chave, jsonExistente);
				} else
				{
					JSONArray list = new JSONArray();
					list.put(valor);
					myMap2.put (chave, list);
				}
			}
		}
		
		Map<String, JSONObject> myMap3 = new HashMap<String, JSONObject>();
		
		String[] hierarquia;
		for (Map.Entry<String, JSONArray> entry : myMap2.entrySet())
		{
			chave = entry.getKey();
			hierarquia = chave.split("\\.");
			
			for (int i = hierarquia.length -1; i >= 0; i--)
			{
				if (i == hierarquia.length-1)
				{
					try 
					{
						JSONObject novoJSON = new JSONObject();
						novoJSON.put(hierarquia[i], entry.getValue());
						myMap3.put(hierarquia[i], novoJSON);
						log.info("i = "+i+". Criando novo JSON: "+hierarquia[i]+", "+novoJSON.toString());
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				} else if (i != hierarquia.length-1)
				{
					if (!myMap3.containsKey(hierarquia[i]))
					{
						try
						{
							log.info("Map não contém JSON "+hierarquia[i]);
							JSONObject novoJSON = new JSONObject();
							novoJSON.put(hierarquia[i], myMap3.get(hierarquia[i+1]));
							myMap3.put(hierarquia[i], novoJSON);
							myMap3.remove(hierarquia[i+1]);
							log.info("i = "+i+". Criando novo JSON: "+hierarquia[i]+", "+novoJSON.toString());
						} catch (JSONException e)
						{
							e.printStackTrace();
						}
					} else 
					{	
						try
						{
								log.info("Map contém JSON "+hierarquia[i]);
								JSONObject novoJSON = new JSONObject();
								
								JSONObject mergedObj = new JSONObject();

								Iterator i1 = myMap3.get(hierarquia[i]).keys();
								Iterator i2 = myMap3.get(hierarquia[i+1]).keys();
								
								String tmp_key;
								while(i1.hasNext()) {
								    tmp_key = (String) i1.next();
								    mergedObj.put(tmp_key, myMap3.get(hierarquia[i]).get(tmp_key));
								}
								while(i2.hasNext()) {
								    tmp_key = (String) i2.next();
								    mergedObj.accumulate(tmp_key, myMap3.get(hierarquia[i+1]).get(tmp_key));
								}
								
								
								myMap3.put(hierarquia[i], mergedObj);
								myMap3.remove(hierarquia[i+1]);
								log.info("i = "+i+". Criando novo JSON: "+hierarquia[i]+", "+mergedObj.toString());
								
								
						} catch (JSONException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		JSONObject hue = myMap3.get("ip");
		
		context.write(NullWritable.get(), new Text(hue.toString()));
		
	}
}
