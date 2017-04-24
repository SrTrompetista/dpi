package dpi.pcap.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class IPReducer extends Reducer<Text, Text, NullWritable, Text>
{ 
	@Override
	public void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException
	{

		String chave, valor;
		String[] temp;
		JSONArray jsonExistente;
		JSONObject jsonFinal = new JSONObject();
		JSONObject indexador = new JSONObject();
		JSONObject metadados = new JSONObject();

		
		Map<String, JSONArray> mapChaveValores = new HashMap<String, JSONArray>();

		for (Text record : records)
		{
			String[] parChaveValor = record.toString().split(";");

			for (int i = 0; i < parChaveValor.length; i++)
			{

				temp = parChaveValor[i].split("=");
				chave = temp[0];
				valor = temp[1];

				if (chave.contains("frame-time_epoch")) 
					valor = valor.substring(0, 10); 
					
				if (mapChaveValores.containsKey(chave))
				{					
					jsonExistente = mapChaveValores.get(chave);
					jsonExistente.put(valor);
					mapChaveValores.put(chave, jsonExistente);
				} else
				{
					JSONArray list = new JSONArray();
					list.put(valor);
					mapChaveValores.put (chave, list);
				}
			}
		}
		
		try 
		{
			jsonFinal.put("key", key);
			jsonFinal.put("protocol", "ip");
			
			for (Map.Entry<String, JSONArray> entry : mapChaveValores.entrySet())
			{
			    jsonFinal.put(entry.getKey(), entry.getValue());
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		try
		{
			
			metadados.put("_index", "dpi");
			metadados.put("_type", "ip");
			indexador.put("index", metadados);
			
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		context.write(NullWritable.get(), new Text(indexador.toString()+"\n"+jsonFinal.toString()));
		
	}
}
