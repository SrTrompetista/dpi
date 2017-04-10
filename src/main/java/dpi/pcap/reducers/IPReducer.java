package dpi.pcap.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
		
		Map<String, String> myMap = new HashMap<String, String>();
		Map<String, JSONObject> myMap2 = new HashMap<String, JSONObject>();

		for (Text record : records)
		{
			String[] parChaveValor = record.toString().split(";");

			for (int i = 0; i < parChaveValor.length; i++)
			{

				log.info("IPREDUCER: AQUIIIIIIIIIIIIIII >>>>>>>> " + parChaveValor[i]);
				temp = parChaveValor[i].split("=");
				chave = temp[0];
				valor = temp[1];

				if (myMap.containsKey(chave))
				{
					valorExistente = myMap.get(chave);
					myMap.put(chave, valorExistente == "" ? valor : valorExistente + ", " + valor);
				} else
				{
					myMap.put (chave, valor);
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


		context.write(NullWritable.get(), new Text(hue.toString()));
		
	}
}
