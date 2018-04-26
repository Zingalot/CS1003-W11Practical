

import java.io.ByteArrayInputStream;
import java.io.IOException;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.*;
import javax.json.stream.JsonParser;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;

public class ScanWordsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	// The output of the mapper is a map from words (including duplicates) to the value 1.

	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

		// The key is the character offset within the file of the start of the line, ignored.
		// The value is a line from the file.

		String line = value.toString();
		InputStream fileToRead = new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8));

		JsonReader reader = Json.createReader(fileToRead);
		JsonObject tweet = reader.readObject();
		JsonObject entities = tweet.getJsonObject("entities");
		if(notNull(entities)) {
			JsonArray urls = entities.getJsonArray("urls");
			if(notNull(urls)){
				for(int i = 0; i < urls.size(); i++){
					JsonObject urlObject = urls.getJsonObject(i);
					if(notNull(urlObject)) {
						try {
							String url = "\"" + urlObject.getString("expanded_url") + "\"";
							output.write(new Text(url), new LongWritable(1));
						}catch (ClassCastException cce){
						}

					}
				}
			}

		}

	}
	private boolean notNull(JsonStructure structure){
		boolean state = true;
		try{
			if(structure.getValueType().equals(JsonValue.ValueType.NULL)){
				state = false;
			}
		} catch (NullPointerException e){
			state = false;
		} catch (ClassCastException cce){
			state = false;
		}
		return state;
	}
}
