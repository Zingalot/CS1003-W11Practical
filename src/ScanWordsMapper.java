

import java.io.ByteArrayInputStream;
import java.io.IOException;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ScanWordsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	// The output of the mapper is a map from words (including duplicates) to the value 1.

	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

		// The key is the character offset within the file of the start of the line, ignored.
		// The value is a line from the file.

		String line = value.toString();
		boolean inUrls = false;
		boolean inEntities = false;
		boolean retweeted = false;
		/*Scanner scanner = new Scanner(line);
		System.out.println(line);*/
		InputStream fileToRead = new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8));

		JsonParser parser = Json.createParser(fileToRead);

		while(parser.hasNext()){
			JsonParser.Event event = parser.next();

			//The current issue is that there are two 'entities' blocks, one within 'retweeted status' block and one not
			//I need to only count the ones that are NOT within 'retweeted status'
			//Difficult to do within a parser because JSON does not have a name fo the element being ended
			//Figure this out soon

			//This bit sets a boolean if 'retweeted status' is being parsed, and then 'continues' through it
			//Until the 'retweeted status' object has ended
			if(event.toString().equals("KEY_NAME")){
				if(parser.getString().equals("retweeted_status")){
					retweeted = true;
					while(retweeted){
						if (event.toString().equals(("KEY_NAME"))){
							if(parser.getString().equals("filter_level")){
								retweeted = false;
							}
						}
						event = parser.next();
					}
				}
			}

			//This bit needs to find objects called 'entities'
			if(event.toString().equals("KEY_NAME")){
				if(parser.getString().equals("entities"))
					inEntities = true;
			}

			//This bit needs to find arrays called 'urls'
			while(inEntities) {
				JsonParser.Event entitiesEvent = parser.next();


				//If we are still before the urls array
				if (entitiesEvent.toString().equals("KEY_NAME")) {
					if (parser.getString().equals("urls")) {
						parser.next();
						parser.next();
						inUrls = true;

						//This bit finds objects labelled 'url'
						while (inUrls) {
							JsonParser.Event arrayEvent = parser.next();
							if (arrayEvent.toString().equals("KEY_NAME")) {
								if (parser.getString().equals("expanded_url")) {
									if (parser.next().toString().equals("VALUE_STRING")) {
										String url = "\"" + parser.getString() + "\"" ;
										output.write(new Text(url), new LongWritable(1));
									}
								}

							}

							if (arrayEvent.toString().equals("END_ARRAY")) {
								inEntities = false;
								inUrls = false;
							}
						}

					}
				}


			}



		}




		/*while (scanner.hasNext()) {
			String word = scanner.next();
			output.write(new Text(word), new LongWritable(1));
		}
		scanner.close();*/
	}
}
