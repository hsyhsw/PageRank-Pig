package kr.ac.kaist.adward.pagerankpig.udf;

import kr.ac.kaist.adward.pagerankpig.model.WikiPage;
import kr.ac.kaist.adward.pagerankpig.util.WikiTextParser;
import kr.ac.kaist.adward.pagerankpig.util.WikiXmlSAXParser;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by adward on 5/30/14.
 */
public class PigLinkExtractor extends EvalFunc<Tuple> {

	private static final Pattern garbageTitles = Pattern.compile("^\\w+:");

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;

		String page = input.toString();
		WikiPage wikiPage = parseXml(page);

		String wikiText = wikiPage.getWikiText();
		Long docId = wikiPage.getDocumentId();
		String title = wikiPage.getTitle();

		Matcher matcher = garbageTitles.matcher(title);
		if (matcher.find())
			return null;
		if (!title.matches("[\\x20-\\x7F]+"))
			return null;
		if (wikiPage.getNs() != 0)
			return null;

		List<String> links = null;

		try {
			links = WikiTextParser.getInstance().parseLinks(wikiText);
		} catch (StackOverflowError e) {
			System.out.println("\tParsing error(stack ovfl): " + docId + ", " + wikiPage.getTitle());
			return null;
		} catch (Exception e) {
			System.out.println("\tParsing error: " + docId + ", " + wikiPage.getTitle());
			e.printStackTrace(System.out);
			return null;
		}

		Tuple t = TupleFactory.getInstance().newTuple();
		t.append(docId.intValue());
		t.append(title);
		DataBag linkBag = BagFactory.getInstance().newDefaultBag();
		for (String link : links) {
			linkBag.add(TupleFactory.getInstance().newTuple(link));
		}
		t.append(linkBag);

		return t;
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema schema = null;

		try {
			schema = new Schema();

			schema.add(new Schema.FieldSchema("id", DataType.INTEGER));
			schema.add(new Schema.FieldSchema("title", DataType.CHARARRAY));
			schema.add(new Schema.FieldSchema("links", new Schema(new Schema.FieldSchema("link", DataType.CHARARRAY)), DataType.BAG));

			schema = new Schema(new Schema.FieldSchema("", schema, DataType.TUPLE));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return schema;
	}

	/**
	 * produce an object representation of a wiki page
	 *
	 * @param xml well-formed xml representation of a wiki page
	 */
	private WikiPage parseXml(String xml) {
		WikiPage wikiPage = null;
		try {
			wikiPage = WikiXmlSAXParser.parse(xml);
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return wikiPage;
	}
}
