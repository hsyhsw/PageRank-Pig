package kr.ac.kaist.adward.pagerankpig;

import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by adward on 6/1/14.
 */
public class LocalLauncher {

	private static final Logger logger = Logger.getLogger(LocalLauncher.class);
	private static final NumberFormat twoDigits = new DecimalFormat("00");

	public static void main(String[] args) throws IOException {

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		PigServer pigServer = new PigServer(ExecType.LOCAL);
		pigServer.registerJar("pagerank-pig.jar");

		Map<String, String> params = new HashMap<String, String>();
		params.put("inputPath", "input");
		pigServer.registerScript("scripts/pagerank-parse-genLUT.pig", params);

		pigServer.registerScript("scripts/pagerank-genGraph.pig");

		long iteration00Time = stopWatch.getTime();
		long iterStart = iteration00Time;

		for (long i = 0; i < 20; ) {
			params.clear();
			params.put("from", twoDigits.format(i));
			params.put("to", twoDigits.format(i + 1));
			pigServer.registerScript("scripts/pagerank-calculateIteration.pig", params);
			++i;
		}

		long iterEnd = stopWatch.getTime();
		long topNStart = iterEnd;

		params.clear();
		params.put("from", "20");
		params.put("n", "300");
		pigServer.registerScript("scripts/pagerank-getTopN.pig", params);

		long topNEnd = stopWatch.getTime();

		stopWatch.stop();
		logger.info("Completed all jobs in " + stopWatch.getTime() / 1000.0 + " seconds.");
		logger.info("Completed parsing, graph generation in " + iteration00Time / 1000.0 + " seconds.");
		logger.info("Completed iterations in " + (iterEnd - iterStart) / 1000.0 + " seconds.");
		logger.info("Completed topN in " + (topNEnd - topNStart) / 1000.0 + " seconds.");
		stopWatch.reset();
	}
}
