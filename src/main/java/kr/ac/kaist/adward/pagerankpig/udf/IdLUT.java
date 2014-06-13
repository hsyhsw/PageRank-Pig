package kr.ac.kaist.adward.pagerankpig.udf;

import kr.ac.kaist.adward.pagerankpig.util.LUTImpl;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by adward on 6/3/14.
 */
public class IdLUT extends EvalFunc<Integer> {

	private static LUTImpl lutImpl;

	public IdLUT(String lutPath) throws FileNotFoundException {
		if (lutImpl == null)
			lutImpl = LUTImpl.getInstance(lutPath);
	}

	@Override
	public Integer exec(Tuple input) throws IOException {
		return lutImpl.lookup((String) input.get(0));
	}
}
