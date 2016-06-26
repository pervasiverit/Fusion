import java.io.IOException;
import java.math.BigInteger;

import com.fusion.elements.BigIntegerElement;
import com.fusion.elements.TripleElement;
import com.fusion.io.Collector;
import com.fusion.io.OutputContext;
import com.fusion.utils.Triple;
import com.fusion.vertex.AbstractVertex;

public class CRTSumVertex extends AbstractVertex<TripleElement>{
	BigInteger CRT = BigInteger.ZERO;
	BigInteger B = BigInteger.ZERO;
	
	@Override
	public void execute(TripleElement element, OutputContext collector)
			throws IOException {
		Triple<BigInteger, BigInteger, BigInteger> triple = element.getElement();
		CRT = CRT.add(triple.getFirst());
		B = triple.getSecond();
	}

	@Override
	public void close(OutputContext collector) {
		System.out.println("Chinese remainder theorem value : "+CRT.remainder(B));
		try {
			collector.add(new BigIntegerElement(CRT));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
