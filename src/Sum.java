import java.io.IOException;
import java.math.BigInteger;

import com.fusion.elements.BigIntegerElement;
import com.fusion.io.Collector;
import com.fusion.io.OutputContext;
import com.fusion.vertex.AbstractVertex;

public class Sum extends AbstractVertex<BigIntegerElement>{
	private BigInteger total = BigInteger.ZERO;
	
	@Override
	public void execute(BigIntegerElement line, OutputContext collector) throws IOException {
		total = total.add(line.getElement());
		System.out.println(total);
	}
	
	@Override
	public void close(OutputContext collector){
		try {
			collector.add(new BigIntegerElement(total));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
