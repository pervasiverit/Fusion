import java.io.IOException;
import java.math.BigInteger;

import com.fusion.elements.BigIntegerElement;
import com.fusion.io.Collector;
import com.fusion.io.OutputContext;
import com.fusion.vertex.AbstractVertex;

public class Hello extends AbstractVertex<String>{
	
	private static final long serialVersionUID = -1749891852729638953L;

	@Override
	public void execute(String line, OutputContext collector) throws IOException {
		collector.add(new BigIntegerElement(BigInteger.valueOf(line.length())));
	}
}
