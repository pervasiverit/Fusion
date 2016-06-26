import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.fusion.elements.TripleElement;
import com.fusion.io.Collector;
import com.fusion.io.OutputContext;
import com.fusion.utils.Triple;
import com.fusion.vertex.AbstractVertex;

public class TransformVertex extends AbstractVertex<String>{

	private long multiplied = 0;
	
	@Override
	public void start(OutputContext collector) throws Exception{
		
		multiplied = Files.lines(Paths.get("CRT"))
			 .mapToLong(e -> Long.parseLong(e.split(",")[1]))
			 .reduce(1L, (c,e) -> c * e);
	}
	
	@Override
	public void execute(String line, OutputContext collector) throws IOException {
		String arr[] = line.split(",");
		BigInteger ele1 = new BigInteger(arr[0]);
		BigInteger ele2 = new BigInteger(arr[1]);
		Triple<BigInteger, BigInteger, BigInteger> tripleElement = 
				new Triple<>(ele1, ele2, BigInteger.valueOf(multiplied));
		collector.add(new TripleElement(tripleElement));
	}
}
