import java.io.IOException;

import com.fusion.builder.BuilderException;
import com.fusion.scheduler.DataFlowJob;
import com.fusion.vertex.VertexList;

public class Main {

	public static void main(String[] args) throws BuilderException, IOException {

		DataFlowJob job = new DataFlowJob();
		job.writeTextFile("CRT_out");
		
		VertexList root = 
				job.readTextFile("CRT")
		.map((line) -> line.length(), 1)
		.<Integer>filter((nbr) -> nbr > 2, 1)
		.sumInt();
		
		job.setRoot(root);
		job.run();
	}
}
