package com.fusion.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import com.fusion.io.InputFormat;
import com.fusion.io.OutputContext;
import com.fusion.partitioner.HashPartitioner;
import com.fusion.partitioner.Partitioner;
import com.fusion.vertex.AbstractVertex;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PointWiseStage extends Stage {

	final private String tempPath = "tmp" + File.separator + getJobId() + File.separator + getTaskId();
	final protected Class<? extends InputFormat> inputFormat;
	final private Class<? extends Partitioner> partitioner;

	public PointWiseStage(Class<? extends InputFormat> inputFormat, Class<? extends Partitioner> partitioner,
			String jobId) {
		super(jobId);
		this.inputFormat = inputFormat;
		this.partitioner = partitioner;
	}

	private static final long serialVersionUID = -6401487707823445353L;

	public <T> void run() throws IOException {
		if (getInputFormat() == null)
			throw new FileNotFoundException("Please provide file inputformat");
		
		OutputContext collector = new OutputContext<>(tempPath);

		FileUtils.forceMkdir(new File(tempPath));
		AbstractVertex abs = queue.poll();
		try {
			abs.start(collector);
			abs.execute("", collector);
			abs.close(collector);
		} catch (Exception e) {
			
		}
		
		final int size = queue.size();

		AbstractVertex mapSide = null;
		for (int i = 0; i < size; ++i) {
			mapSide = queue.poll();
			try {
				mapSide.start(collector);
			} catch (Exception e) {
			}
			List<Object> buffer = collector.getBuffer();
			collector.clearBuffer();
			for (Object element : buffer) {
				mapSide.execute(element, collector);
			}

		}

		String collectedFile = collector.finish();
		Optional<Partitioner> ptnr = createPartitionerInstance();
		if (ptnr.isPresent() && partitionCount > 0) {
			partition(collectedFile, ptnr.get());
		}
	}

	private Class<? extends InputFormat> getInputFormat() {
		return inputFormat;
	}

	private Optional<Partitioner> createPartitionerInstance() {
		Optional<Partitioner> optional;
		Partitioner ptnr = null;
		try {
			ptnr = partitioner.newInstance();
		} catch (Exception e) {
			System.err.println("Error creating partitioner instance, using " + "default Hash Partitioner");
			ptnr = new HashPartitioner();
		}
		optional = Optional.ofNullable(ptnr);
		return optional;
	}

	@Override
	public String toString() {
		return tempPath;
	}

	public String getPath() {
		return tempPath;
	}

	private void partition(String filePath, Partitioner ptnr) throws IOException {
		Path path = Paths.get(filePath);
		String partitionPath = path.getParent().toString() + File.separator + "partition_";
		FileInputStream fis = new FileInputStream(path.toFile());

		List<ObjectOutputStream> partitionOuts = new ArrayList<>();
		Map<Integer, String> partitionFiles = new HashMap<>();

		ObjectOutputStream output;
		for (int i = 0; i < partitionCount; i++) {
			partitionFiles.put(i, partitionPath + i);
			output = new ObjectOutputStream(new FileOutputStream(new File(partitionPath + i)));
			partitionOuts.add(output);
		}

		try (ObjectInputStream stream = new ObjectInputStream(fis)) {
			Object ele;
			while ((ele = stream.readObject()) != null) {
				output = partitionOuts.get(ptnr.partitionLogic(ele, partitionCount));

				output.writeObject(ele);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		setPartitionFiles(partitionFiles);
		for (ObjectOutputStream out : partitionOuts) {
			out.writeObject(null);
			out.close();
		}
	}

}
