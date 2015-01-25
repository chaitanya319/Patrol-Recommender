package group.project;

import java.io.IOException;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;




public class ClusterHelper {

	public static void writePointsToFile(List<Vector> points, Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}

	/*public static List<List<Cluster>> readClusters(Configuration conf, Path output)
			throws IOException {
		List<List<Cluster>> Clusters = Lists.newArrayList();
		FileSystem fs = FileSystem.get(output.toUri(), conf);

		for (FileStatus s : fs.listStatus(output, new ClustersFilter())) {
			List<Cluster> clusters = Lists.newArrayList();
			for (ClusterWritable value : new SequenceFileDirValueIterable<ClusterWritable>(
					s.getPath(), PathType.LIST, PathFilters.logsCRCFilter(),
					conf)) {
				Cluster cluster = value.getValue();
				clusters.add(cluster);
			}
			Clusters.add(clusters);
		}
		return Clusters;
	}
	*/
}
