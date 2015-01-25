package group.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;


import weka.core.Instances;

public class KmeanCluster {
	

	public static List<Vector> getPoints() 
	{
		  
		  try 
		  {
				// Read from a Arff file
				BufferedReader reader = new BufferedReader(new FileReader(
						"test4.arff"));
				Instances data = new Instances(reader);
				int size = data.numInstances();
				//size = size;

				// get instance in an arraylist

				ArrayList ar = new ArrayList(size);

				for (int i = 0; i < size; i++) {
					// double [] fr = <double []> ;
					ar.add(data.instance(i));
				}
				System.out.println("Size of arraylist: " + ar.size());

				int comma;
				double[] p = new double[2];
				
				List<Vector> points = new ArrayList<Vector>();

				for (int j = 0; j < size; j++) {
					// get object
					Object o1 = (Object) ar.get(j);
					String point = o1.toString();
					// System.out.println(point);

					// Get lat and Long in string format
					comma = point.indexOf(",");
					// System.out.println(comma);
					String lat = point.substring(0, comma);
					String lon = point.substring(comma + 1);
					// System.out.println("Lat:"+lat+"long:"+lon);

					p[0] = Double.parseDouble(lat);
					p[1] = Double.parseDouble(lon);

					// System.out.println("to be vector is: "+p[0]+" "+ p[1]);
					
					Vector vec = new RandomAccessSparseVector(2);
					vec.assign(p);
					points.add(vec);
				}
				reader.close();
				return points;
		  }
		  catch(Exception e)
		  {
			  System.out.println("Errors no vector to return!");
			  e.printStackTrace();
		  }
		  return null;
}

	// Returns arraylist of vectors created using the points

	public static void main(String args[]) throws Exception {

		int k = 10;

		
		double starttime = System.currentTimeMillis();
		
		List<Vector> vectors = getPoints();

		// Make a folder called testdata
		File testData = new File("testdata");
		if (!testData.exists()) {
			testData.mkdir();
		}
		// Make a folder called points
		testData = new File("testdata/points");
		if (!testData.exists()) {
			testData.mkdir();
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		ClusterHelper.writePointsToFile(vectors, conf, new Path(
				"testdata/points/file1"));

		// Path is a file like object
		Path path = new Path("testdata/clusters/part-m-00000");

		// hadoop implementation to write in a file using key value pair
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				Text.class, Cluster.class);

		// form intial clusters, every point in the vector is a cluster, with
		// the point as centroid itself

		for (int i = 0; i < k; i++) {

			Vector vec = vectors.get(i);
			// Form a cluster usign vector (i.e a point, and an id ->i)
			Cluster cluster = new Cluster(vec, i,
					new EuclideanDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();

		// Path gets path object for the given string (file or directory)
		Path output = new Path("output");

		HadoopUtil.delete(conf, output);

		// run k-mean :
		KMeansDriver.run(conf, new Path("testdata/points"), new Path(
				"testdata/clusters"), output, new EuclideanDistanceMeasure(),
				0.5, 5, true, false);
		

		/*print out center of clusters
		 SequenceFile.Reader reader1 = new SequenceFile.Reader(fs, new Path(
						"output/clusters-1-final/part-r-00000"), conf);
		 
		Cluster value1 = new Cluster();
		
		while(reader1.next(value1)){
			System.out.println(value1.getCenter() + value1.getRadius().toString()+"belongs to cluster"+value1.getId());
		}
		reader1.close();
		
		*/
		
		
		//print out all points
		SequenceFile.Reader reader2 = new SequenceFile.Reader(fs, new Path(
				"output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"),
				conf);
		IntWritable key = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
		 
		while (reader2.next(key, value)) {
		
			System.out.println(value.getVector().toString() + " belongs to cluster "
					+ key.toString());
		}
		centroid ct= new centroid();
		System.out.println("Result set that we get after Kmeans");
		ct.display();
		reader2.close();
		double stoptime = System.currentTimeMillis();
		double t = stoptime-starttime;
		t = t/1000;
		System.out.println("Result set is generated in "+t+"seconds");
	}

}