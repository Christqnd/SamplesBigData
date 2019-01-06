/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucuenca.bigdata.hadoop;
/**
 *
 * @author hduser
 */
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Principal {

    public static class csvMapper extends Mapper<Object, Text, Text, IntWritable>{
        private String[] valFilasSplit; 
        private static IntWritable atrasoOutput;
        private final Text clave = new Text();
        private  IntWritable vuelo = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] filas = value.toString().split("\\n");
            for(int i=0; i<filas.length; i++) {
                String fila = filas[i];
                String[] splitCsvFila = fila.split(",");
                String Year="0",Month,Origin,Dest;
                try{
                    Month = splitCsvFila[1];
                    Origin = splitCsvFila[16];
                    Dest = splitCsvFila[17];
                }catch(Exception e){
                    Origin = "NA";
                    Dest = "NA";
                    Month = "NA";
                }
                vuelo = new IntWritable(1);
                clave.set(Origin+","+Dest+","+Month);
                context.write(clave, vuelo);
            }
        }
    }

    public static class IntermediateSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable totalAtrasos = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sumVuelos = 0;
            for (IntWritable value : values) {
                sumVuelos += value.get();
            }
            totalAtrasos.set(sumVuelos);
            context.write(key, totalAtrasos);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JOB: Mayor número de vuelos por ruta");
        job.setJarByClass(Principal.class);
        job.setMapperClass(csvMapper.class);
        job.setCombinerClass(IntermediateSumReducer.class);
        job.setReducerClass(IntermediateSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}