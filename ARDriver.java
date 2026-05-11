import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ARDriver {


    public static class RatingStatsWritable implements org.apache.hadoop.io.Writable {
        private double sumRatings;
        private long count;
        private double minRating;
        private double maxRating;

        public RatingStatsWritable() {
            this.sumRatings = 0.0;
            this.count = 0;
            this.minRating = Double.MAX_VALUE;
            this.maxRating = Double.MIN_VALUE;
        }

        public RatingStatsWritable(double sumRatings, long count, double minRating, double maxRating) {
            this.sumRatings = sumRatings;
            this.count = count;
            this.minRating = minRating;
            this.maxRating = maxRating;
        }

        public double getSumRatings() { return sumRatings; }
        public long getCount() { return count; }
        public double getMinRating() { return minRating; }
        public double getMaxRating() { return maxRating; }

        public void set(double sumRatings, long count, double minRating, double maxRating) {
            this.sumRatings = sumRatings;
            this.count = count;
            this.minRating = minRating;
            this.maxRating = maxRating;
        }

        public void merge(RatingStatsWritable other) {
            this.sumRatings += other.sumRatings;
            this.count += other.count;
            this.minRating = Math.min(this.minRating, other.minRating);
            this.maxRating = Math.max(this.maxRating, other.maxRating);
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            out.writeDouble(sumRatings);
            out.writeLong(count);
            out.writeDouble(minRating);
            out.writeDouble(maxRating);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            sumRatings = in.readDouble();
            count = in.readLong();
            minRating = in.readDouble();
            maxRating = in.readDouble();
        }

        @Override
        public String toString() {
            return sumRatings + "," + count + "," + minRating + "," + maxRating;
        }
    }


    public static class ARMapper extends Mapper<LongWritable, Text, Text, RatingStatsWritable> {

        private Text outputKey = new Text();
        private RatingStatsWritable outputValue = new RatingStatsWritable();

        // Helper method to safely get CSV field considering quoted values
        private String getField(String[] fields, int index) {
            if (index >= fields.length) return "";
            String field = fields[index].trim();
            // Remove quotes if present
            if (field.startsWith("\"") && field.endsWith("\"")) {
                field = field.substring(1, field.length() - 1);
            }
            return field;
        }


        private String extractDecade(String releasedAt) {
            if (releasedAt == null || releasedAt.isEmpty()) return null;

            try {
                String yearStr = "";
                if (releasedAt.contains("-")) {
                    yearStr = releasedAt.split("-")[0];
                } else if (releasedAt.matches("\\d{4}")) {
                    yearStr = releasedAt;
                } else {
                    return null;
                }

                int year = Integer.parseInt(yearStr);
                if (year < 1900 || year > 2030) return null; // Sanity check

                int decadeStart = (year / 10) * 10;
                return decadeStart + "s";
            } catch (NumberFormatException e) {
                return null;
            }
        }


        private String extractFirstGenre(String genreField) {
            if (genreField == null || genreField.isEmpty()) return null;

            String firstGenre;
            if (genreField.contains(",")) {
                firstGenre = genreField.split(",")[0].trim();
            } else {
                firstGenre = genreField.trim();
            }

            // Skip if empty after trimming
            if (firstGenre.isEmpty()) return null;
            return firstGenre;
        }



        private double extractRating(String imdbRatingStr) {
            if (imdbRatingStr == null || imdbRatingStr.isEmpty()) return -1.0;

            try {
                String ratingStr = imdbRatingStr.trim();

                // Handle "7.5/10" format
                if (ratingStr.contains("/")) {
                    ratingStr = ratingStr.split("/")[0];
                }
                // Handle "7.5 (123 votes)" format
                if (ratingStr.contains("(")) {
                    ratingStr = ratingStr.split("\\(")[0];
                }
                // Handle "7.5/10 (123 votes)" format
                if (ratingStr.contains("/") && ratingStr.contains("(")) {
                    ratingStr = ratingStr.substring(0, ratingStr.indexOf("(")).trim();
                    if (ratingStr.contains("/")) {
                        ratingStr = ratingStr.split("/")[0];
                    }
                }

                double rating = Double.parseDouble(ratingStr);
                // Valid IMDb rating range: 0-10
                if (rating < 0 || rating > 10) return -1.0;
                return rating;
            } catch (NumberFormatException e) {
                return -1.0;
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            if (key.get() == 0) {
                return;
            }

            String line = value.toString();

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");


            if (fields.length < 12) {
                return; // Skip invalid rows
            }

            String releasedAt = getField(fields, 3);
            String genreField = getField(fields, 4);
            String type = getField(fields, 9);
            String imdbRatingStr = getField(fields, 11);


            if (type == null || type.isEmpty() || type.equalsIgnoreCase("null")) {
                return;
            }

            if (releasedAt == null || releasedAt.isEmpty() || releasedAt.equalsIgnoreCase("null")) {
                return;
            }

            if (genreField == null || genreField.isEmpty() || genreField.equalsIgnoreCase("null")) {
                return;
            }

            if (imdbRatingStr == null || imdbRatingStr.isEmpty() || imdbRatingStr.equalsIgnoreCase("null")) {
                return;
            }


            String decade = extractDecade(releasedAt);
            if (decade == null) {
                return;
            }


            String firstGenre = extractFirstGenre(genreField);
            if (firstGenre == null) {
                return;
            }


            double rating = extractRating(imdbRatingStr);
            if (rating < 0) {
                return;
            }


            String compositeKey = type + "," + firstGenre + "," + decade;
            outputKey.set(compositeKey);

            // Store rating statistics for this record
            outputValue.set(rating, 1L, rating, rating);
            context.write(outputKey, outputValue);
        }
    }

    //  COMBINER CLASS (BONUS)
    public static class ARCombiner extends Reducer<Text, RatingStatsWritable, Text, RatingStatsWritable> {

        @Override
        protected void reduce(Text key, Iterable<RatingStatsWritable> values, Context context)
                throws IOException, InterruptedException {

            double sumRatings = 0.0;
            long count = 0;
            double minRating = Double.MAX_VALUE;
            double maxRating = Double.MIN_VALUE;

            for (RatingStatsWritable stats : values) {
                sumRatings += stats.getSumRatings();
                count += stats.getCount();
                minRating = Math.min(minRating, stats.getMinRating());
                maxRating = Math.max(maxRating, stats.getMaxRating());
            }

            RatingStatsWritable combinedStats = new RatingStatsWritable(sumRatings, count, minRating, maxRating);
            context.write(key, combinedStats);
        }
    }

    // ======================= REDUCER CLASS =======================
    public static class ARReducer extends Reducer<Text, RatingStatsWritable, Text, Text> {

        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<RatingStatsWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalRatingSum = 0.0;
            long totalCount = 0;
            double minRating = Double.MAX_VALUE;
            double maxRating = Double.MIN_VALUE;


            for (RatingStatsWritable stats : values) {
                totalRatingSum += stats.getSumRatings();
                totalCount += stats.getCount();
                minRating = Math.min(minRating, stats.getMinRating());
                maxRating = Math.max(maxRating, stats.getMaxRating());
            }


            double averageRating = totalCount > 0 ? totalRatingSum / totalCount : 0.0;


            String outputStr = key.toString() + "," + totalCount + "," +
                    String.format("%.2f", averageRating) + "," +
                    String.format("%.1f", minRating) + "," +
                    String.format("%.1f", maxRating);

            outputValue.set(outputStr);
            context.write(new Text(""), outputValue);  // Empty key to avoid extra tab
        }
    }

    // ======================= DRIVER CLASS =======================
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ARDriver <input path> <output path>");
            System.err.println("Example: hadoop jar ardriver.jar ARDriver input.csv output");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie and TV Show Statistics");

        job.setJarByClass(ARDriver.class);


        job.setMapperClass(ARMapper.class);
        job.setCombinerClass(ARCombiner.class);
        job.setReducerClass(ARReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RatingStatsWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}