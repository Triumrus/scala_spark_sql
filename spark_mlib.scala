import spark.implicits._

val df = spark.read.option("header","true").csv("hdfs://hadoop/user/a.kuzmichev/data_iris/iris.csv")
    .withColumn("Iris_virginica", when('Species === "Iris-virginica", lit(1)).otherwise(lit(0)))
    .withColumn("Iris_setosa", when('Species === "Iris-setosa", lit(1)).otherwise(lit(0)))
    .withColumn("Iris_versicolor", when('Species === "Iris-versicolor", lit(1)).otherwise(lit(0)))
    .drop("Species")
    .select(
        'Sepal_Length.cast("Double") as "Sepal_Length",
        'Sepal_Width.cast("Double") as "Sepal_Width",
        'Petal_Length.cast("Double") as "Petal_Length",
        'Petal_Width.cast("Double") as "Petal_Width",
        'Iris_virginica.cast("Double") as "Iris_virginica",
        'Iris_setosa.cast("Double") as "Iris_setosa",
        'Iris_versicolor.cast("Double") as "Iris_versicolor"
    
    )
    
df.show(5)
// df.rdd.map(l=> println(l))
df.filter('Sepal_Length === 5.1).show


#FROM DATAFRAME TO RDD
val dfRdd = df.rdd


#FROM RDD TO RDD OF LABELEDPOINT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

val labelPoint = df.rdd.map{row => 

    val label = row.getAs[Double](0)
    val vector = Vectors.dense(row.getAs[Double]("Sepal_Width"),row.getAs[Double]("Petal_Length")
    ,row.getAs[Double]("Petal_Width"),row.getAs[Double]("Iris_virginica"),row.getAs[Double]("Iris_setosa"),row.getAs[Double]("Iris_versicolor")
    )
   LabeledPoint(label,vector)
}





  import java.io.File
  import org.apache.spark.mllib.util.MLUtils
 
  
  def saveLibsvm(labeledPoints: RDD[LabeledPoint], outputDir: String): Unit = {

    
    MLUtils.saveAsLibSVMFile(labeledPoints.coalesce(1), outputDir)

  }
  
 import scala.sys.process._
 "hdfs dfs -rm -r hdfs://hadoop/user/a.kuzmichev/data_iris/labelPoint" !
    
  saveLibsvm(labelPoint,"hdfs://hadoop/user/a.kuzmichev/data_iris/labelPoint")




import org.apache.spark.ml.regression.LinearRegression

// Load training data
 val data = spark.read.format("libsvm")
   .load("hdfs://hadoop/user/a.kuzmichev/data_iris/labelPoint").repartition(100).persist

val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3),777L)



val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(trainingData)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Summarize the model over the training set and print out some metrics
val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")


println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

lrModel.intercept


lrModel.transform(testData).show(false)

//Coefficients: [0.0,0.23624997257992242,0.008006896761660111,0.0] Intercept: 4.953500146508115
//Coefficients: (5,[1,2],[0.2029691359369646,0.17935995430495869]) Intercept: 4.876518832320689







df.filter('Sepal_Length === 5.0).show
// |         6.7|        3.1|         5.6|        2.4|           1.0|        0.0|            
// 6.38645540484053

// |         5.0|        3.5|         1.3|        0.3|           0.0|        1.0|            0.0|






val prre2 = 1.3*0.17680582823929394+0.3*0.16739331448341713
val prinet2 = prre2+4.960059581216303








val prre = 3.5*0+1.6*0.21005665626474643+0.2*0.11011621387032978+0
val prinet = prre+4.940680540567269
//|         5.1|        3.5|*0         1.4|*0.21        0.2|*0.11           0.0|*0
//|         5.1|        3.5|*0         1.4|*0.21        0.2|*0.11           0.0|*0
//|         5.1|        3.8|*0         1.6|*0.21        0.2|*0.11           0.0|*0        
// |6.7  |(5,[0,1,2,3],[3.1,5.6,2.4,1.0])|6.38645540484053  |





// lrModel.save("hdfs://hadoop/user/a.kuzmichev/data_iris/model")




val model=lrModel
// Here are the coefficient and intercept
val weights: org.apache.spark.mllib.linalg.Vector = model.weights
val intercept = model.intercept
val weightsData: Array[Double] = weights.asInstanceOf[DenseVector].values
