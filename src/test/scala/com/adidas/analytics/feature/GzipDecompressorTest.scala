package com.adidas.analytics.feature

import com.adidas.utils.TestUtils._
import com.adidas.analytics.algo.GzipDecompressor
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.utils.BaseAlgorithmTest
import org.apache.hadoop.fs.Path
import org.scalatest.FeatureSpec
import org.scalatest.Matchers._


class GzipDecompressorTest extends FeatureSpec with BaseAlgorithmTest {

  private val paramFileName: String = "params.json"
  private val paramFilePathHDFS: Path = new Path(hdfsRootTestPath, paramFileName)
  private val paramFilePathLocal: Path = new Path(resolveResource(paramFileName))

  private val fileNames: Seq[String] = Seq(
    "data_20180719111849_data_1-3",
    "data_20180719111849_data_2-3",
    "data_20180719111849_data_3-3"
  )

  private val compressedFiles: Seq[String] = fileNames.map(_ + ".gz").map(fileName => resolveResource(fileName, withProtocol = true))
  private val uncompressedFiles: Seq[String] = fileNames.map(fileName => resolveResource(fileName, withProtocol = true))

  private val testDataPath: Path = new Path(hdfsRootTestPath, "test_landing/bi/test_data_table/data")
  private val testDataLocation: String = testDataPath.toUri.toString

  private val uncompressedDataPath: Path = new Path(hdfsRootTestPath, "test_landing/bi/uncompressed_data_table/data")
  private val uncompressedDataLocation: String = uncompressedDataPath.toUri.toString

  private val recursive: Boolean = true


  feature("Correct execution of GzipDecompressorBytes") {
    scenario("File names are correct, compressed files are deleted and file size increases") {

      // prepare data
      fs.mkdirs(testDataPath)
      fs.mkdirs(uncompressedDataPath)

      compressedFiles.foreach(location => fs.copyFromLocalFile(new Path(location), testDataPath))
      uncompressedFiles.foreach(location => fs.copyFromLocalFile(new Path(location), uncompressedDataPath))

      // checking pre-conditions
      val sourceData = spark.read.textFile(testDataLocation).toDF()
      val expectedData = spark.read.textFile(uncompressedDataLocation).toDF().persist()

      expectedData.hasDiff(sourceData) shouldEqual false
      sourceData.count() shouldEqual expectedData.count()

      fs.ls(testDataPath, recursive).size shouldEqual 3
      fs.ls(testDataPath, recursive).map(path=> fs.getFileStatus(path).getLen)
      fs.ls(testDataPath, recursive).forall(_.getName.endsWith(".gz")) shouldBe true
      val baseFileNamesAndSizesBeforeDecompression = fs.ls(testDataPath, recursive).map(path =>
        path.getName.substring(0, path.getName.lastIndexOf("."))-> fs.getFileStatus(path).getLen
      ).toMap


      fs.ls(uncompressedDataPath, recursive).size shouldEqual 3
      fs.ls(uncompressedDataPath, recursive).forall(!_.getName.endsWith(".gz")) shouldBe true

      // running the algorithm
      GzipDecompressor(spark, dfs, paramFilePathHDFS.toUri.toString).run()

      // validating results
      fs.ls(testDataPath, recursive).length shouldEqual 3
      fs.ls(testDataPath, recursive).count(!_.getName.endsWith(".gz")) shouldEqual 3
      fs.ls(testDataPath, recursive).count(_.getName.endsWith(".gz")) shouldEqual 0
      val baseFileNamesAndSizesAfterDecompression = fs.ls(testDataPath, recursive).map(path =>
        path.getName.substring(0, path.getName.lastIndexOf("."))-> fs.getFileStatus(path).getLen
      ).toMap

      baseFileNamesAndSizesBeforeDecompression.size shouldBe baseFileNamesAndSizesAfterDecompression.size
      baseFileNamesAndSizesBeforeDecompression.forall{
        case (k, v) => baseFileNamesAndSizesAfterDecompression.contains(k) && baseFileNamesAndSizesAfterDecompression(k) >= v
      }
    }

    scenario("Only .gz files are processed"){

      // prepare data
      fs.mkdirs(testDataPath)

      uncompressedFiles.foreach(location => fs.copyFromLocalFile(new Path(location), testDataPath))

      val expectedFiles = fs.ls(testDataPath, recursive)
      val expectedData = spark.read.textFile(testDataLocation).collect()

      // running the algorithm
      GzipDecompressor(spark, dfs, paramFilePathHDFS.toUri.toString).run()

      // validating results
      val actualFiles = fs.ls(testDataPath, recursive)
      actualFiles.length shouldEqual expectedFiles.length
      actualFiles.toSet shouldEqual expectedFiles.toSet

      val actualData = spark.read.textFile(testDataLocation).collect()
      actualData.length shouldEqual expectedData.length
      actualData.toSet shouldEqual expectedData.toSet
    }

    scenario("Expect exception if directory does not exist"){

      val caught =
        intercept[RuntimeException] {
        GzipDecompressor(spark, dfs, paramFilePathHDFS.toUri.toString).run()
      }

      assert(caught.getMessage.contains("does not exist"))
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    fs.copyFromLocalFile(paramFilePathLocal, paramFilePathHDFS)
  }
}
