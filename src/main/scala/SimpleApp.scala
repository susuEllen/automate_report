import java.io.{PrintWriter, File}
import org.apache.spark.{SparkContext, SparkConf}

object SimpleApp {

  def main(args: Array[String]) = {

    if (args.length == 0 || args(0) == "usage" || args.length < 2) {
      println("\n\nUsage:\n\tSimpleApp <AppName> e.g RetentionRisk/CustomerCollections <FileName> e.g rr_data.csv")
      System.exit(0)
    }

    val appName = args(0)// "RetentionRisk" or "CustomerCollections
    val csvFileName = args(1)
    val basePath = "/Users/ellenwong/Desktop/stabilityReportData/"
    println(s"Generating StabilityReport for $appName ...")

    var csvFile1 = basePath + csvFileName
    val outputFileName = s"${appName}_out.csv"
    val currentDir = new java.io.File( "." ).getCanonicalPath
    var outputFile = if(new File(basePath).exists()) {
      new File(basePath + outputFileName)
    } else {
      new java.io.File(currentDir + "/" +  outputFileName)
    }

    if (!new File(csvFile1).exists()) {
      println(s"basePath csvFile does not exist [${csvFile1}] \tDefaulting to working directory ... ")
      csvFile1 = currentDir + "/" + csvFileName
      println(s"Sending output file to working directory ... ")
      outputFile = new java.io.File(currentDir + "/" +  outputFileName)
    }

    val writer = new PrintWriter(outputFile)

    //Read the raw file
    val conf = new SparkConf().setAppName("Simple App").setMaster("local")
    val sc = new SparkContext(conf)
    val dataArray: Array[String] = sc.textFile(csvFile1).collect()

    println(s"TotalDataCount: ${dataArray.length}")
    //Filters
    val dataArrayFiltered = dataArray.filter(!_.contains("\"Run Start\""))
    println(s"FilteredDataCount: ${dataArrayFiltered.length}")

    val evalJobName = "\"" + appName + "EvalJob\""
    val fetchJobName = "\"" + appName + "FetchJob\""
    val publishJobName = "\"" + appName + "PublishJob\""

    val dataArrayEval = dataArrayFiltered.filter(_.contains(evalJobName))
    val dataArrayFetch = dataArrayFiltered.filter(_.contains(fetchJobName))
    val dataArrayPubish = dataArrayFiltered.filter(_.contains(publishJobName))
    val dataArrayUnexpected = dataArrayFiltered.filter{ dataStr =>
      !dataStr.contains(evalJobName) && !dataStr.contains(fetchJobName) && !dataStr.contains(publishJobName)
    }
    if (dataArrayUnexpected.isEmpty) {
      println("No unexpected rows. Awesome!")
    } else {
      println(s"Unexpected line count: ${dataArrayUnexpected.length}\n")
      dataArrayUnexpected.map(println(_))
    }
    println(s"EvalJobCount: ${dataArrayEval.length}\nFetchJobCount: ${dataArrayFetch.length}\nPublishJobCount: ${dataArrayPubish.length}\n")
    val totalJobCount = dataArrayEval.length + dataArrayFetch.length + dataArrayPubish.length
    println(s"Total Job Count: ${totalJobCount}\t FilteredDataCount: ${dataArrayFiltered.length}")
    if (totalJobCount != dataArrayFiltered.length) {
      throw new RuntimeException("The count is off, something is wrong. Look at your data")
    }

    val columns = Seq("tenant","jobName","startDate","result","timeTaken","auc")
    columns.foreach( col => writer.write(s"${col},"))
    //writer.write("tenant,jobName,startDate,result,timeTaken,auc")
    dataArrayFiltered.map(processRow).map {
      processedCsvStr =>
      writer.write(s"\n${processedCsvStr}")
    }
    writer.close()
    sc.stop()
  }


  def processRow(rowStr : String) = {
    val dataArray0 =  rowStr.replace("\"\"","\"")
    val splittedRow = dataArray0.split(',')
      //dataArray0.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).filter(_.contains("og.scala"))

    var rowInfoMap = Map[String, String]()

    splittedRow.map {
      splittedStr =>
        //println(splittedStr)
        splittedStr match {
          case str if str startsWith "/var" => {
            val (tenant, jobName, startDate) = processStartWithVar(str)
            rowInfoMap = rowInfoMap + ("tenant" -> tenant) + ("jobName" -> jobName) + ("startDate" -> startDate)
          }
          //case str if str contains "\"jobName\":" => println(splittedStr)
          case str if str contains "\"result\":" => {
            rowInfoMap = rowInfoMap + ("result" -> processJsonStr(splittedStr))
          }
          case str if str contains "\"timeTaken\":" => {
            rowInfoMap = rowInfoMap + ("timeTaken" -> processTimeTaken(splittedStr).toString)
          }

          case str if str contains "\"AUCROC\":" => {
            rowInfoMap = rowInfoMap + ("auc" -> processJsonStr(splittedStr))
          }

          case str if str contains "\"aucBootstrap\":" => {
            val aucSubString = dataArray0.substring(dataArray0.indexOf("\"aucBootstrap\""))
            //val aucBootstrapStr = aucSubString.substring(aucSubString.indexOf("25%"), aucSubString.indexOf("med"))
            //val stringArray = dataArray0.substring(, )
            rowInfoMap = rowInfoMap + ("auc" -> processAucBootstrap(aucSubString))
          }
          case _  => //println("not it!")
        }
    }
    //print items in comma separated values
    val NA = "NA"
    val csvStr =
      s"${rowInfoMap.get("tenant").getOrElse(NA)}," +
      s"${rowInfoMap.get("jobName").getOrElse(NA)}," +
      s"${rowInfoMap.get("startDate").getOrElse(NA)}," +
      s"${rowInfoMap.get("result").getOrElse(NA)}," +
      s"${rowInfoMap.get("timeTaken").getOrElse(NA)}," +
      s"${rowInfoMap.get("auc").getOrElse(NA)}"

    //println(csvStr)
    csvStr
  }

  def processStartWithVar(str: String) = {
    //Example:
    ///var/log/job_scheduler/RetentionRiskFetchJob/wd5-impl.troweprice7.RetentionRiskFetchJob.2016-04-18_00-07-11.37-c7444ea9e.af99945fa11bc5e37660877e.log
    val splittedVarStr = str.split('.')
    val (tenant, jobName, startDate) = (splittedVarStr(1),splittedVarStr(2),splittedVarStr(3).split('_')(0))
    (tenant, jobName, startDate)
  }

  def processJsonStr(str: String) = {
    str.replaceAll("\"", "").split(':').apply(1)
  }


  //"aucBootstrap":"\"{\\\"n\\\":10.0,\\\"min\\\":0.13636363636363635,\\\"25%\\\":0.37374999999999997,\\\"med\\\":0.40669642857142857,\\\"75%\\\":
  def processAucBootstrap(str: String) = {
    str.substring(str.indexOf("25%"), str.indexOf("med")).split(",|:")(1)
  }

  def processTimeTaken(str: String) = {
    val timeTakenStr = processJsonStr(str)
    val timeTakenStrArray = timeTakenStr.replaceAll("h|m", "").split(' ')
    val (hour, min) = (timeTakenStrArray(0).toDouble, timeTakenStrArray(1).toDouble)
    BigDecimal(((hour * 60) + min) /60).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      //.replaceAll("\"|\\{|\\}",
  }
}

//TODO: double check if "Run Start" filter is doing the right thing, or is it filtering more than I intended to
//TODO: see if you can also parse memory?
//TODO: filter to only specific tenants in RR


/*
What info do you want extracted?

Fetch: runtime/ result/ memory
Eval: runtime/ result/ memory/ auc
publish: result/ runtime
*/

// Raw data examples:
//Fetch
//,"maxUsedMemory":
//Eval
//"MaxMemoryUsedAfterEval":"17
//"MaxMemoryUsedBeforeEval":"26.20gb
//"AUCROC":"0.8680341943266613",

//""result"":""s
//,"timeTaken":"
