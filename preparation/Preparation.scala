package chronos
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import chronos.DataFrameConstants._
import chronos.PrepData.{getDataFrame, spark}
import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.spark.sql.functions._



object Preparation {

  System.setProperty("hadoop.home.dir", "D://spark//winutls//");
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("PrepData").getOrCreate()
  import spark.implicits._

  def jsonConversion()={
    val str = """ {"name" : "Avik" , "age" : "24"} """

    val gson = new Gson()
    val json = new JsonParser().parse(str).getAsJsonObject
    println(json.get("name").toString)
    json.addProperty("city","Bangalore")
    val newjson = gson.fromJson(json, classOf[JsonObject])
    println(newjson)
  }

  def getValueFromRow(row : String,start : Int,end : Int) = {
    row.substring(start,end)
  }
  val getValueFromRowUdf = udf(getValueFromRow _)



  def getDataFrame(df : DataFrame , recordType : List[(String,Int,Int)]) : DataFrame = {
    if(recordType.isEmpty)
      df
    else {
      val data = recordType(0)
      val ddf = df.withColumn(data._1, getValueFromRowUdf($"value", lit(data._2), lit(data._3)))
      getDataFrame(ddf, recordType.tail)
    }
  }

  def main(args: Array[String]): Unit = {

    val data = spark.read.text("D:\\spark\\prep.txt").filter($"value" =!= "00" && $"value" =!= "99" && $"value" =!= "98").cache()

    val company01Df = getDataFrame(data.filter($"value".startsWith("01")),Company_01).withColumnRenamed("value","01_Company")
    val act14Df = getDataFrame(data.filter($"value".startsWith("14")),Acts_14).withColumnRenamed("value","14_Acts")
    val companyComplimentaryData17Df = getDataFrame(data.filter($"value".startsWith("17")),CompanyComplimentaryData_17).withColumnRenamed("value","17_CompanyComplimentaryData")
    val situation10Df = getDataFrame(data.filter($"value".startsWith("10")),Situation_10).withColumnRenamed("value","10_Situation")
    val legalNature11Df = getDataFrame(data.filter($"value".startsWith("11")),LegalNature_11).withColumnRenamed("value","11_LegalNature")
    val branchInfo05Df = getDataFrame(data.filter($"value".startsWith("05")),BranchInfo_05).withColumnRenamed("value","05_branch")
    val partnerIndividual02Df = getDataFrame(data.filter($"value".startsWith("02")),PartnerIndividual_02).withColumnRenamed("value","02_PartnerIndividual")
    val partnerCondition13Df = getDataFrame(data.filter($"value".startsWith("13")),PartnerCondition_13).withColumnRenamed("value","13_PartnerCondition")
    val address04Df = getDataFrame(data.filter($"value".startsWith("04")),Address_04).withColumnRenamed("value","04_Address")
    val partnerComplimentaryData18Df = getDataFrame(data.filter($"value".startsWith("18")),PartnerComplimentaryData_18).withColumnRenamed("value","18_PartnerComplimentaryData")
    val partnerCompany03Df = getDataFrame(data.filter($"value".startsWith("03")),PartnerCompany_03).withColumnRenamed("value","03_PartnerCompany")
    val partnerCompanyAddress15Df = getDataFrame(data.filter($"value".startsWith("15")),PartnerCompanyAddress_15).withColumnRenamed("value","15_PartnerCompanyAddress")
    val activity06Df = getDataFrame(data.filter($"value".startsWith("06")),Activity_06).withColumnRenamed("value","06_Activity")
    val activityDesc07Df = getDataFrame(data.filter($"value".startsWith("07")),Activity_Desc_07).withColumnRenamed("value","07_Activity_Desc")

    val parentCompanyInfoDf =  company01Df
                              .join(act14Df,company01Df("acts") === act14Df("act_code"),"inner")
                              .join(situation10Df,company01Df("company_status_code") === situation10Df("business_situation_code"),"inner")
                              .join(legalNature11Df,company01Df("code_of_legal_nature") === legalNature11Df("legal_nature_code"),"inner")
                              .join(companyComplimentaryData17Df,company01Df("nire") === companyComplimentaryData17Df("nire17"),"inner")

    val branches = branchInfo05Df
                               .join(situation10Df,branchInfo05Df("situation_code") === situation10Df("branch_situation_code"),"inner")
                               .join(legalNature11Df,branchInfo05Df("nature_code") === legalNature11Df("branch_nature_code"),"inner")


    val parentCompanyDf = parentCompanyInfoDf.select($"nire",$"sequence_no",struct($"01_Company",$"14_Acts",$"10_Situation",$"11_LegalNature",$"17_CompanyComplimentaryData")
     .as("parentCompany"))

    val branchesDf = branches.select($"parent_nire_05",$"05_branch",$"10_Situation",$"11_LegalNature").groupBy($"parent_nire_05")
      .agg(collect_list(struct("05_branch","10_Situation","11_LegalNature")).as("Branches"))

    val parentCompany_Branch_Df = parentCompanyDf.join(branchesDf,parentCompanyDf("nire")===branchesDf("parent_nire_05"),"full_outer")
      //.drop(branchesDf("parent_nire_05"))
   // parentCompany_Branch_Df.toJSON.coalesce(1).write.text("D:\\spark\\prep")

    val ddf = parentCompany_Branch_Df
      .withColumn("parentCompany",when(isnull($"parentCompany"),struct(concat(lit("01"),branchesDf("parent_nire_05"),lit("0"*27)).as("01_Company"),lit("xxx").as("14_Acts"),
        lit("xxx").as("10_Situation"),lit("xxx").as("11_LegalNature"),lit("xxx").as("17_CompanyComplimentaryData")))
      .otherwise($"parentCompany")).drop(branchesDf("parent_nire_05"))
    ddf.show(false)

    parentCompany_Branch_Df.show(false)

    val individual_partners = partnerIndividual02Df
                             .join(partnerCondition13Df,partnerIndividual02Df("condition_code_02") === partnerCondition13Df("condition_code"))
                             .join(address04Df,partnerIndividual02Df("cpf_02") === address04Df("cpf_04"))
                             .join(partnerComplimentaryData18Df,partnerIndividual02Df("nire_02") === partnerComplimentaryData18Df("nire_18"),"left_outer")

     val individualPartnersDf = individual_partners.select($"sequence_no_02",$"02_PartnerIndividual",$"13_PartnerCondition",$"04_Address",$"18_PartnerComplimentaryData")
                                                  .groupBy($"sequence_no_02").agg(collect_list(struct("02_PartnerIndividual","13_PartnerCondition","04_Address","18_PartnerComplimentaryData"))
                                                  .as("Individual_Partners"))

    val parent_Branch_IndividualPartners_Df = ddf.join(individualPartnersDf,ddf("sequence_no") === individualPartnersDf("sequence_no_02"),"full_outer")
                                              .drop("sequence_no_02").drop("sequence_no")
    //parent_Branch_IndividualPartners_Df.toJSON.coalesce(1).write.text("D:\\spark\\prep")

    val partner_company = partnerCompany03Df
      .join(partnerCondition13Df,partnerCompany03Df("condition_code_03") === partnerCondition13Df("condition_code"))
      .join(partnerCompanyAddress15Df,partnerCompany03Df("partner_nire") === partnerCompanyAddress15Df("nire_15"))

    val partnerCompanyDf = partner_company.select($"parent_nire_03",$"03_PartnerCompany",$"13_PartnerCondition",$"15_PartnerCompanyAddress")
      .groupBy($"parent_nire_03").agg(collect_list(struct("03_PartnerCompany","13_PartnerCondition","15_PartnerCompanyAddress" ))
      .as("Company_Partner"))

    val parent_Branch_IndividualPartners_PartnerCompany_Df = parent_Branch_IndividualPartners_Df.join(partnerCompanyDf,parent_Branch_IndividualPartners_Df("nire") === partnerCompanyDf("parent_nire_03"),"full_outer")
      .drop("parent_nire_03")
//    parent_Branch_IndividualPartners_PartnerCompany_Df.toJSON.coalesce(1).write.text("D:\\spark\\prep")

    val activity = activity06Df
      .join(activityDesc07Df,activity06Df("activity_code") === activityDesc07Df("activity_code_07"))

    val activitiesDf = activity.select($"nire_06",$"06_Activity",$"07_Activity_Desc").groupBy($"nire_06")
      .agg(collect_list(struct($"06_Activity",$"07_Activity_Desc")).as("Activities"))

    val parent_Branch_IndividualPartners_PartnerCompany_Activities_Df = parent_Branch_IndividualPartners_PartnerCompany_Df
      .join(activitiesDf,parent_Branch_IndividualPartners_PartnerCompany_Df("nire") === activitiesDf("nire_06"),"full_outer")
      .drop($"nire").drop($"nire_06")
    parent_Branch_IndividualPartners_PartnerCompany_Activities_Df.toJSON.coalesce(1).write.text("D:\\spark\\prep")
    //parent_Branch_IndividualPartners_PartnerCompany_Activities_Df.show(false)
  }


  //    val ddf = data.transform(dt => {
  //      x.foldLeft(dt)((dt,x) =>{
  //        val df = dt.withColumn(x._1,getValueFromRowUdf($"value",lit(x._2),lit(x._3)))
  //        df
  //
  //      })
  //    })

}
