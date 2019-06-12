package org.example

import com.google.gson.{Gson, JsonArray, JsonObject, JsonParser}
import org.example.JsonConversion.{parsingDataAsPerRecord, recordConfig}

object CombiningJson {

  def parsingDataAsPerRecord(recordText: String, fileType : String, recordType: String,
                             recordConfiguration: Map[(String,String), Map[String, (Int, Int)]]): JsonObject = {
    val fieldLevelJsonData = new JsonObject
    val fieldDetail = recordConfiguration.get((fileType,recordType)).getOrElse(Map())

    fieldDetail.keys.foreach(
      fieldLevelData => {
        val fieldDataContent = fieldDetail.get(fieldLevelData).getOrElse(-1, -1)
        val startPosition = fieldDataContent._1
        val lengthOfTheData = fieldDataContent._2
        val extractedData = recordText.substring(startPosition, (startPosition + lengthOfTheData))
        fieldLevelJsonData.addProperty(fieldLevelData, extractedData)
      }
    )
    fieldLevelJsonData
  }

  val recordConfig = Map(
    ("SP","01") -> Map("NIRE" -> (3,11),"SEQ_NO" -> (14,5), "CNPJ" -> (19,14), "COMPANY_STATUS_CODE" -> (33,3) , "CODE_OF_LEGAL_NATURE" -> (36,2) , "ACTS" -> (38,3)),
    ("SP","14") -> Map("ACT_CODE" -> (3,3),"ACT_DESC" -> (6,11)),
    ("SP","05") -> Map("CNPJ" -> (3,14),"PARENT_NIRE" -> (17,11),"PARTNER_NIRE" -> (28,11),"SITUATION_CODE" -> (39,3),"LEGAL_NATURE_CODE" -> (42,3)),
    ("SP","10") -> Map("BUSINESS_SITUATION_CODE" -> (3,3),"BUSINESS_SITUATION_DESC" -> (6,11),"BRANCH_SITUATION_CODE" -> (17,3)),
    ("SP","11") -> Map("LEGAL_NATURE_CODE" -> (3,2),"LEGAL_NATURE_DESC" -> (5,11),"BRANCH_NATURE_CODE" -> (16,3))
  )


  def main(args: Array[String]): Unit = {

    val record =
      """ {"parentCompany":{"01_Company":"0111111111111222223333333333333300701100","14_Acts":"14100description","10_Situation":"10007description563","11_LegalNature":"1101description463","17_CompanyComplimentaryData":"171111111111187878787878787Sample"},"Branches":[{"05_branch":"05646464646464641111111111133333333333563463","10_Situation":"10007description563","11_LegalNature":"1101description463"},{"05_branch":"05646464646444441111111111122222222222563463","10_Situation":"10007description563","11_LegalNature":"1101description463"}],
                      "Individual_Partners":[{"02_PartnerIndividual":"0222222           9876987698769878234156111","13_PartnerCondition":"1378234156111999879879",
                      "04_Address":"0498769876987698some_description826003"},{"02_PartnerIndividual":"0222222111111111119876987698777778234156111","13_PartnerCondition":"1378234156111999879879",
                      "04_Address":"0498769876987777some_description826002","18_PartnerComplimentaryData":"1811111111111F234567"}],
                      "Company_Partner":[{"03_PartnerCompany":"0311111111111555555555552222278234156111","13_PartnerCondition":"1378234156111999879879",
                      "15_PartnerCompanyAddress":"15545454545454541604201955555555555"},{"03_PartnerCompany":"0311111111111777777777772222278234156111","13_PartnerCondition":"1378234156111999879879",
                      "15_PartnerCompanyAddress":"15545454545454541604201977777777777"}],"Activities":[{"06_Activity":"0611111111111555","07_Activity_Desc":"07555989898"},
                      {"06_Activity":"0611111111111222","07_Activity_Desc":"07222567894"}]}
                    """.stripMargin

    val gson = new Gson()
    val json = new JsonParser().parse(record).getAsJsonObject

    val combinedJson = new JsonObject
    val parentCompanyJson = new JsonObject
    val branchJsonObject = new JsonObject
    val branchJsonArray = new JsonArray

    // Parent company info from json
    val parentCompany = json.getAsJsonObject("parentCompany")
    val company_01 = parentCompany.get("01_Company").toString
    val acts_14 = parentCompany.get("14_Acts").toString
    val situation_10 = parentCompany.get("10_Situation").toString
    val legalNature_11 = parentCompany.get("11_LegalNature").toString
    val companyCompData_17 = parentCompany.get("17_CompanyComplimentaryData").toString

    val company_01_FieldsJson = parsingDataAsPerRecord(company_01, "SP", "01", recordConfig)
    val acts_14_FieldsJson = parsingDataAsPerRecord(acts_14, "SP", "14", recordConfig)

    parentCompanyJson.add("company_01",company_01_FieldsJson)
    parentCompanyJson.add("acts_14",acts_14_FieldsJson)

    combinedJson.add("ParentCompany",parentCompanyJson)

    // Branch info from json
    val branches = json.getAsJsonArray("Branches")
    for (i <- 0 until branches.size()) {
      val branch = branches.get(i).getAsJsonObject
      val branch_05 = branch.get("05_branch").toString
      val situation_10 = branch.get("10_Situation").toString
      val legalNature_11 = branch.get("11_LegalNature").toString

      val branch_05_FieldsJson = parsingDataAsPerRecord(branch_05, "SP", "05", recordConfig)
      val situation_10_FieldsJson = parsingDataAsPerRecord(situation_10, "SP", "10", recordConfig)
      val legalNature_11_FieldsJson =  parsingDataAsPerRecord(legalNature_11, "SP", "11", recordConfig)

      branchJsonObject.add("branch_05",branch_05_FieldsJson)
      branchJsonObject.add("situation_10",situation_10_FieldsJson)
      branchJsonObject.add("legalNature_11",legalNature_11_FieldsJson)

      branchJsonArray.add(branchJsonObject)
    }

    combinedJson.add("Branches",branchJsonArray)
    println(combinedJson)

  }

}
