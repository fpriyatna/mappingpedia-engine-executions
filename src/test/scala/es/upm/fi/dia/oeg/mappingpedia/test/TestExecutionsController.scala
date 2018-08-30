package es.upm.fi.dia.oeg.mappingpedia.test

import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController
import es.upm.fi.dia.oeg.mappingpedia.model.MappingExecution

object TestExecutionsController {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	val executionsController = MappingExecutionController.apply();

def main(args:Array[String]) = {
		//this.testExecuteMapping();
	this.testFindInstancesByClassName("Building");
  }

  def testExecuteMapping() {
    val organizationId = "test-mobileage-upm4";
    val ckanPackageId = "203edd3a-bf01-45e3-aba9-efa9bf61d1e0";
		val datasetId = "99a532f3-ee00-4aee-a814-5e28098bad03"
    val distributionDownloadUrl = List("https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.csv");
    val mdDownloadUrl = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.r2rml.ttl";
    
		val mappingExecution = MappingExecution(
		    organizationId
		    , ckanPackageId
		    , distributionDownloadUrl
		    , mdDownloadUrl
				);
		
		//IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
		executionsController.executeMapping(mappingExecution);   
  }

	def testFindInstancesByClassName(className:String) = {
		val instances = executionsController.findInstancesByClass(className)
		logger.info(s"instances = ${instances}")
	}
}