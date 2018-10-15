package es.upm.fi.dia.oeg.mappingpedia.model

import java.net.HttpURLConnection
import java.util.UUID

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import es.upm.fi.dia.oeg.mappingpedia.MPCConstants
import org.slf4j.{Logger, LoggerFactory}
import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.model.result.ExecuteMappingResult
import es.upm.fi.dia.oeg.mappingpedia.utility.MpcUtility

object MappingExecution {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def getOrganizationId(datasetId:String) : String = {
    val getDatasetUri = MPCConstants.ENGINE_DATASETS_SERVER + "dataset?dataset_id=" + datasetId;
    logger.info("Hitting getDatasetUri:" + getDatasetUri);
    val getDatasetUriJson = Unirest.get(getDatasetUri).asJson();
    val getDatasetUriResponseStatus = getDatasetUriJson.getStatus();
    logger.info("responseStatus = " + getDatasetUriResponseStatus);
    val organizationId = if(getDatasetUriResponseStatus >= 200 && getDatasetUriResponseStatus < 300) {
      val responseResultObject = getDatasetUriJson.getBody().getObject();
      //logger.info("responseResultObject = " + responseResultObject);
      responseResultObject.getJSONArray("results").getJSONObject(0).getString("ckan_organization_name");
    } else {
      val errorMessage = s"Unable to obtain organization id from the dataset id: ${datasetId}"
      val internalError = new ExecuteMappingResult(
        HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to obtain organization id"
      );
      throw new Exception(errorMessage);
    }
    organizationId
  }

  def getDistributionDownloadUrl(datasetId:String) = {
    val getDistributionsUri = MPCConstants.ENGINE_DATASETS_SERVER + "distributions?dataset_id=" + datasetId;
    logger.info("Hitting getDistributionsUri:" + getDistributionsUri);
    val jsonResponse = Unirest.get(getDistributionsUri).asJson();
    val responseStatus = jsonResponse.getStatus();
    logger.info("responseStatus = " + responseStatus);
    val distributionDownloadUrl = if(responseStatus >= 200 && responseStatus < 300) {
      val responseResultObject = jsonResponse.getBody().getObject();
      responseResultObject.getJSONArray("results").getJSONObject(0).getString("download_url");
    } else {
      val errorMessage = "Unable to obtain mapping document download URL";
      throw new Exception(errorMessage);
    }
    logger.info("distributionDownloadUrl = " + distributionDownloadUrl);
    distributionDownloadUrl
  }

  def getMappingDetails(mdId:String) : MappingDocument = {
    val url = MPCConstants.ENGINE_MAPPINGS_SERVER + "mappings?id=" + mdId;
    logger.info("Hitting url:" + url);
    val jsonResponse = Unirest.get(url).asJson();
    val responseStatus = jsonResponse.getStatus();
    logger.info("responseStatus = " + responseStatus);
    val result = if(responseStatus >= 200 && responseStatus < 300) {
      val firstObject = jsonResponse.getBody().getObject().getJSONArray("results").getJSONObject(0);
      val md = new MappingDocument(mdId);
      md.setDownloadURL(firstObject.getString("downloadURL"))
      md.hash = try { firstObject.getString("hash") } catch { case e:Exception => { null } }
      md.mappingLanguage = try { firstObject.getString("mapping_language") } catch { case e:Exception => { null } }
      md;
    } else {
      val errorMessage = s"Unable to obtain mapping document details: ${mdId}";
      throw new Exception(errorMessage);
    }
    logger.info("result = " + result);
    result
  }

  def apply(datasetId:String, mdId:String, queryFileUrl:String) : MappingExecution = {
    logger.info(s"queryFileUrl = ${queryFileUrl}")
    val pOutputFileExtension = if(queryFileUrl == null || queryFileUrl.equals("")) { "txt" } else { "xml"}
    logger.info(s"pOutputFileExtension= ${pOutputFileExtension}")
    val pOutputFileName = null;
    val pOutputMediaType = null;
    val storeToCKAN = if(queryFileUrl == null || queryFileUrl.equals("")) { true} else { false }
    logger.info(s"storeToCKAN = ${storeToCKAN}")
    val pStoreToGithub = true;
    val pStoreExecutionResultToVirtuoso = if(queryFileUrl == null || queryFileUrl.equals("")) { true } else { false }
    logger.info(s"pStoreExecutionResultToVirtuoso = ${pStoreExecutionResultToVirtuoso}")
    val useCache:Boolean = true;
    val callbackURL = null;
    val updateResource:Boolean = false;

    //GET ORGANIZATION ID BASED ON DATASET ID
    val organizationId = this.getOrganizationId(datasetId)
    logger.info("organizationId = " + organizationId);

    //GET DISTRIBUTION DOWNLOAD URL
    val distributionDownloadUrl = this.getDistributionDownloadUrl(datasetId);
    logger.info("distributionDownloadUrl = " + distributionDownloadUrl);

    //CREATE UNANNOTATION DISTRIBUTION INSTANCE
    val unannotatedDistribution = new UnannotatedDistribution(organizationId, datasetId);
    unannotatedDistribution.dcatDownloadURL = distributionDownloadUrl;

    //CREATE MAPPINGS DETAILS FROM ID
    val md = this.getMappingDetails(mdId);

    val mappingExecution = new MappingExecution(
      List(unannotatedDistribution)
    , null: JDBCConnection
    , queryFileUrl:String
    , pOutputFileName:String
    , pOutputFileExtension:String
    , pOutputMediaType: String
    //, pStoreToCKAN:Boolean
    , pStoreToGithub:Boolean
    , pStoreExecutionResultToVirtuoso:Boolean
    , useCache:Boolean
    , callbackURL:String
    , updateResource:Boolean
    , mdId: String
    , md.hash: String
    , md.getDownloadURL():String
    , md.mappingLanguage:String
    )
    logger.info(s"mappingExecution.pStoreExecutionResultToVirtuoso = ${mappingExecution.pStoreExecutionResultToVirtuoso}")
    logger.info(s"mappingExecution.storeToCKAN = ${mappingExecution.storeToCKAN}")
    logger.info(s"mappingExecution.outputFileExtension = ${mappingExecution.outputFileExtension}")

    mappingExecution.storeToCKAN = storeToCKAN;
    mappingExecution;
  }

  def apply(
      organizationId:String
      , ckanPackageId:String
      , distributionDownloadUrls:List[String]
      , mdDownloadUrl:String
      ) : MappingExecution = {
    val jdbcConnection = null;
    val queryFileName = null;
    val outputFileName = null;
    val outputFileExtension = null;
    val outputMediaType = null;
    val storeToCKAN = true;
    val storeToGithub = true;
    val storeExecutionResultToVirtuoso = true;
    val useCache = false;
    val callbackURL = null;
    val updateResource = false;
    val fieldSeparator = null;
    val distributionMediaType = null;
    
    val organization = Agent.apply(organizationId);
    val getDatasetsUrl = s"${MPCConstants.ENGINE_DATASETS_SERVER}datasets?ckan_package_id=${ckanPackageId}";
    logger.info("Hitting datasets Server Url:" + getDatasetsUrl);
    val response = Unirest.get(getDatasetsUrl);
    val responseStatus = response.asString().getStatus;
    
    logger.info("responseStatus = " + responseStatus);
    val responseResultObject = if(responseStatus >= 200 && responseStatus < 300) {
      val jsonResponse = Unirest.get(getDatasetsUrl).asJson();
      jsonResponse.getBody().getObject();
    } else {
      null
    }
    
    val datasetId = if(responseResultObject != null) {
      val responseArray = responseResultObject.getJSONArray("results");
      if(responseArray.size > 0 ) {
        responseArray.getJSONObject(0).getString("id");
      } else {
        null
      }
    } else {
      null
    }
    
    val unannotatedDistributions:List[UnannotatedDistribution] = distributionDownloadUrls.map(distributionDownloadUrl => {
      val unannotatedDistribution = new UnannotatedDistribution(organizationId, datasetId);
      val distributionDownloadURLTrimmed = distributionDownloadUrl.trim();
      val getResourceIdUrl = s"${MPCConstants.ENGINE_DATASETS_SERVER}ckan_resource_id?package_id=${ckanPackageId}&resource_url=${distributionDownloadURLTrimmed}";
      logger.info("Hitting getResourceIdUrl:" + getResourceIdUrl);
      val getResourceIdResponse = Unirest.get(getResourceIdUrl);
      val getResourceIdResponseStatus = getResourceIdResponse.asString().getStatus;
      val resourceId = if(getResourceIdResponseStatus >= 200 && getResourceIdResponseStatus < 300) {
        //getResourceIdResponse.asJson().getBody.getObject.getJSONArray("results").getJSONObject(0).getString("id");
        getResourceIdResponse.asString().getBody;
      } else {
        null
      }
      unannotatedDistribution.ckanResourceId = resourceId;
      unannotatedDistribution.dcatDownloadURL = distributionDownloadURLTrimmed;
      unannotatedDistribution.csvFieldSeparator = fieldSeparator;
      unannotatedDistribution.dcatMediaType = distributionMediaType;
      unannotatedDistribution
    });
    
    val mpeMappingsUrl = MPCConstants.ENGINE_MAPPINGS_SERVER + "mappings";

    val md = new MappingDocument();
    md.setDownloadURL(mdDownloadUrl);
    md.hash = MpcUtility.calculateHash(mdDownloadUrl, "UTF-8")

    
    val mdId = md.dctIdentifier;
    val mdHash = md.hash;
    logger.info("md.getDownloadURL() = " + md.getDownloadURL());

    val mdLanguage = MpcUtility.detectMappingLanguage(mdDownloadUrl);
    logger.info("mdLanguage = " + mdLanguage);

    val postMappingsUrl = mpeMappingsUrl + "/" + organizationId + "/" + datasetId;
    val postMappingsResponse = Unirest.post(postMappingsUrl)
                    .field("mapping_document_download_url", mdDownloadUrl)
                    .field("mapping_language", md.mappingLanguage)
                    .asJson();

    logger.info("postMappingsResponse = " + postMappingsResponse);
                
    
    val mappingExecution = new MappingExecution(
      //mappingDocument
      unannotatedDistributions
      , jdbcConnection
      , queryFileName
      , outputFileName
      , outputFileExtension
      , outputMediaType
      //, storeToCKAN
      , storeToGithub
      , storeExecutionResultToVirtuoso
      , useCache
      , callbackURL
      , updateResource
      , mdId: String
      , mdHash: String
      , mdDownloadUrl
      , mdLanguage:String
    );
    mappingExecution.storeToCKAN = storeToCKAN;
    mappingExecution

  }
  
  def apply(
             //mappingDocument:MappingDocument
             unannotatedDistributions: java.util.List[UnannotatedDistribution]
             , jdbcConnection: JDBCConnection
             , queryFileName:String
             , pOutputFileName:String
             , pOutputFileExtension:String
             , pOutputMediaType: String
             , pStoreToCKAN:Boolean
             , pStoreToGithub:Boolean
             , pStoreExecutionResultToVirtuoso:Boolean
             , useCache:Boolean
             , callbackURL:String
             , updateResource:Boolean
             , mdId: String
             , mdHash: String
             , mdDownloadURL:String
             , mdLanguage:String
           ) {
    val mappingExecution = new MappingExecution(
      //mappingDocument
      unannotatedDistributions.asScala.toList
      , jdbcConnection
      , queryFileName
      , pOutputFileName
      , pOutputFileExtension
      , pOutputMediaType
      //, pStoreToCKAN
      , pStoreToGithub
      , pStoreExecutionResultToVirtuoso
      , useCache
      , callbackURL
      , updateResource
      , mdId: String
      , mdHash: String
      , mdDownloadURL:String
      , mdLanguage:String
    );
    mappingExecution.storeToCKAN = pStoreToCKAN;

  }
}

class MappingExecution(
                        // val mappingDocument:MappingDocument
                        val unannotatedDistributions: List[UnannotatedDistribution]
                        , val jdbcConnection: JDBCConnection
                        , val queryFileName:String
                        , val pOutputFileName:String
                        , val pOutputFileExtension:String
                        , val pOutputMediaType: String
                        //, val pStoreToCKAN:Boolean
                        , val pStoreToGithub:Boolean
                        , val pStoreExecutionResultToVirtuoso:Boolean
                        , val useCache:Boolean
                        , val callbackURL:String
                        , val updateResource:Boolean
                        , val mdId: String
                        , val mdHash: String
                        , val mdDownloadURL:String
                        , val mdLanguage:String
                      ) {
  def this(
            //mappingDocument:MappingDocument
            unannotatedDistributions: java.util.List[UnannotatedDistribution]
            , jdbcConnection: JDBCConnection
            , queryFileName:String
            , pOutputFileName:String
            , pOutputFileExtension:String
            , pOutputMediaType: String
            //, pStoreToCKAN:Boolean
            , pStoreToGithub:Boolean
            , pStoreExecutionResultToVirtuoso:Boolean
            , useCache:Boolean
            , callbackURL:String
            , updateResource:Boolean
            , mdId: String
            , mdHash: String
            , mdDownloadURL:String
            , mdLanguage:String
          ) {
    this(
      //mappingDocument
      unannotatedDistributions.asScala.toList
      , jdbcConnection
      , queryFileName
      , pOutputFileName
      , pOutputFileExtension
      , pOutputMediaType
      //, pStoreToCKAN
      , pStoreToGithub
      , pStoreExecutionResultToVirtuoso
      , useCache
      , callbackURL
      , updateResource
      , mdId: String
      , mdHash: String
      , mdDownloadURL:String
      , mdLanguage:String
    )
  }


  /*
    def this(mappingDocument:MappingDocument
             , unannotatedDistributions: List[UnannotatedDistribution]
             , queryFileName:String
             , pOutputFileDirectory:String
             , pOutputFilename:String
             , pOutputFileExtension:String
            ) {
      this(mappingDocument, unannotatedDistributions, null, queryFileName
        , pOutputFileDirectory, pOutputFilename, pOutputFileExtension);
    }

    def this(mappingDocument:MappingDocument
             , jdbcConnection: JDBCConnection
             , queryFileName:String
             , pOutputFileDirectory:String
             , pOutputFilename:String
             , pOutputFileExtension:String
            ) {
      this(mappingDocument, null, jdbcConnection, queryFileName
        , pOutputFileDirectory, pOutputFilename, pOutputFileExtension);
    }
  */

  //val dataset: Dataset = null;
  //var outputFileName = "output.txt";
  var outputDirectory:String = null;

  var storeToCKAN:Boolean = true;

  val outputFileName = if (pOutputFileName == null) {
    UUID.randomUUID.toString
  } else {
    pOutputFileName;
  }
  val outputFileExtension = if(pOutputFileExtension == null) {
    "txt"
  } else {
    pOutputFileExtension
  }

  def getOutputFileWithExtension = s"${outputFileName}.${outputFileExtension}";

}
