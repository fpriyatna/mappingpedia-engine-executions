package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.{ByteArrayOutputStream, File, InputStream}
import java.net.{HttpURLConnection, URL}
import java.util.{Date, Properties, UUID}

import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.{MPCConstants, MappingPediaConstant, MappingPediaProperties}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility._
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory, MorphRDBProperties, MorphRDBRunnerFactory}
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.http.HttpStatus

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object MappingExecutionController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  //var executionQueue = new mutable.Queue[MappingExecution];

  def apply(): MappingExecutionController = {
    val propertiesFilePath = "/" + MappingPediaConstant.DEFAULT_CONFIGURATION_FILENAME;

    /*
    val url = getClass.getResource(propertiesFilePath)
    logger.info(s"loading mappingpedia-engine-datasets configuration file from:\n ${url}")
    val properties = new Properties();
    if (url != null) {
      val source = Source.fromURL(url)
      val reader = source.bufferedReader();
      properties.load(reader)
      logger.debug(s"properties.keySet = ${properties.keySet()}")
    }
    */

    logger.info(s"loading mappingpedia-engine-executions configuration file from:\n ${propertiesFilePath}")
    val in = getClass.getResourceAsStream(propertiesFilePath)
//    val properties = new Properties();
//    properties.load(in)
    val properties = new MappingPediaProperties(in);

    logger.info(s"properties.keySet = ${properties.keySet()}")

    MappingExecutionController(properties)
  }

  def apply(properties: MappingPediaProperties): MappingExecutionController = {
    val ckanUtility = new MpcCkanUtility(
      properties.getProperty(MappingPediaConstant.CKAN_URL)
      , properties.getProperty(MappingPediaConstant.CKAN_KEY)
    );

    val githubUtility = new MpcGithubUtility(
      properties.getProperty(MappingPediaConstant.GITHUB_REPOSITORY)
      , properties.getProperty(MappingPediaConstant.GITHUB_USER)
      , properties.getProperty(MappingPediaConstant.GITHUB_ACCESS_TOKEN)
    );

    val virtuosoUtility = new MpcVirtuosoUtility(
      properties.getProperty(MappingPediaConstant.VIRTUOSO_JDBC)
      , properties.getProperty(MappingPediaConstant.VIRTUOSO_USER)
      , properties.getProperty(MappingPediaConstant.VIRTUOSO_PWD)
      , properties.getProperty(MappingPediaConstant.GRAPH_NAME)
    );

    val schemaOntology = MPCJenaUtility.loadSchemaOrgOntology(virtuosoUtility
      , MappingPediaConstant.SCHEMA_ORG_FILE, MappingPediaConstant.FORMAT)
    val jenaUtility = new MPCJenaUtility(schemaOntology);

    new MappingExecutionController(ckanUtility, githubUtility, virtuosoUtility, jenaUtility, properties);

  }

  /*
  val ckanUtility = new CKANUtility(
    MappingPediaEngine.mappingpediaProperties.ckanURL, MappingPediaEngine.mappingpediaProperties.ckanKey)
  val githubClient = MappingPediaEngine.githubClient;
  */


  def executeR2RMLMappingWithRDB(mappingExecution: MappingExecution) = {
    logger.info("Executing R2RML mapping (RDB) ...")
    //val md = mappingExecution.mappingDocument;
    val mappingDocumentDownloadURL = mappingExecution.mdDownloadURL
    logger.info(s"mappingDocumentDownloadURL = $mappingDocumentDownloadURL");

    val outputFilepath = if(mappingExecution.outputDirectory == null) { mappingExecution.getOutputFileWithExtension; }
    else { s"${mappingExecution.outputDirectory}/${mappingExecution.getOutputFileWithExtension}"}
    logger.info(s"outputFilepath = $outputFilepath");

    val jDBCConnection = mappingExecution.jdbcConnection

    //val distribution = dataset.getDistribution();
    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${mappingExecution.mdId}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphRDBProperties = new MorphRDBProperties
    properties.setNoOfDatabase(1)
    properties.setDatabaseUser(jDBCConnection.dbUserName)
    properties.setDatabasePassword(jDBCConnection.dbPassword)
    properties.setDatabaseName(jDBCConnection.dbName)
    properties.setDatabaseURL(jDBCConnection.jdbc_url)
    properties.setDatabaseDriver(jDBCConnection.databaseDriver)
    properties.setDatabaseType(jDBCConnection.databaseType)
    properties.setMappingDocumentFilePath(mappingDocumentDownloadURL)
    properties.setOutputFilePath(outputFilepath)


    val runnerFactory: MorphRDBRunnerFactory = new MorphRDBRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

  def executeMapping(mappingExecution:MappingExecution) = {
    //val md = mappingExecution.mappingDocument;
    val mappingLanguage =
      if (mappingExecution.mdLanguage == null) { MappingPediaConstant.MAPPING_LANGUAGE_R2RML }
      else { mappingExecution.mdLanguage }

    if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {
      //this.morphRDBQueue.enqueue(mappingExecution)
      MappingExecutionController.executeR2RMLMapping(mappingExecution);
    } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
      MappingExecutionController.executeRMLMapping(mappingExecution);
    } else if (MappingPediaConstant.MAPPING_LANGUAGE_xR2RML.equalsIgnoreCase(mappingLanguage)) {
      throw new Exception(mappingLanguage + " Language is not supported yet");
    } else {
      throw new Exception(mappingLanguage + " Language is not supported yet");
    }
    logger.info("mapping execution done!")
  }

  def executeR2RMLMapping(mappingExecution:MappingExecution) = {
    if(mappingExecution.jdbcConnection != null) {
      this.executeR2RMLMappingWithRDB(mappingExecution)
    } else if(mappingExecution.unannotatedDistributions != null) {
      this.executeR2RMLMappingWithCSV(mappingExecution);
    }
  }

  def executeR2RMLMappingWithCSV(mappingExecution:MappingExecution) = {
    logger.info("Executing R2RML mapping (CSV) ...")
    //val md = mappingExecution.mappingDocument;
    val unannotatedDistributions = mappingExecution.unannotatedDistributions
    val queryFileName = mappingExecution.queryFileName
    val outputFilepath = if(mappingExecution.outputDirectory == null) { mappingExecution.getOutputFileWithExtension; }
    else { s"${mappingExecution.outputDirectory}/${mappingExecution.getOutputFileWithExtension}"}
    logger.info(s"outputFilepath = $outputFilepath");

    val mappingDocumentDownloadURL = mappingExecution.mdDownloadURL
    logger.info(s"mappingDocumentDownloadURL = $mappingDocumentDownloadURL");

    //val distributions = unannotatedDataset.dcatDistributions
    val downloadURLs = unannotatedDistributions.map(distribution => distribution.dcatDownloadURL);

    val datasetDistributionDownloadURL = downloadURLs.mkString(",")
    logger.info(s"datasetDistributionDownloadURL = $datasetDistributionDownloadURL");

    val csvSeparator = unannotatedDistributions.iterator.next().csvFieldSeparator;

    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${mappingExecution.mdId}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphCSVProperties = new MorphCSVProperties
    properties.setDatabaseName(databaseName)
    properties.setMappingDocumentFilePath(mappingDocumentDownloadURL)
    properties.setOutputFilePath(outputFilepath);
    properties.setCSVFile(datasetDistributionDownloadURL);
    properties.setQueryFilePath(queryFileName);
    if (csvSeparator != null) {
      properties.fieldSeparator = Some(csvSeparator);
    }

    val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

  def executeRMLMapping(mappingExecution: MappingExecution) = {
    logger.info("Executing RML mapping ...")
    val rmlConnector = new RMLMapperConnector();
    rmlConnector.executeWithMain(mappingExecution);
  }



  def getMappingExecutionResultURL(mdSHA:String, datasetDistributionSHA:String) = {

  }

  def generateManifestFile(mappingExecutionResult:AnnotatedDistribution
                           //, datasetDistribution: Distribution
                           , unannotatedDistributions: List[UnannotatedDistribution]
                           //, mappingDocument:MappingDocument
                           , mdId: String
                           , mdHash: String
                           //, mdDownloadURL:String
                           //, pMdHash:String
                          ) = {
    logger.info("Generating manifest file for Mapping Execution Result ...")
    try {
      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-mappingexecutionresult-template.ttl"
      );

      //val datasetDistributionDownloadURL:String = "";

      val downloadURL = if(mappingExecutionResult.dcatDownloadURL == null) { "" }
      else { mappingExecutionResult.dcatDownloadURL }
      logger.info(s"downloadURL = ${downloadURL}")

      val mappingDocumentHash = if(mdHash == null) { "" } else { mdHash }
      logger.info(s"mappingDocumentHash = ${mappingDocumentHash}")

      //val datasetDistributionHash = unannotatedDataset.dcatDistributions.hashCode().toString
      val datasetDistributionHash = MappingPediaUtility.calculateHash(unannotatedDistributions);
      logger.info(s"datasetDistributionHash = ${datasetDistributionHash}")

      val datasetId = mappingExecutionResult.dataset.dctIdentifier;

      val mapValues:Map[String,String] = Map(
        "$mappingExecutionResultID" -> mappingExecutionResult.dctIdentifier
        , "$mappingExecutionResultTitle" -> mappingExecutionResult.dctTitle
        , "$mappingExecutionResultDescription" -> mappingExecutionResult.dctDescription
        , "$datasetID" -> datasetId
        , "$mappingDocumentID" -> mdId
        , "$downloadURL" -> downloadURL
        , "$mappingDocumentHash" -> mappingDocumentHash
        , "$datasetDistributionHash" -> datasetDistributionHash
        , "$issued" -> mappingExecutionResult.dctIssued
        , "$modified" -> mappingExecutionResult.dctModified

      );

      val manifestString = MpcUtility.generateManifestString(mapValues, templateFiles);
      val filename = s"metadata-mappingexecutionresult-${mappingExecutionResult.dctIdentifier}.ttl";
      val manifestFile = MpcUtility.generateManifestFile(manifestString, filename, datasetId);
      manifestFile;
    } catch {
      case e:Exception => {
        e.printStackTrace()
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage
        null;
      }
    }
  }

}


class MappingExecutionController(
                                  val ckanClient:MpcCkanUtility
                                  , val githubClient:MpcGithubUtility
                                  , val virtuosoClient: MpcVirtuosoUtility
                                  , val jenaClient:MPCJenaUtility
                                  , val properties: MappingPediaProperties
                                )
{
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  //val mappingDocumentController:MappingDocumentController = new MappingDocumentController(ckanClient, githubClient, virtuosoClient, jenaClient);
  //val mapper = new ObjectMapper();

  val helper = new MappingExecutionControllerHelper(this);
  val helperThread = new Thread(helper);
  helperThread.start();

  def addQueryFile(queryFile: File, organizationId:String, datasetId:String) : GeneralResult = {
    logger.info("storing a new query file in github ...")
    logger.debug("organizationId = " + organizationId)
    logger.debug("datasetId = " + datasetId)

    try {
      val commitMessage = "Add a new query file by mappingpedia-engine"
      val response = githubClient.encodeAndPutFile(organizationId
        , datasetId, queryFile.getName, commitMessage, queryFile)
      logger.debug("response.getHeaders = " + response.getHeaders)
      logger.debug("response.getBody = " + response.getBody)
      val responseStatus = response.getStatus
      logger.debug("responseStatus = " + responseStatus)
      val responseStatusText = response.getStatusText
      logger.debug("responseStatusText = " + responseStatusText)
      if (HttpURLConnection.HTTP_CREATED == responseStatus) {
        val queryURL = response.getBody.getObject.getJSONObject("content").getString("url")
        logger.debug("queryURL = " + queryURL)
        logger.info("query file stored.")
        val executionResult = new GeneralResult(responseStatusText, responseStatus)
        return executionResult
      }
      else {
        val executionResult = new GeneralResult(responseStatusText, responseStatus)
        return executionResult
      }
    } catch {
      case e: Exception =>
        val errorMessage = e.getMessage
        logger.error("error uploading a new query file: " + errorMessage)
        val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
        val executionResult = new GeneralResult(errorMessage, errorCode)
        return executionResult
    }
  }


  def findByHash(mdHash:String, datasetDistributionHash:String) = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> this.properties.graphName
      , "$mdHash" -> mdHash
      , "$datasetDistributionHash" -> datasetDistributionHash
    );

    val queryString: String = MpcUtility.generateStringFromTemplateFile(
      mapValues, "templates/findMappingExecutionResultByHash.rq");
    logger.info(s"queryString = ${queryString}");

    var results: List[String] = List.empty;

    if(this.virtuosoClient != null) {
      val qexec = this.virtuosoClient.createQueryExecution(queryString);
      try {
        val rs = qexec.execSelect
        while (rs.hasNext) {
          val qs = rs.nextSolution
          val mappedClass = qs.get("downloadURL").toString;
          results = mappedClass :: results;
        }
      } finally qexec.close
    }

    new ListResult(results.length, results);
  }


  @throws(classOf[Exception])
  def executeMapping(mappingExecution: MappingExecution) : ExecuteMappingResult = {
    val mapper = new ObjectMapper();
    val callbackURL = mappingExecution.callbackURL;

    val executeMappingResult = if(callbackURL != null) {
      this.helper.executionQueue.enqueue(mappingExecution);
      new ExecuteMappingResult(
        HttpURLConnection.HTTP_ACCEPTED, HttpStatus.ACCEPTED.getReasonPhrase
        , mappingExecution
        , null
      )
    } else {
      var result:ExecuteMappingResult = null;
      while(result == null) {
        if(!this.helper.isProcessing && this.helper.executionQueue.size==0) {
          this.helper.isProcessing = true;
          val f = this.executeMappingWithFuture(mappingExecution);
          logger.info("Await.result");
          result = Await.result(f, 60 second)
          this.helper.isProcessing = false;
        } else {
          Thread.sleep(1000) // wait for 1000 millisecond
        }
      }

      result;
    }



    //MappingExecutionController.executionQueue.enqueue(mappingExecution);


    /*    val f = this.executeMappingWithFuture(mappingExecution);
        val mapper = new ObjectMapper();
        val callbackURL = mappingExecution.callbackURL
        val executeMappingResult = if(callbackURL == null) {
          logger.info("Await.result");
          val result = Await.result(f, 60 second)
          MappingExecutionController.executionQueue.dequeue()
          result;
        } else {
          f.onComplete {
            case Success(forkExecuteMappingResult:ExecuteMappingResult) => {
              logger.info("f.onComplete Success");

              val forkExecuteMappingResultAsString = mapper.writeValueAsString(forkExecuteMappingResult)
              logger.info(s"forkExecuteMappingResultAsString = ${forkExecuteMappingResultAsString}");

              val manifestFile = forkExecuteMappingResult.getManifest_download_url;
              val jsonObj = if(manifestFile == null ) {
                val annotatedDistributionURL = forkExecuteMappingResult.getMapping_execution_result_download_url;
                logger.debug(s"annotatedDistributionURL = ${annotatedDistributionURL}");

                val newJsonObj = new JSONObject();
                newJsonObj.put("@id", forkExecuteMappingResult.mappingExecutionResult.dctIdentifier);
                newJsonObj.put("downloadURL", annotatedDistributionURL);

                val context = new JSONObject();
                newJsonObj.put("@context", context);

                val downloadURLContext = new JSONObject();
                context.put("downloadURL", downloadURLContext);

                downloadURLContext.put("type", "@id")
                downloadURLContext.put("@id", "http://www.w3.org/ns/dcat#downloadURL")

                newJsonObj
              } else {
                val manifestStringJsonLd = JenaClient.urlToString(manifestFile, Some("TURTLE"));
                val jsonObjFromManifest = new JSONObject(manifestStringJsonLd);
                jsonObjFromManifest
              }

              val response = Unirest.post(callbackURL)
                .header("Content-Type", "application/json")
                .body(jsonObj)
                .asString();
              logger.info(s"POST to ${callbackURL} with body = ${jsonObj.toString(3)}")

              try {
                logger.info(s"response from callback = ${response.getBody}")
              } catch {
                case e:Exception => {
                  e.printStackTrace()
                }
              }

              MappingExecutionController.executionQueue.dequeue()
            }
            case Failure(e) => {
              logger.info("f.onComplete Success Failure");
              e.printStackTrace
              MappingExecutionController.executionQueue.dequeue()
            }
          }


          logger.info("In Progress");
          new ExecuteMappingResult(
            HttpURLConnection.HTTP_ACCEPTED, HttpStatus.ACCEPTED.getReasonPhrase
            , mappingExecution
            , null
          )
        }*/



    try {
      val executeMappingResultAsString = mapper.writeValueAsString(executeMappingResult);
      logger.info(s"executeMappingResult = ${executeMappingResult}");
    } catch {
      case e:Exception => {
        logger.error(s"executeMappingResult = ${executeMappingResult}")
      }
    }

    executeMappingResult
  }

  @throws(classOf[Exception])
  def executeMappingWithFuture(mappingExecution: MappingExecution)
  : Future[ExecuteMappingResult] = {
    val pStoreToGithub = mappingExecution.pStoreToGithub
    val useCache = mappingExecution.useCache
    val pStoreToCKAN = mappingExecution.storeToCKAN;
    val mdId = mappingExecution.mdId;
    val pMdHash = mappingExecution.mdHash
    val mdDownloadURL = mappingExecution.mdDownloadURL

    val f = Future {
      var errorOccured = false;
      var collectiveErrorMessage: List[String] = Nil;

      //val md = mappingExecution.mappingDocument
      val unannotatedDistributions = mappingExecution.unannotatedDistributions
      val dataset = unannotatedDistributions.iterator.next().dataset;

      val organization = dataset.dctPublisher
      //val unannotatedDistributions = dataset.getUnannotatedDistributions
      val unannotatedDatasetHash = MappingPediaUtility.calculateHash(
        unannotatedDistributions);

      //val mdDownloadURL = md.getDownloadURL();
      logger.info(s"pMdHash = ${pMdHash}");
      logger.info(s"mdDownloadURL = ${mdDownloadURL}");
      val mdHash = if (pMdHash == null && mdDownloadURL != null ) {
        MappingPediaUtility.calculateHash(mdDownloadURL, "UTF-8");
      } else {
        pMdHash
      }

      val cacheExecutionURL = this.findByHash(mdHash, unannotatedDatasetHash);
      logger.debug(s"cacheExecutionURL = ${cacheExecutionURL}");

      if(cacheExecutionURL == null || cacheExecutionURL.results.isEmpty || !useCache) {
        val mappedClasses:String = try {
          //this.mappingDocumentController.findMappedClassesByMappingDocumentId(md.dctIdentifier).results.mkString(",");

          val mappedClassesURL = MPCConstants.ENGINE_DATASETS_SERVER + "mapped_classes?mapping_document_id=" + mdId;
          logger.info("mappedClassesURL = " + mappedClassesURL);
          val response = Unirest.get(mappedClassesURL).asJson();
          if(response.getStatus >= 200 && response.getStatus < 300) {
            response.getBody().getObject().getJSONArray("results").toList.toArray.toList.mkString(",")
          } else { null }
        } catch {
          case e:Exception => {
            e.printStackTrace()
            null;
          }
        }
        logger.info(s"mappedClasses = $mappedClasses")

        val organizationId = if(organization != null) { organization.dctIdentifier }
        else { UUID.randomUUID.toString }

        val datasetId = dataset.dctIdentifier

        val mappingExecutionId = UUID.randomUUID.toString;
        val annotatedDistribution = new AnnotatedDistribution(dataset, mappingExecutionId)
        annotatedDistribution.dcatMediaType = mappingExecution.pOutputMediaType
        annotatedDistribution.dctDescription = "Annotated Dataset using the annotation: " + mdDownloadURL;


        val mappingExecutionDirectory = s"executions/$organizationId/$datasetId/${mdId}";

        val outputFileNameWithExtension:String = mappingExecution.getOutputFileWithExtension;

        val githubOutputFilepath:String = s"$mappingExecutionDirectory/$outputFileNameWithExtension"
        val localOutputDirectory = s"$mappingExecutionDirectory/$mappingExecutionId";
        mappingExecution.outputDirectory = localOutputDirectory;
        val localOutputFilepath:String = s"$localOutputDirectory/$outputFileNameWithExtension"
        val localOutputFile: File = new File(localOutputFilepath)
        annotatedDistribution.distributionFile = localOutputFile;

        //EXECUTING MAPPING
        try {
          MappingExecutionController.executeMapping(mappingExecution);
        }
        catch {
          case e: Exception => {
            errorOccured = true;
            e.printStackTrace()
            val errorMessage = "Error executing mapping: " + e.getMessage
            logger.error(errorMessage)
            collectiveErrorMessage = errorMessage :: collectiveErrorMessage
          }
        }



        //STORING MAPPING EXECUTION RESULT ON GITHUB
        val githubResponse = if(this.properties.githubEnabled && pStoreToGithub) {
          try {
            val response = githubClient.encodeAndPutFile(githubOutputFilepath
              , "add mapping execution result by mappingpedia engine", localOutputFile);
            response
          } catch {
            case e: Exception => {
              e.printStackTrace()
              errorOccured = true;
              val errorMessage = "Error storing mapping execution result on GitHub: " + e.getMessage
              logger.error(errorMessage)
              collectiveErrorMessage = errorMessage :: collectiveErrorMessage
              null
            }
          }
        } else {
          null
        }


        val mappingExecutionResultURL = if(githubResponse != null) {
          if (HttpURLConnection.HTTP_CREATED == githubResponse.getStatus || HttpURLConnection.HTTP_OK == githubResponse.getStatus) {
            githubResponse.getBody.getObject.getJSONObject("content").getString("url");
          } else {
            errorOccured = true;
            val errorMessage = "Error storing mapping execution result on GitHub: " + githubResponse.getStatus
            logger.error(errorMessage)
            collectiveErrorMessage = errorMessage :: collectiveErrorMessage
            null
          }
        } else {
          null
        }
        annotatedDistribution.dcatAccessURL = mappingExecutionResultURL;

        val mappingExecutionResultDownloadURL = if(mappingExecutionResultURL != null) {
          try {
            val response = Unirest.get(mappingExecutionResultURL).asJson();
            response.getBody.getObject.getString("download_url");
          } catch {
            case e:Exception => mappingExecutionResultURL
          }
        } else {
          null
        }
        logger.info(s"mappingExecutionResultDownloadURL = $mappingExecutionResultDownloadURL")
        annotatedDistribution.dcatDownloadURL = mappingExecutionResultDownloadURL;



        //STORING MAPPING EXECUTION RESULT AS A RESOURCE ON CKAN
        val ckanAddResourceResponse = try {
          if(this.properties.ckanEnable && pStoreToCKAN) {
            val annotatedResourcesIds = if(dataset.ckanPackageId != null) {
              ckanClient.getAnnotatedResourcesIds(dataset.ckanPackageId);
            } else { null }
            logger.info(s"annotatedResourcesIds = ${annotatedResourcesIds}");

            logger.info("STORING MAPPING EXECUTION RESULT ON CKAN ...")
            //val unannotatedDistributionsDownloadURLs = unannotatedDistributions.map(distribution => distribution.dcatDownloadURL);
            val mapTextBody:Map[String, String] = Map(
              MappingPediaConstant.CKAN_RESOURCE_ORIGINAL_DATASET_DISTRIBUTION_DOWNLOAD_URL ->
                unannotatedDistributions.map(distribution => distribution.dcatDownloadURL).mkString(",")
              , MappingPediaConstant.CKAN_RESOURCE_MAPPING_DOCUMENT_DOWNLOAD_URL -> mdDownloadURL
              //, MappingPediaConstant.CKAN_RESOURCE_PROV_TRIPLES -> annotatedDistribution.manifestDownloadURL
              , MappingPediaConstant.CKAN_RESOURCE_CLASS -> mappedClasses
              //, "$manifestDownloadURL" -> annotatedDistribution.manifestDownloadURL
              //, MappingPediaConstant.CKAN_RESOURCE_CLASSES -> mappedClasses
              , MappingPediaConstant.CKAN_RESOURCE_IS_ANNOTATED -> "true"
              , MappingPediaConstant.CKAN_RESOURCE_ORIGINAL_DISTRIBUTION_CKAN_ID ->
                unannotatedDistributions.map(distribution => distribution.ckanResourceId).mkString(",")
            )

            if(annotatedResourcesIds != null && annotatedResourcesIds.size >= 1 && mappingExecution.updateResource) {
              val updateStatusList = annotatedResourcesIds.map(annotatedResourceId => {
                annotatedDistribution.ckanResourceId = annotatedResourceId;
                ckanClient.updateResource(annotatedDistribution, Some(mapTextBody));
              })

              val updateStatus = updateStatusList.iterator.next();
              updateStatus
            } else {
              val createStatus = ckanClient.createResource(annotatedDistribution, Some(mapTextBody));
              createStatus
            }

          } else {
            null
          }
        }
        catch {
          case e: Exception => {
            errorOccured = true;
            e.printStackTrace()
            val errorMessage = "Error storing mapping execution result on CKAN: " + e.getMessage
            logger.error(errorMessage)
            collectiveErrorMessage = errorMessage :: collectiveErrorMessage
            null
          }
        }

        val ckanAddResourceResponseStatusCode:Integer = {
          if(ckanAddResourceResponse == null) { null }
          else { ckanAddResourceResponse.getStatusLine.getStatusCode }
        }

        if(ckanAddResourceResponseStatusCode != null && ckanAddResourceResponseStatusCode >= 200
          && ckanAddResourceResponseStatusCode <300) {
          try {
            val ckanAddResourceResult = MpcCkanUtility.getResult(ckanAddResourceResponse);
            val packageId = ckanAddResourceResult.getString("package_id")
            val resourceId = ckanAddResourceResult.getString("id")
            val resourceURL = ckanAddResourceResult.getString("url")

            annotatedDistribution.dcatAccessURL= s"${this.ckanClient.ckanUrl}/dataset/${packageId}/resource/${resourceId}";
            logger.debug(s"annotatedDistribution.dcatAccessURL = ${annotatedDistribution.dcatAccessURL}")

            annotatedDistribution.dcatDownloadURL = resourceURL;
            logger.debug(s"annotatedDistribution.dcatDownloadURL = ${annotatedDistribution.dcatDownloadURL}")
          } catch { case e:Exception => { e.printStackTrace() } }
        }



        //GENERATING MANIFEST FILE
        val manifestFile = MappingExecutionController.generateManifestFile(
          annotatedDistribution, unannotatedDistributions
          , mdId: String
          , mdHash: String
        )
        logger.info("Manifest file generated.")


        //STORING MANIFEST ON GITHUB
        val addManifestFileGitHubResponse:HttpResponse[JsonNode] =
          if(this.properties.githubEnabled && pStoreToGithub) {
            try {
              this.storeManifestFileOnGitHub(manifestFile, dataset, mdId);
            } catch {
              case e: Exception => {
                errorOccured = true;
                e.printStackTrace()
                val errorMessage = "error storing manifest file on GitHub: " + e.getMessage
                logger.error(errorMessage)
                collectiveErrorMessage = errorMessage :: collectiveErrorMessage
                null
              }
            }
          } else {
            null
          }
        //val manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse);
        annotatedDistribution.manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse);;

        //val manifestDownloadURL = this.githubClient.getDownloadURL(manifestAccessURL)
        annotatedDistribution.manifestDownloadURL = this.githubClient.getDownloadURL(annotatedDistribution.manifestAccessURL);








        //STORING MANIFEST FILE AS TRIPLES ON VIRTUOSO
        val addManifestVirtuosoResponse:String = try {
          if(this.properties.virtuosoEnabled) {
            this.storeManifestOnVirtuoso(manifestFile);
          } else {
            "Storing to Virtuoso is not enabled!";
          }
        } catch {
          case e: Exception => {
            errorOccured = true;
            e.printStackTrace()
            val errorMessage = "error storing manifest file of a mapping execution result on Virtuoso: " + e.getMessage
            val manifestFileInString = scala.io.Source.fromFile(manifestFile).getLines.reduceLeft(_+_)
            logger.error(errorMessage);
            logger.error(s"manifestFileInString = $manifestFileInString");
            collectiveErrorMessage = errorMessage :: collectiveErrorMessage
            e.getMessage
          }
        }

        //STORING EXECUTION RESULT FILE AS TRIPLES ON VIRTUOSO
        val addExecutionResultVirtuosoResponse:String = try {
          if(this.properties.virtuosoEnabled) {
            this.storeExecutionResultOnVirtuoso(localOutputFile);
          } else {
            "Storing to Virtuoso is not enabled!";
          }
        } catch {
          case e: Exception => {
            errorOccured = true;
            e.printStackTrace()
            val errorMessage = "error storing execution result triples on Virtuoso: " + e.getMessage
            val executionResultInString = scala.io.Source.fromFile(manifestFile).getLines.reduceLeft(_+_)
            logger.error(errorMessage);
            logger.error(s"executionResultInString = $executionResultInString");
            collectiveErrorMessage = errorMessage :: collectiveErrorMessage
            e.getMessage
          }
        }


        val (responseStatus, responseStatusText) = if(errorOccured) {
          (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
        } else {
          (HttpURLConnection.HTTP_OK, "OK")
        }

        logger.info("\n")

        new ExecuteMappingResult(
          responseStatus, responseStatusText
          , mappingExecution
          , annotatedDistribution
        )
      } else {
        val mappingExecutionResultURL = cacheExecutionURL.results.iterator.next().toString;
        val annotatedDistribution = new AnnotatedDistribution(dataset);
        annotatedDistribution.dcatDownloadURL = mappingExecutionResultURL;

        new ExecuteMappingResult(
          HttpURLConnection.HTTP_OK, "OK"
          , mappingExecution
          , annotatedDistribution
        )
      }
    }

    f;

  }


  def findInstancesByClass(className:String) = {
    logger.info("findInstancesByClass")
    val queryTemplateFile = "templates/findInstancesByClass.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> this.properties.getProperty(MappingPediaConstant.GRAPH_NAME)
      , "$className" -> className
    );

    val queryString: String = MpcUtility.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    logger.info(s"queryString = ${queryString}")
    this.findByQueryString(queryString);
  }

  def findByQueryString(queryString:String) = {
    logger.debug(s"queryString = $queryString");

    val qexec = this.virtuosoClient.createQueryExecution(queryString);

    var results: List[Instance] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val instanceId = qs.get("s").toString;
        val instanceType = qs.get("className").toString;
        val instanceTitle = MappingPediaUtility.getStringOrElse(qs, "title", null)
        val instance = new Instance(instanceId, instanceType, instanceTitle);
        results = instance :: results;
      }
    }
    catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"Error execution query: ${e.getMessage}")
      }
    }
    finally qexec.close

    val listResult = new ListResult[Instance](results.length, results);
    listResult
  }

  def getInstances(aClass:String, maxMappingDocuments:Integer, useCache:Boolean
                   , updateResource:Boolean) = {
    logger.info(s"useCache = ${useCache}");

    //val mappingDocuments = this.mappingDocumentController.findByClassAndProperty(aClass, null, true).results
    val url = s"${MPCConstants.ENGINE_DATASETS_SERVER}ogd/annotations?class={aClass}";
    logger.info("url = " + url);
    val jsonResponse = Unirest.get(url).asJson();
    val mappingDocuments = jsonResponse.getBody().getObject().getJSONArray("results").toList.toArray.toList

    var executedMappingDocuments:List[(String, String)]= Nil;

    var i = 0;
    val executionResults:Iterable[ExecuteMappingResult] = mappingDocuments.flatMap(
      mdAux => {
        val md = mdAux.asInstanceOf[MappingDocument];
        val mappingLanguage = md.mappingLanguage;
        val distributionFieldSeparator = if(md.distributionFieldSeparator != null
          && md.distributionFieldSeparator.isDefined) {
          md.distributionFieldSeparator.get
        } else {
          null
        }
        val outputFileName = UUID.randomUUID.toString
        val outputFileExtension = ".nt";
        //val mappingDocumentDownloadURL = md.getDownloadURL();

        val dataset = new Dataset(new Agent());
        val unannotatedDistribution = new UnannotatedDistribution(dataset);
        val unannotatedDistributions = List(unannotatedDistribution);
        //dataset.addDistribution(unannotatedDistribution);
        unannotatedDistribution.dcatDownloadURL = md.dataset.getDistribution().dcatDownloadURL;
        unannotatedDistribution.hash = md.dataset.getDistribution().hash
        if (unannotatedDistribution.hash == null && unannotatedDistribution.dcatDownloadURL != null ) {
          val hashValue = MappingPediaUtility.calculateHash(unannotatedDistribution.dcatDownloadURL, unannotatedDistribution.encoding);
          unannotatedDistribution.hash = hashValue
        }


        logger.info(s"mapping document SHA = ${md.hash}");
        logger.info(s"dataset distribution hash = ${unannotatedDistribution.hash}");

        if(executedMappingDocuments.contains((md.hash,unannotatedDistribution.hash))) {
          None
        } else {
          if(i < maxMappingDocuments) {
            val jDBCConnection = null;
            val queryFileName = null;
            val outputMediaType = "text/turtle";

            val mappingExecution = new MappingExecution(
              //md
              unannotatedDistributions
              , jDBCConnection, queryFileName
              , outputFileName, outputFileExtension, outputMediaType
              , false
              , true
              , false
              , useCache
              , null
              , updateResource
              , md.dctIdentifier
              , md.hash
              , md.getDownloadURL()
              , md.getMapping_language
            );

            val mappingExecutionURLs = if(useCache) { this.findByHash(md.hash,unannotatedDistribution.hash); }
            else { null }

            if(useCache && mappingExecutionURLs != null && mappingExecutionURLs.results.size > 0) {
              val cachedMappingExecutionResultURL:String = mappingExecutionURLs.results.iterator.next().toString;
              logger.info(s"cachedMappingExecutionResultURL = " + cachedMappingExecutionResultURL)

              i +=1
              val mappingExecutionResult = new AnnotatedDistribution(dataset);
              mappingExecutionResult.dcatAccessURL = cachedMappingExecutionResultURL;
              mappingExecutionResult.dcatDownloadURL = cachedMappingExecutionResultURL;

              Some(new ExecuteMappingResult(HttpURLConnection.HTTP_OK, "OK"
                , mappingExecution, mappingExecutionResult))
            } else {
              val unannotatedDistributions = dataset.getUnannotatedDistributions;
              val storeToCKAN = MappingPediaUtility.stringToBoolean("false");
              mappingExecution.storeToCKAN = storeToCKAN

              //THERE IS NO NEED TO STORE THE EXECUTION RESULT IN THIS PARTICULAR CASE
              val executionResult = this.executeMapping(mappingExecution);
              //val executionResult = MappingExecutionController.executeMapping2(mappingExecution);

              executedMappingDocuments = (md.hash,unannotatedDistribution.hash) :: executedMappingDocuments;

              val executionResultAccessURL = executionResult.getMapping_execution_result_access_url()
              val executionResultDownloadURL = executionResult.getMapping_execution_result_download_url
              //executionResultURL;

              i +=1
              Some(executionResult);
            }
          } else {
            None
          }
        }



      })
    new ListResult(executionResults.size, executionResults);

  }


  def storeManifestFileOnGitHub(manifestFile:File, dataset:Dataset
                                //                                , mappingDocument: MappingDocument
                                , mdId:String
                               ) = {
    val organization = dataset.dctPublisher;

    logger.info("storing manifest file on github ...")
    val addNewManifestCommitMessage = s"Add manifest file for the execution of mapping document: ${mdId}"
    val githubResponse = githubClient.encodeAndPutFile(organization.dctIdentifier
      , dataset.dctIdentifier, manifestFile.getName, addNewManifestCommitMessage
      , manifestFile)
    logger.info("manifest file stored on github ...")
    githubResponse
  }

  def storeManifestOnVirtuoso(manifestFile:File) = {
    if(manifestFile != null) {
      logger.info("storing the manifest triples of a mapping execution result on virtuoso ...")
      logger.debug("manifestFile = " + manifestFile);
      this.virtuosoClient.storeFromFile(manifestFile)
      logger.info("manifest triples stored on virtuoso.")
      "OK";
    } else {
      "No manifest file specified/generated!";
    }
  }

  def storeExecutionResultOnVirtuoso(executionResultFile:File) = {
    if(executionResultFile != null) {
      logger.info("storing the execution result triples on virtuoso ...")
      logger.debug("executionResultFile = " + executionResultFile);
      this.virtuosoClient.storeFromFile(executionResultFile)
      logger.info("execution result triples stored on virtuoso.")
      "OK";
    } else {
      "No execution result file specified/generated!";
    }
  }
}

