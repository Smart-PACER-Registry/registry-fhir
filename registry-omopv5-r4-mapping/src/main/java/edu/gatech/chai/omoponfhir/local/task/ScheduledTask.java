package edu.gatech.chai.omoponfhir.local.task;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.codec.binary.Base64;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;

import ca.uhn.fhir.parser.IParser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.opencsv.CSVParser;

import edu.gatech.chai.omoponfhir.local.dao.FhirOmopVocabularyMapImpl;
import edu.gatech.chai.omoponfhir.local.model.FhirOmopVocabularyMapEntry;
import edu.gatech.chai.omoponfhir.omopv5.r4.mapping.OmopServerOperations;
import edu.gatech.chai.omoponfhir.omopv5.r4.utilities.StaticValues;
import edu.gatech.chai.omopv5.dba.service.ConceptRelationshipService;
import edu.gatech.chai.omopv5.dba.service.ConceptService;
import edu.gatech.chai.omopv5.dba.service.ParameterWrapper;
import edu.gatech.chai.omopv5.dba.service.RelationshipService;
import edu.gatech.chai.omopv5.dba.service.CaseLogService;
import edu.gatech.chai.omopv5.dba.service.CaseInfoService;
import edu.gatech.chai.omopv5.dba.service.VocabularyService;
import edu.gatech.chai.omopv5.model.entity.Concept;
import edu.gatech.chai.omopv5.model.entity.ConceptRelationship;
import edu.gatech.chai.omopv5.model.entity.ConceptRelationshipPK;
import edu.gatech.chai.omopv5.model.entity.Relationship;
import edu.gatech.chai.omopv5.model.entity.CaseInfo;
import edu.gatech.chai.omopv5.model.entity.CaseLog;
import edu.gatech.chai.omopv5.model.entity.Vocabulary;

@Component
public class ScheduledTask {
	private static final Logger logger = LoggerFactory.getLogger(ScheduledTask.class);
	ObjectMapper mapper = new ObjectMapper();

	private String smartPacerBasicAuth = "user:secret";
	private OmopServerOperations myMapper;

	@Autowired
	private ConceptService conceptService;
	@Autowired
	private ConceptRelationshipService conceptRelationshipService;
	@Autowired
	private CaseInfoService caseInfoService;
	@Autowired
	private CaseLogService caseLogService;
	@Autowired
	private RelationshipService relationshipService;
	@Autowired
	private VocabularyService vocabularyService;

	private Long conceptIdStart;
	
	private Long thresholdDuration1;
	private Long thresholdDuration2;
	private Long thresholdDuration3;

	private Long queryPeriod1;
	private Long queryPeriod2;
	private Long queryPeriod3;

	protected FhirOmopVocabularyMapImpl fhirOmopVocabularyMap;

	public ScheduledTask() {
		conceptIdStart = StaticValues.CONCEPT_MY_SPACE;
		fhirOmopVocabularyMap = new FhirOmopVocabularyMapImpl();
		setSmartPacerBasicAuth("username:password");

		// We are using the server operations implementation. 
		WebApplicationContext myAppCtx = ContextLoaderListener.getCurrentWebApplicationContext();
		myMapper = new OmopServerOperations(myAppCtx);

		// Get PACER query logic variables.
		thresholdDuration1 = System.getenv("thresholdDuration1") == null ? StaticValues.TWO_WEEKS_IN_SEC : Long.getLong(System.getenv("thresholdDuration1"));
		thresholdDuration2 = System.getenv("thresholdDuration2") == null ? StaticValues.FOUR_WEEKS_IN_SEC : Long.getLong(System.getenv("thresholdDuration2"));
		thresholdDuration3 = System.getenv("thresholdDuration3") == null ? StaticValues.EIGHT_WEEKS_IN_SEC : Long.getLong(System.getenv("thresholdDuration3"));

		queryPeriod1 = System.getenv("queryPeriod1") == null ? 86400L : Long.getLong(System.getenv("queryPeriod1"));
		queryPeriod2 = System.getenv("queryPeriod2") == null ? 604800L : Long.getLong(System.getenv("queryPeriod2"));
		queryPeriod3 = System.getenv("queryPeriod3") == null ? 1209600L : Long.getLong(System.getenv("queryPeriod3"));
	}

	public String getSmartPacerBasicAuth() {
		return this.smartPacerBasicAuth;
	}

	public void setSmartPacerBasicAuth(String smartPacerBasicAuth) {
		this.smartPacerBasicAuth = smartPacerBasicAuth;
	}

	protected void writeToLog (CaseInfo caseInfo, String message) {
		CaseLog caseLog = new CaseLog();

		caseLog.setCaseInfo(caseInfo);
		caseLog.setText(message);
		caseLog.setLogDateTime(new Date());
		caseLogService.create(caseLog);	
	}

	protected void changeCaseInfoStatus (CaseInfo caseInfo, String status) {
		caseInfo.setStatus(status);
		caseInfoService.update(caseInfo);
	}

	@Scheduled(initialDelay = 30000, fixedDelay = 30000)
	public void runPeriodicQuery() {
		List<CaseInfo> caseInfos = caseInfoService.searchWithoutParams(0, 0, "id ASC");
		RestTemplate restTemplate = new RestTemplate();

		for (CaseInfo caseInfo : caseInfos) {
			Date currentTime = new Date();
			if (StaticValues.ACTIVE.equals(caseInfo.getStatus())) {
				// check if it's time to do the query.
				if (currentTime.before(caseInfo.getTriggerAt())) {
					continue;
				}

				// call status URL to get FHIR syphilis registry data.
				String statusURL = caseInfo.getStatusUrl();
				HttpEntity<String> reqAuth = new HttpEntity<String>(createHeaders());
				ResponseEntity<String> response = null;
		
				try {
					response = restTemplate.exchange(statusURL, HttpMethod.GET, reqAuth, String.class);
				} catch (HttpClientErrorException e) {
					// this is error like 4xx.
					writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") STATUS GET FAILED: " + e.getMessage());		
					e.printStackTrace();

					// If the error is 404, then change the task type to REQUEST.
					if (HttpStatus.NOT_FOUND == e.getStatusCode()) {
						writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") STATUS Changing to " + StaticValues.REQUEST);		
						changeCaseInfoStatus(caseInfo, StaticValues.REQUEST);
					}
					continue;
				}

				HttpStatus statusCode = response.getStatusCode();
				if (!statusCode.is2xxSuccessful()) {
					logger.debug("Status Query Failed and Responded with statusCode:" + statusCode.toString());
					writeToLog(caseInfo, "Status Query Failed and Responded with statusCode:" + statusCode.toString());
				} else {
					// Get response body
					String responseBody = response.getBody();

					if (responseBody != null && !responseBody.isEmpty()) {
						String resultBundleString = null;
						try {
							JsonNode responseJson = mapper.readTree(responseBody);
							JsonNode resultBundle = responseJson.get("results");
							resultBundleString = mapper.writeValueAsString(resultBundle);
						} catch (JsonProcessingException e) {
							writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") was not successful: " + e.getMessage());
							e.printStackTrace();
							continue;
						}

						if (resultBundleString != null) {
							// Convert the response body to FHIR Resource
							IParser parser = StaticValues.myFhirContext.newJsonParser();
							Bundle responseBundle = parser.parseResource(Bundle.class, resultBundleString);

							// Save the response bundle to registry db.
							if (responseBundle != null && !responseBundle.isEmpty()) {
								List<BundleEntryComponent> entries = responseBundle.getEntry();
								List<BundleEntryComponent> responseEntries = myMapper.createEntries(entries, caseInfo);
								int errorFlag = 0;
								String errMessage = "";
								for (BundleEntryComponent responseEntry : responseEntries) {
									if (!responseEntry.getResponse().getStatus().startsWith("201") 
										&& !responseEntry.getResponse().getStatus().startsWith("200")) {
										String jsonResource = StaticValues.serializeIt(responseEntry.getResource());
										errMessage += "Failed to create/add " + jsonResource;
										logger.error(errMessage);
										errorFlag = 1;
									}
								}

								if (errorFlag == 1) {
									// Error occurred on one of resources.
									writeToLog(caseInfo, errMessage);
								} else {
									if (currentTime.before(new Date(caseInfo.getActivated().getTime()+thresholdDuration1))) {
										caseInfo.setTriggerAt(new Date(currentTime.getTime()+queryPeriod1));
									} else if (currentTime.before(new Date(caseInfo.getActivated().getTime()+thresholdDuration2))) {
										caseInfo.setTriggerAt(new Date(currentTime.getTime()+queryPeriod2));
									} else if (currentTime.before(new Date(caseInfo.getActivated().getTime()+thresholdDuration3))) {
										caseInfo.setTriggerAt(new Date(currentTime.getTime()+queryPeriod3));
									} else {
										caseInfo.setStatus(StaticValues.INACTIVE);
										writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") changed status to " + caseInfo.getStatus());
									}
									caseInfoService.update(caseInfo);
								}
							}
						} else {
							writeToLog(caseInfo, "The response for PACER query has no results");
						}
					}
				}
			} else if (StaticValues.REQUEST.equals(caseInfo.getStatus())) {
				// Send a request. This is triggered by a new ELR or NoSuchRequest from PACER server
				JsonNode patientNode = mapper.createObjectNode();
				String patientIdentifier = caseInfo.getPatientIdentifier();
				if (patientIdentifier != null) {
					((ObjectNode) patientNode).put("patient_id", patientIdentifier);

					JsonNode requestJson = mapper.createObjectNode();
					((ObjectNode) requestJson).put("jobType", "SyphilisRegistry");
					((ObjectNode) requestJson).set("params", patientNode);
			
					HttpHeaders headers = createHeaders();
					headers.setContentType(MediaType.APPLICATION_JSON);
					HttpEntity<JsonNode> entity = new HttpEntity<JsonNode>(requestJson, headers);

					ResponseEntity<String> response = null;
					try {
						response = restTemplate.postForEntity(caseInfo.getServerUrl() + "/Jobs/", entity, String.class);
						Map<String, String> what = entity.getHeaders().toSingleValueMap();
						for (Map.Entry<String, String> entry : what.entrySet()) {
							logger.debug(entry.getKey() + ":" + entry.getValue());
						}

						logger.debug(entity.getBody().toPrettyString());
					} catch (RestClientException e) {
						// log this session
						writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") REQUEST FAILED: " + e.getMessage());
						e.printStackTrace();
						continue;
					}
					if (response.getStatusCode().equals(HttpStatus.CREATED) || response.getStatusCode().equals(HttpStatus.OK)) {
						// Get Location
						Long jobId = 0L;
						String statusUrl = "";
						HttpHeaders responseHeaders = response.getHeaders();
						URI locationUri = responseHeaders.getLocation();

						if (locationUri != null) {
							String locationUriPath = locationUri.getPath();
							if (locationUriPath != null) {
								if (locationUri.isAbsolute()) {
									statusUrl = locationUriPath;
								} else {
									statusUrl = caseInfo.getServerUrl() + locationUriPath;
								}

								String[] paths = locationUriPath.split("/");
								String jobIdString = paths[paths.length - 1];
								jobId = Long.parseLong(jobIdString);
							}
						}

						if (jobId == 0L) {
							// We failed to get a JobID.
							writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") failed to get jobId");
						} else {
							if (statusUrl != null && !statusUrl.isEmpty()) {
								// Done. set it to in query
								caseInfo.setStatusUrl(statusUrl);
								caseInfo.setJobId(jobId);
								caseInfo.setStatus(StaticValues.ACTIVE);
								caseInfo.setActivated(currentTime);

								// set the triggered_at using first query period
								Long triggeredAt = currentTime.getTime() + queryPeriod1;
								caseInfo.setTriggerAt(new Date(triggeredAt));

								caseInfoService.update(caseInfo);

								// log this session
								writeToLog(caseInfo, "caes info (" + caseInfo.getId() + ") is updated to IN_QUERY");
							} else {
								writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") gets incorrect response");
							}
						}
					} else {
						writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") error response (" + response.getStatusCode().toString() + ")");
					}					
				} else {
					// This cannot happen as patient identifier is a required field.
					// BUt, if this ever happens, we write this in session log and return.
					writeToLog(caseInfo, "case info (" + caseInfo.getId() + ") without patient identifier");
				}
			}
		}
	}

	@Scheduled(fixedDelay = 60000)
	public void localCodeMappingTask() {
		// We may need to load local mapping data. Get a path where the mapping CSV
		// file(s) are located and load them if files exist. The files will then be
		// deleted.
		CSVParser parser = new CSVParser();
		String localMappingFilePath = System.getenv("LOCAL_CODEMAPPING_FILE_PATH");

		if (localMappingFilePath != null && !localMappingFilePath.trim().isEmpty()
				&& !"none".equalsIgnoreCase(localMappingFilePath)) {
			logger.debug("LocalMappingFilePath is set to " + localMappingFilePath);

			// create if folder does not exist.
			Path path = Paths.get(localMappingFilePath);

	        if (!Files.exists(path)) {
	        	try {
					Files.createDirectory(path);
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
	        }

			// get the list of files in this path.
			BufferedReader reader = null;
			try (Stream<Path> walk = Files.walk(path)) {
				List<String> result = walk.filter(Files::isRegularFile).map(x -> x.toString())
						.collect(Collectors.toList());

				for (String aFile : result) {
					reader = new BufferedReader(new FileReader(aFile));
					String line = reader.readLine();
					int i = 0;

					String omopSourceVocab = null;
					String omopTargetVocab = null;
					String fhirSourceCodeSystem = null;
					String fhirTargetCodeSystem = null;

					int sourceCodeSystemIndex = -1;
					int sourceCodeIndex = -1;
					int sourceCodeDescIndex = -1;
					int targetCodeIndex = -1;

					while (line != null) {
						String line_ = line.trim();
						if (line_.startsWith("#") || line_.charAt(1) == '#' || line_.isEmpty()) {
							// This is comment line skip...
							line = reader.readLine();
							continue;
						}

						i++;
						if (i <= 2) {
							// First line. Must identify coding translation.
							String[] mappingCodes = parser.parseLine(line_);
							if (mappingCodes.length != 2) {
								// Incorrectly formed file. delete this file and move to next one.
								logger.error("Line #" + i + " must be two values. But, there are " + mappingCodes.length
										+ " values. values=" + line_ + ". File, " + aFile
										+ ", is skipped and deleted.");
								reader.close();
								Files.deleteIfExists(Paths.get(aFile));
								break;
							}

							if (i == 1) {
								omopSourceVocab = mappingCodes[0];
								omopTargetVocab = mappingCodes[1];

								if (vocabularyService.findById(omopTargetVocab) == null) {
									logger.error("Line #" + i + " must have standard coding for target. See if "
											+ omopTargetVocab + " exists in OMOP Concept table. File, " + aFile
											+ "is skipped and deleted.");
									reader.close();
									Files.deleteIfExists(Paths.get(aFile));
									break;
								}

								// Done for line 1.
							} else {
								fhirSourceCodeSystem = mappingCodes[0];
								fhirTargetCodeSystem = mappingCodes[1];

								// Done for line 2.
							}
							line = reader.readLine();
							continue;
						}

						if (omopSourceVocab == null || omopTargetVocab == null || fhirSourceCodeSystem == null
								|| fhirTargetCodeSystem == null) {
							// Incorrectly formed file.
							logger.error(
									"OMOP Vocabulary, OMOP Concept Type and FHIR Code System must be defined in the first 2 lines. File, "
											+ aFile + ", is skipped and deleted.");
							reader.close();
							Files.deleteIfExists(Paths.get(aFile));
							break;
						}

						if (i == 3) {
							// This is a header. Get right index for our needs
							String[] mappingCodes = parser.parseLine(line_);
							for (int index = 0; index < mappingCodes.length; index++) {
								if ("SOURCE_CODESYSTEM".equals(mappingCodes[index])) {
									sourceCodeSystemIndex = index;
									continue;
								}
								if ("SOURCE_CODE".equals(mappingCodes[index])) {
									sourceCodeIndex = index;
									continue;
								}
								if ("SOURCE_DESC".equals(mappingCodes[index])) {
									sourceCodeDescIndex = index;
									continue;
								}
								if ("TARGET_CODE".equals(mappingCodes[index])) {
									targetCodeIndex = index;
									continue;
								}
							}

							if (sourceCodeSystemIndex == -1 || sourceCodeIndex == -1 || targetCodeIndex == -1) {
								// These MUST be set.
								logger.error("localCodeMapping failed to set index(es). sourceCodeSystemIndex="
										+ sourceCodeSystemIndex + ", sourceCodeIndex=" + sourceCodeIndex
										+ ", and targetCodeIndex=" + targetCodeIndex + ". This file, " + aFile
										+ ", is skipped and deleted");
								reader.close();
								Files.deleteIfExists(Paths.get(aFile));
								break;
							}

							// We got indexes. Done for line 3.
							line = reader.readLine();
							continue;
						}

						// Now, we are at the actual code translation rows.
						String[] omopSrc = omopSourceVocab.split("\\^");
						logger.debug(omopSrc[0]+"\n"+omopSrc[1]+"\n\n\n\n\n\n\n\n\n\n\n");
						Vocabulary myVocab = vocabularyService.findById(omopSrc[0]);
						if (myVocab == null) {
							// We need to add this to our local code mapping database.
							myVocab = createNewEntry(omopSrc, fhirSourceCodeSystem);
							if (myVocab == null) {
								logger.error("localCodeMapping failed to create a new entry for " + omopSrc[0]
										+ ". This file, " + aFile + ", is skipped and deleted");
								reader.close();
								Files.deleteIfExists(Paths.get(aFile));
								break;
							}
						}

						// Now CSV lines. parse one line at a time.
						String[] fields = parser.parseLine(line);

						List<ParameterWrapper> paramList;
						ParameterWrapper param = new ParameterWrapper();

						// From target code, collect necessary information such as domain id and concept
						// class id.
						String targetCode = fields[targetCodeIndex];
						param.setParameterType("String");
						param.setParameters(Arrays.asList("vocabulary.id", "conceptCode"));
						param.setOperators(Arrays.asList("=", "="));
						param.setValues(Arrays.asList(omopTargetVocab, targetCode));
						param.setRelationship("and");
						paramList = Arrays.asList(param);
						List<Concept> retParam = conceptService.searchWithParams(0, 0, paramList, null);
						if (retParam.size() == 0) {
							// We should have this target code in the concept table.
							logger.error("localCodeMapping task failed to locate the target code system, "
									+ omopTargetVocab + "/" + targetCode + ". Skipping line #" + i);
							line = reader.readLine();
							continue;
						}
						Concept targetConcept = retParam.get(0);

						// Create concept and concept relationship
						String sourceCodeName = fields[sourceCodeSystemIndex];
						if (!fhirSourceCodeSystem.equals(sourceCodeName) && !omopSrc[0].equals(sourceCodeName)) {
							logger.error("The Source Code System, " + sourceCodeName + ", name should be either "
									+ fhirSourceCodeSystem + " or " + omopSrc[0] + ". Skipping line #" + i);
							line = reader.readLine();
							continue;
						}

						// Check the source code. If we don't have this, add it to concept table.
						String sourceCode = fields[sourceCodeIndex];

						param.setParameterType("String");
						param.setParameters(Arrays.asList("vocabulary.id", "conceptCode"));
						param.setOperators(Arrays.asList("=", "="));
						param.setValues(Arrays.asList(omopSrc[0], sourceCode));
						param.setRelationship("and");

						retParam = conceptService.searchWithParams(0, 0, paramList, null);

						Concept sourceConcept;
						if (retParam.size() <= 0) {
							sourceConcept = new Concept();
							sourceConcept.setId(getTheLargestConceptId());

							String conceptName;
							if (sourceCodeDescIndex >= 0 && fields[sourceCodeDescIndex] != null
									&& !fields[sourceCodeDescIndex].trim().isEmpty()) {
								conceptName = fields[sourceCodeDescIndex];
							} else {
								conceptName = omopSrc[0];
							}

							sourceConcept.setConceptName(conceptName);
							sourceConcept.setDomainId(targetConcept.getDomainId());
							sourceConcept.setVocabularyId(myVocab.getId());
							sourceConcept.setConceptClassId(targetConcept.getConceptClassId());
							sourceConcept.setConceptCode(sourceCode);
							sourceConcept.setValidStartDate(targetConcept.getValidStartDate());
							sourceConcept.setValidEndDate(targetConcept.getValidEndDate());

							sourceConcept = conceptService.create(sourceConcept);
							if (sourceConcept == null) {
								logger.error("The Source Code, " + sourceCodeName + "|" + sourceCode
										+ ", could not be created. Skipping line #" + i);
								line = reader.readLine();
								continue;
							}
						} else {
							sourceConcept = retParam.get(0);
						}

						// Now create relationship if this relationship does not exist.
						String relationshipId = omopSrc[0] + " - " + omopTargetVocab + " eq";
						String relationshipName = omopSrc[0] + " to " + omopTargetVocab + " equivalent";
						String revRelationshipId = omopTargetVocab + " - " + omopSrc[0] + " eq";

						Relationship relationship = relationshipService.findById(relationshipId);
						if (relationship == null) {
							relationship = createOmopRelationshipConcept(relationshipId, relationshipName,
									revRelationshipId);
						}

						// see if this relationship exists. If not create one.
						ConceptRelationshipPK conceptRelationshipPk = new ConceptRelationshipPK(sourceConcept.getId(),
								targetConcept.getId(), relationshipId);
						ConceptRelationship conceptRelationship = conceptRelationshipService
								.findById(conceptRelationshipPk);
						if (conceptRelationship != null) {
							line = reader.readLine();
							continue;
						}

						// Create concept_relationship entry
						conceptRelationship = new ConceptRelationship();
						conceptRelationship.setId(conceptRelationshipPk);
						conceptRelationship.setValidStartDate(new Date(0L));
						SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
						try {
							Date date = format.parse("2099-12-31");
							conceptRelationship.setValidEndDate(date);
						} catch (ParseException e) {
							e.printStackTrace();
						}

						conceptRelationshipService.create(conceptRelationship);

						// read next line
						line = reader.readLine();

					} // while
					reader.close();
					Files.deleteIfExists(Paths.get(aFile));
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	private HttpHeaders createHeaders() {
		HttpHeaders httpHeaders = new HttpHeaders();
		byte[] encodedAuth = Base64.encodeBase64(smartPacerBasicAuth.getBytes());
		String authHeader = "Basic " + new String(encodedAuth);
		httpHeaders.set("Authorization", authHeader);
		httpHeaders.setAccept(Arrays.asList(new MediaType("application", "fhir+json")));

		return httpHeaders;
	}

	private Long getTheLargestConceptId() {
		Long largestId = conceptService.getLargestId();
		if (largestId != null && largestId >= conceptIdStart) {
			conceptIdStart = largestId + 1L;
		}

		return conceptIdStart;
	}

	private Vocabulary createNewEntry(String[] omopVacab, String fhirCoding) {
		Vocabulary vocab = createOmopVocabularyConcept(omopVacab);

		// Create FHIR representation of the vocabulary.
		String fhirCodeSystem = fhirOmopVocabularyMap.getFhirSystemNameFromOmopVocabulary(vocab.getId());
		if ("none".equalsIgnoreCase(fhirCodeSystem)) {
			// add this to local omopvocab2fhir code map db
			FhirOmopVocabularyMapEntry conceptMapEntry;
			if (fhirCodeSystem.startsWith("http")) {
				conceptMapEntry = new FhirOmopVocabularyMapEntry(vocab.getId(), fhirCoding, null);
			} else {
				conceptMapEntry = new FhirOmopVocabularyMapEntry(vocab.getId(), null, fhirCoding);
			}

			fhirOmopVocabularyMap.save(conceptMapEntry);
		}

		return vocab;
	}

	private Vocabulary createOmopVocabularyConcept(String[] values) {
		Vocabulary newVocab = new Vocabulary();
		String vocName = null;
		newVocab.setId(values[0]);
		if (values.length > 1) {
			vocName = values[1];
			newVocab.setVocabularyName(values[1]);
		}
		
		if (values.length > 2) {
			newVocab.setVocabularyReference(values[2]);
		} else {
			newVocab.setVocabularyReference("OMOPonFHIR generated");
		}
		
		if (values.length > 3) {
			newVocab.setVocabularyVersion(values[3]);
		}

		// If we created a new vocabulary, we also need to add this to Concept table.
		// Add to concept table and put the new concept id to vocabulary table.

		// create concept
		String name;
		if (vocName != null)
			name = vocName;
		else
			name = values[0];

		// See if we have this vocabulary in concept.
		List<ParameterWrapper> paramList;
		ParameterWrapper param = new ParameterWrapper();
		param.setParameterType("String");
		param.setParameters(Arrays.asList("name", "vocabulary.id", "conceptCode"));
		param.setOperators(Arrays.asList("=", "="));
		param.setValues(Arrays.asList(name, "Vocabulary", "OMOPonFHIR generated"));
		param.setRelationship("and");
		paramList = Arrays.asList(param);
		List<Concept> concepts = conceptService.searchWithParams(0, 0, paramList, null);
		
		Concept vocConcept;
		if (!concepts.isEmpty()) {
			vocConcept = concepts.get(0);
		} else {
			vocConcept = createVocabularyConcept(name, "Vocabulary");
		}
		
		if (vocConcept == null)
			return null;
		
		newVocab.setVocabularyConcept(vocConcept);

		// create vocabulary
		return vocabularyService.create(newVocab);
	}

	private Relationship createOmopRelationshipConcept(String id, String name, String revId) {
		Relationship newRelationship = new Relationship();
		newRelationship.setId(id);
		newRelationship.setRelationshipName(name);
		newRelationship.setIsHierarchical('0');
		newRelationship.setDefinesAncestry('0');
		newRelationship.setReverseRelationshipId(revId);

		// See if we have this vocabulary in concept.
		List<ParameterWrapper> paramList;
		ParameterWrapper param = new ParameterWrapper();
		param.setParameterType("String");
		param.setParameters(Arrays.asList("name", "vocabulary.id", "conceptCode"));
		param.setOperators(Arrays.asList("=", "="));
		param.setValues(Arrays.asList(name, "Relationship", "OMOPonFHIR generated"));
		param.setRelationship("and");
		paramList = Arrays.asList(param);
		List<Concept> concepts = conceptService.searchWithParams(0, 0, paramList, null);
		
		Concept relationshipConcept;
		if (!concepts.isEmpty()) {
			relationshipConcept = concepts.get(0);
		} else {
			relationshipConcept = createVocabularyConcept(name, "Relationship");
		}

		if (relationshipConcept == null)
			return null;
		newRelationship.setRelationshipConcept(relationshipConcept);

		// create vocabulary
		return relationshipService.create(newRelationship);
	}

	private Concept createVocabularyConcept(String name, String vocabId) {
		Concept conceptVoc = new Concept();
		conceptVoc.setId(getTheLargestConceptId());
		conceptVoc.setConceptName(name);
		conceptVoc.setDomainId("Metadata");
		
		conceptVoc.setVocabularyId(vocabId);
		conceptVoc.setConceptClassId(vocabId);
		conceptVoc.setConceptCode("OMOPonFHIR generated");
		conceptVoc.setValidStartDate(new Date(0L));

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = format.parse("2099-12-31");
			conceptVoc.setValidEndDate(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		// Create concept
		logger.debug("Trying to create a concept:\n"+conceptVoc.toString());
		Concept newConcept = conceptService.create(conceptVoc);
		if (newConcept != null) {
			logger.debug("Scheduled Task: new concept created for " + name);
		} else {
			logger.debug("Scheduled Task: creating a new concept for " + name + "failed. Vocabulary not created");
			return null;
		}

		return newConcept;
	}
}
