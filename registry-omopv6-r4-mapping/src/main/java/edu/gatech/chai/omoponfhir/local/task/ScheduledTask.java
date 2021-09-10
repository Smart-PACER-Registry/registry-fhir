package edu.gatech.chai.omoponfhir.local.task;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.codec.binary.Base64;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ResourceType;
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
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;

import ca.uhn.fhir.parser.IParser;

import com.opencsv.CSVParser;

import edu.gatech.chai.omoponfhir.local.dao.FhirOmopVocabularyMapImpl;
import edu.gatech.chai.omoponfhir.local.model.FhirOmopVocabularyMapEntry;
import edu.gatech.chai.omoponfhir.omopv6.r4.mapping.OmopServerOperations;
import edu.gatech.chai.omoponfhir.omopv6.r4.utilities.StaticValues;
import edu.gatech.chai.omopv6.dba.service.ConceptRelationshipService;
import edu.gatech.chai.omopv6.dba.service.ConceptService;
import edu.gatech.chai.omopv6.dba.service.ParameterWrapper;
import edu.gatech.chai.omopv6.dba.service.RelationshipService;
import edu.gatech.chai.omopv6.dba.service.SSessionLogsService;
import edu.gatech.chai.omopv6.dba.service.SSessionService;
import edu.gatech.chai.omopv6.dba.service.VocabularyService;
import edu.gatech.chai.omopv6.model.entity.Concept;
import edu.gatech.chai.omopv6.model.entity.ConceptRelationship;
import edu.gatech.chai.omopv6.model.entity.ConceptRelationshipPK;
import edu.gatech.chai.omopv6.model.entity.Relationship;
import edu.gatech.chai.omopv6.model.entity.SSession;
import edu.gatech.chai.omopv6.model.entity.SSessionLogs;
import edu.gatech.chai.omopv6.model.entity.Vocabulary;

@Component
public class ScheduledTask {
	private static final Logger logger = LoggerFactory.getLogger(ScheduledTask.class);
	private static final long CONCEPT_MY_SPACE = 2000000000L;
	private String smartPacerBasicAuth = "client:secret";
	private OmopServerOperations myMapper;

	protected static final String IN_QUERY   = "IN_QUERY";
	protected static final String COMPLETE   = "COMPLETE";
	protected static final String ERROR      = "ERROR";
	protected static final String REPEAT_IN_ = "REPEAT_IN_";

	@Autowired
	private ConceptService conceptService;
	@Autowired
	private ConceptRelationshipService conceptRelationshipService;
	@Autowired
	private VocabularyService vocabularyService;
	@Autowired
	private RelationshipService relationshipService;
	@Autowired
	private SSessionService ssessionService;
	@Autowired
	private SSessionLogsService ssessionLogsService;

	private Long conceptIdStart;

	private int done = 0;

	protected FhirOmopVocabularyMapImpl fhirOmopVocabularyMap;

	public ScheduledTask() {
		conceptIdStart = ScheduledTask.CONCEPT_MY_SPACE;
		fhirOmopVocabularyMap = new FhirOmopVocabularyMapImpl();
		setSmartPacerBasicAuth("client:secret");

		// We are using the server operations implementation. 
		WebApplicationContext myAppCtx = ContextLoaderListener.getCurrentWebApplicationContext();
		myMapper = new OmopServerOperations(myAppCtx);
	}

	public String getSmartPacerBasicAuth() {
		return this.smartPacerBasicAuth;
	}

	public void setSmartPacerBasicAuth(String smartPacerBasicAuth) {
		this.smartPacerBasicAuth = smartPacerBasicAuth;
	}

	@Scheduled(fixedDelay = 30000)
	public void runPeriodicQuery() {
		List<SSession> sessions = ssessionService.searchWithoutParams(0, 0, "id ASC");

		for (SSession session : sessions) {
			if (ScheduledTask.IN_QUERY.equals(session.getStatus())) {
				// If we are still in query status, call status URL to get FHIR syphilis registry data.
				// String statusURL = session.getStatusUrl() + "?jobid=" + session.getJodId();
				// RestTemplate restTemplate = new RestTemplate();
				// HttpEntity<String> reqAuth = new HttpEntity<String>(createHeaders());
				// ResponseEntity<String> response;
		
				// response = restTemplate.exchange(statusURL, HttpMethod.GET, reqAuth, String.class);
				// HttpStatus statusCode = response.getStatusCode();
				// if (!statusCode.is2xxSuccessful()) {
				// 	logger.debug("Status Query Failed and Responded with statusCode:" + statusCode.toString());
				// 	return;
				// }

				// Get response body
				//String responseBody = response.getBody();
				if (done == 0) {
					done = 1;
				} else {
					return;
				}

				logger.debug("I am writing to OMOP");
				String responseBody = "{\"resourceType\":\"Bundle\",\"id\":\"d269460a-d7fa-404c-a2bd-bdda0edd802e\",\"type\":\"collection\",\"total\":24,\"entry\":[{\"fullUrl\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\",\"resource\":{\"resourceType\":\"Patient\",\"id\":\"e8efe3f9-63e7-49ab-a8c3-5e24c9521993\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"}],\"name\":[{\"use\":\"official\",\"family\":\"Aberson\",\"given\":[\"Abe\"]}]}},{\"fullUrl\":\"Condition/a3e2d4ef-f160-4a72-876d-c714aa2ae136\",\"resource\":{\"resourceType\":\"Condition\",\"id\":\"a3e2d4ef-f160-4a72-876d-c714aa2ae136\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"a3e2d4ef-f160-4a72-876d-c714aa2ae136\"}],\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"resolved\",\"display\":\"Resolved\"}]},\"code\":{\"coding\":[{\"system\":\"http://snomed.info/sct\",\"code\":\"76272004\",\"display\":\"Syphilis (disorder)\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"onsetDateTime\":\"2021-02-01T00:00:00-05:00\"}},{\"fullUrl\":\"MedicationStatement/95710e94-be64-42a7-aa88-5647c54c0704\",\"resource\":{\"resourceType\":\"MedicationStatement\",\"id\":\"95710e94-be64-42a7-aa88-5647c54c0704\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"95710e94-be64-42a7-aa88-5647c54c0704\"}],\"medicationCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7984\",\"display\":\"penicillin V\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"effectiveDateTime\":\"2021-02-02T00:00:00-05:00\"}},{\"fullUrl\":\"MedicationStatement/3b33fb1b-4975-4ee1-a1d5-af5ab6b47fc7\",\"resource\":{\"resourceType\":\"MedicationStatement\",\"id\":\"3b33fb1b-4975-4ee1-a1d5-af5ab6b47fc7\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"3b33fb1b-4975-4ee1-a1d5-af5ab6b47fc7\"}],\"medicationCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7980\",\"display\":\"penicillin G\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"effectiveDateTime\":\"2021-02-03T00:00:00-05:00\"}},{\"fullUrl\":\"MedicationStatement/459fd8d6-1028-4591-b68f-6fa1bfd0f19c\",\"resource\":{\"resourceType\":\"MedicationStatement\",\"id\":\"459fd8d6-1028-4591-b68f-6fa1bfd0f19c\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"459fd8d6-1028-4591-b68f-6fa1bfd0f19c\"}],\"medicationCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"1665020\",\"display\":\"ceftriaxone 1000 MG\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"effectiveDateTime\":\"2021-02-03T00:00:00-05:00\"}},{\"fullUrl\":\"Condition/6f7b9b30-a978-483a-9c86-bb4f786aa346\",\"resource\":{\"resourceType\":\"Condition\",\"id\":\"6f7b9b30-a978-483a-9c86-bb4f786aa346\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"6f7b9b30-a978-483a-9c86-bb4f786aa346\"}],\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"resolved\",\"display\":\"Resolved\"}]},\"code\":{\"coding\":[{\"system\":\"http://snomed.info/sct\",\"code\":\"76272004\",\"display\":\"Syphilis (disorder)\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"onsetDateTime\":\"2021-04-03T00:00:00-04:00\"}},{\"fullUrl\":\"MedicationStatement/0e1d0d81-9b59-431b-9e04-fce15c85f27d\",\"resource\":{\"resourceType\":\"MedicationStatement\",\"id\":\"0e1d0d81-9b59-431b-9e04-fce15c85f27d\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"0e1d0d81-9b59-431b-9e04-fce15c85f27d\"}],\"medicationCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7984\",\"display\":\"penicillin V\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"effectiveDateTime\":\"2021-04-03T00:00:00-04:00\"}},{\"resource\":{\"resourceType\":\"MedicationStatement\",\"id\":\"e54904da-56f1-4e3b-9691-7a70f805f4a3\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"e54904da-56f1-4e3b-9691-7a70f805f4a3\"}],\"medicationCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7980\",\"display\":\"penicillin G\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"effectiveDateTime\":\"2021-04-04T00:00:00-04:00\"}},{\"fullUrl\":\"MedicationStatement/866dd23f-e088-4aea-a3eb-5cb036ec56c0\",\"resource\":{\"resourceType\":\"MedicationStatement\",\"id\":\"866dd23f-e088-4aea-a3eb-5cb036ec56c0\",\"identifier\":[{\"system\":\"https://apps.hdap.gatech.edu/omoponfhir3/\",\"value\":\"e54904da-56f1-4e3b-9691-7a70f805f4a3\"}],\"medicationCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"1665020\",\"display\":\"ceftriaxone 1000 MG\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"effectiveDateTime\":\"2021-04-04T00:00:00-04:00\"}},{\"fullUrl\":\"Observation/eb4dc994-0a3b-4725-9679-adc24865fadb\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"eb4dc994-0a3b-4725-9679-adc24865fadb\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000001\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/95710e94-be64-42a7-aa88-5647c54c0704\"}],\"valueCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7980\",\"display\":\"penicillin G\"}]},\"note\":[{\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^7984\"}]}},{\"resource\":{\"resourceType\":\"Observation\",\"id\":\"0a059863-a6d2-4008-a66a-75925d6308a8\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000002\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/95710e94-be64-42a7-aa88-5647c54c0704\"}],\"valueQuantity\":{\"value\":10,\"unit\":\"mg\"},\"note\":[{\"text\":\"10 mg\"}]}},{\"fullUrl\":\"Observation/b9e2728d-543c-490c-b191-a45521782e39\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"b9e2728d-543c-490c-b191-a45521782e39\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000003\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/95710e94-be64-42a7-aa88-5647c54c0704\"}],\"valueDateTime\":\"2021-02-02T00:00:00-05:00\",\"note\":[{\"text\":\"01-02-2021\"}]}},{\"fullUrl\":\"Observation/c652149b-8cca-472d-8290-c468778d81cf\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"c652149b-8cca-472d-8290-c468778d81cf\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000002\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/3b33fb1b-4975-4ee1-a1d5-af5ab6b47fc7\"}],\"valueCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7980\",\"display\":\"penicillin G\"}],\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^7980\"},\"note\":[{\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^7980\"}]}},{\"fullUrl\":\"Observation/849847bf-defa-44e4-aa14-cf7187eecc13\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"849847bf-defa-44e4-aa14-cf7187eecc13\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000002\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/3b33fb1b-4975-4ee1-a1d5-af5ab6b47fc7\"}],\"valueQuantity\":{\"value\":20,\"unit\":\"mg\"},\"note\":[{\"text\":\"20 mg\"}]}},{\"fullUrl\":\"Observation/e47d72b1-a565-4d67-81ca-1f01e630827a\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"e47d72b1-a565-4d67-81ca-1f01e630827a\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000003\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/3b33fb1b-4975-4ee1-a1d5-af5ab6b47fc7\"}],\"valueDateTime\":\"2021-02-03T00:00:00-05:00\",\"note\":[{\"text\":\"01-03-2021\"}]}},{\"fullUrl\":\"Observation/76958aa2-927e-4ddd-a193-ab7c62048518\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"76958aa2-927e-4ddd-a193-ab7c62048518\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000001\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/0e1d0d81-9b59-431b-9e04-fce15c85f27d\"}],\"valueCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"7980\",\"display\":\"penicillin G\"}],\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^7984\"},\"note\":[{\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^7980\"}]}},{\"fullUrl\":\"Observation/3664dd9c-f06d-4bef-9f7e-d876cec364b6\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"3664dd9c-f06d-4bef-9f7e-d876cec364b6\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000002\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/0e1d0d81-9b59-431b-9e04-fce15c85f27d\"}],\"valueQuantity\":{\"value\":10,\"unit\":\"mg\"},\"note\":[{\"text\":\"10 mg\"}]}},{\"fullUrl\":\"Observation/3eb3ef4b-7b24-4aec-9bcd-b23960d0936f\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"3eb3ef4b-7b24-4aec-9bcd-b23960d0936f\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000003\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/0e1d0d81-9b59-431b-9e04-fce15c85f27d\"}],\"valueDateTime\":\"2021-04-03T00:00:00-04:00\",\"note\":[{\"text\":\"03-03-2021\"}]}},{\"fullUrl\":\"Observation/6fe38b23-9332-400a-9352-211c63946cc4\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"6fe38b23-9332-400a-9352-211c63946cc4\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000001\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/459fd8d6-1028-4591-b68f-6fa1bfd0f19c\"}],\"valueCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"1665020\",\"display\":\"ceftriaxone 1000 MG\"}],\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^16650121\"},\"note\":[{\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^46287327\"}]}},{\"fullUrl\":\"Observation/58ed2513-530d-4df5-924f-262b5b2625cf\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"58ed2513-530d-4df5-924f-262b5b2625cf\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000002\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/459fd8d6-1028-4591-b68f-6fa1bfd0f19c\"}],\"valueQuantity\":{\"value\":100,\"unit\":\"mg\"},\"note\":[{\"text\":\"100 mg\"}]}},{\"fullUrl\":\"Observation/e6d52f69-ec06-4a12-8521-d04806d0620c\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"e6d52f69-ec06-4a12-8521-d04806d0620c\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000003\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/459fd8d6-1028-4591-b68f-6fa1bfd0f19c\"}],\"valueDateTime\":\"2021-02-03T00:00:00-05:00\",\"note\":[{\"text\":\"01-03-2021\"},{\"text\":\"03-04-2021\"}]}},{\"fullUrl\":\"Observation/a7ff0d1b-cb25-4908-816b-e30d938d1717\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"a7ff0d1b-cb25-4908-816b-e30d938d1717\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000001\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/866dd23f-e088-4aea-a3eb-5cb036ec56c0\"}],\"valueCodeableConcept\":{\"coding\":[{\"system\":\"http://www.nlm.nih.gov/research/umls/rxnorm\",\"code\":\"1665020\",\"display\":\"ceftriaxone 1000 MG\"}],\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^16650121\"},\"note\":[{\"text\":\"http://www.nlm.nih.gov/research/umls/rxnorm^46287327\"}]}},{\"fullUrl\":\"Observation/8e64160a-416a-44b0-a043-07e885601753\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"8e64160a-416a-44b0-a043-07e885601753\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000002\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/866dd23f-e088-4aea-a3eb-5cb036ec56c0\"}],\"valueQuantity\":{\"value\":100,\"unit\":\"mg\"},\"note\":[{\"text\":\"100 mg\"}]}},{\"fullUrl\":\"Observation/8030175d-c339-4ab7-ac61-53263db44ed6\",\"resource\":{\"resourceType\":\"Observation\",\"id\":\"8030175d-c339-4ab7-ac61-53263db44ed6\",\"category\":[{\"coding\":[{\"system\":\"http://hl7.org/fhir/ValueSet/observation-category\",\"code\":\"survey\",\"display\":\"Survey\"}]}],\"code\":{\"coding\":[{\"system\":\"urn:gtri:heat:syphilisregistry\",\"code\":\"2000000003\"}]},\"subject\":{\"reference\":\"Patient/e8efe3f9-63e7-49ab-a8c3-5e24c9521993\"},\"focus\":[{\"reference\":\"MedicationStatement/866dd23f-e088-4aea-a3eb-5cb036ec56c0\"}],\"valueDateTime\":\"2021-04-04T00:00:00-04:00\"}}]}";
				if (responseBody != null && !responseBody.isEmpty()) {
					// Convert the response body to FHIR Resource
					IParser parser = StaticValues.myFhirContext.newJsonParser();
					Bundle responseBundle = parser.parseResource(Bundle.class, responseBody);

					// Save the response bundle to registry db.
					if (responseBundle != null && !responseBundle.isEmpty()) {
						List<BundleEntryComponent> entries = responseBundle.getEntry();
						List<BundleEntryComponent> responseEntries = myMapper.createEntries(entries, session);
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
							SSessionLogs ssessionLogs = new SSessionLogs();
							ssessionLogs.setSession(session);
							ssessionLogs.setText(errMessage);
							ssessionLogs.setDateTime(new Date());
							ssessionLogsService.create(ssessionLogs);
						}
					}
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
						System.out.println(omopSrc[0]+"\n"+omopSrc[1]+"\n\n\n\n\n\n\n\n\n\n\n");
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
		byte[] encodedAuth = Base64.encodeBase64(smartPacerBasicAuth.getBytes(Charset.forName("US-ASCII")));
		String authHeader = "Basic " + new String(encodedAuth);
		httpHeaders.set("Authorization", authHeader);
		httpHeaders.setAccept(Arrays.asList(new MediaType("application/fhir+json")));

		return httpHeaders;
	}

	private Long getTheLargestConceptId() {
		Long largestId = conceptService.getLargestId();
		if (largestId != null) {
			if (largestId >= conceptIdStart) {
				conceptIdStart = largestId + 1L;
			}
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
		if (concepts.size() > 0) {
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
		if (concepts.size() > 0) {
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
