/*******************************************************************************
 * Copyright (c) 2019 Georgia Tech Research Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package edu.gatech.chai.omoponfhir.omopv5.r4.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.MessageHeader.MessageHeaderResponseComponent;
import org.hl7.fhir.r4.model.MessageHeader.ResponseType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.exceptions.FHIRException;

import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenParam;
import edu.gatech.chai.omoponfhir.omopv5.r4.mapping.OmopServerOperations;
import edu.gatech.chai.omoponfhir.omopv5.r4.utilities.CodeableConceptUtil;
import edu.gatech.chai.omoponfhir.omopv5.r4.utilities.StaticValues;
import edu.gatech.chai.omoponfhir.omopv5.r4.utilities.ThrowFHIRExceptions;
import edu.gatech.chai.omopv5.dba.service.CaseInfoService;
import edu.gatech.chai.omopv5.dba.service.ParameterWrapper;
import edu.gatech.chai.omopv5.model.entity.CaseInfo;
import edu.gatech.chai.omopv5.model.entity.FPerson;

public class ServerOperations {
	private static final Logger logger = LoggerFactory.getLogger(ServerOperations.class);
	private OmopServerOperations myMapper;
	
	private CaseInfoService caseInfoService;

	public ServerOperations() {
		WebApplicationContext myAppCtx = ContextLoaderListener.getCurrentWebApplicationContext();
		myMapper = new OmopServerOperations(myAppCtx);
		caseInfoService = myAppCtx.getBean(CaseInfoService.class);
		// caseInfoService = new CaseInfoServiceImp();
	}
	
	@Operation(name="$process-message")
	public Bundle processMessageOperation(
			@OperationParam(name="content") Bundle theContent,
			@OperationParam(name="async") BooleanType theAsync,
			@OperationParam(name="response-url") UriType theUri			
			) {
		Bundle retVal = new Bundle();
		MessageHeader messageHeader = null;
//		List<BundleEntryComponent> resources = new ArrayList<BundleEntryComponent>();
		List<BundleEntryComponent> entries = theContent.getEntry();
		
		if (theContent.getType() == BundleType.MESSAGE) {
			// Evaluate the first entry, which must be MessageHeader
//			BundleEntryComponent entry1 = theContent.getEntryFirstRep();
//			Resource resource = entry1.getResource();
			if (entries != null && !entries.isEmpty() && 
					entries.get(0).getResource() != null &&
					entries.get(0).getResource().getResourceType() == ResourceType.MessageHeader) {
				messageHeader = (MessageHeader) entries.get(0).getResource();
				// We handle observation-type.
				// TODO: Add other types later.
				Coding event = messageHeader.getEventCoding();
//				Coding obsprovided = new Coding("http://hl7.org/fhir/message-events", "observation-provide", "Provide a simple observation or update a previously provided simple observation.");
				Coding obsprovided = new Coding("http://terminology.hl7.org/CodeSystem/observation-category", "laboratory", "Laboratory");
				if (CodeableConceptUtil.compareCodings(event, obsprovided) != 0) {
					ThrowFHIRExceptions.unprocessableEntityException(
							"We currently support only observation-provided Message event");
				}
			}
		} else {
			ThrowFHIRExceptions.unprocessableEntityException(
					"The bundle must be a MESSAGE type");
		}
		MessageHeaderResponseComponent messageHeaderResponse = new MessageHeaderResponseComponent();
		messageHeaderResponse.setId(messageHeader.getId());

		List<BundleEntryComponent> resultEntries = null;
		try {
			resultEntries = myMapper.createEntries(entries);
			messageHeaderResponse.setCode(ResponseType.OK);
		} catch (FHIRException e) {
			e.printStackTrace();
			messageHeaderResponse.setCode(ResponseType.OK);
			OperationOutcome outcome = new OperationOutcome();
			CodeableConcept detailCode = new CodeableConcept();
			detailCode.setText(e.getMessage());
			outcome.addIssue().setSeverity(IssueSeverity.ERROR).setDetails(detailCode);
			messageHeaderResponse.setDetailsTarget(outcome);
		}
		
		messageHeader.setResponse(messageHeaderResponse);
		BundleEntryComponent responseMessageEntry = new BundleEntryComponent();
		UUID uuid = UUID.randomUUID();
		responseMessageEntry.setFullUrl("urn:uuid:"+uuid.toString());
		responseMessageEntry.setResource(messageHeader);
		
		if (resultEntries == null) resultEntries = new ArrayList<BundleEntryComponent>();
		
		resultEntries.add(0, responseMessageEntry);
		retVal.setEntry(resultEntries);
		
		return retVal;
	}

	@Operation(name="$registry-control")
	public Bundle registryControlOperation(RequestDetails theRequestDetails,
		@OperationParam(name = "case-id") StringParam theCaseId,
		@OperationParam(name = "patient-identifier") TokenParam thePatientIdentifier,
		@OperationParam(name = "set-status") StringParam theSetStatus,
		@OperationParam(name = "lab-results") Bundle theLabResults) {

		Bundle returnBundle = new Bundle();

		// Set parameterwrapper for the caseId if available
		List<ParameterWrapper> paramList = new ArrayList<ParameterWrapper>();
		ParameterWrapper parameterWrapper = new ParameterWrapper();
		if (theCaseId != null) {
			parameterWrapper.setParameterType("String");
			parameterWrapper.setParameters(Arrays.asList("id"));
			parameterWrapper.setOperators(Arrays.asList("="));
			parameterWrapper.setValues(Arrays.asList(theCaseId.getValue()));
			parameterWrapper.setRelationship("or");
			paramList.add(parameterWrapper);
		}

		// get the value of set-status parameter.
		if (theSetStatus != null) {
			String newStatus = theSetStatus.getValue();
			if (StaticValues.REQUEST.equals(newStatus)) {
				// Set the status to REQUEST.
				if (theCaseId != null) {
					List<CaseInfo> caseInfos = caseInfoService.searchWithParams(0, 0, paramList, "id ASC");
					for (CaseInfo caseInfo : caseInfos) {
						caseInfo.setStatus(StaticValues.REQUEST);
						caseInfoService.update(caseInfo);
					}
				} else {
					// This is a new REQUEST. 
					// Get patient identifier if available.
					String patientIdentifier = "";
					if (thePatientIdentifier == null || thePatientIdentifier.isEmpty()) {
						ThrowFHIRExceptions.unprocessableEntityException("Patient Identifier is required to create a new REQUEST");
					} else {
						patientIdentifier = thePatientIdentifier.getValue();
					}

					if (theLabResults == null || theLabResults.isEmpty()) {
						ThrowFHIRExceptions.unprocessableEntityException("Lab Results with a patient are required to create a new REQUEST");
					}
										
					// We have a lab. Create these results in the
					// OMOP database.
					List<BundleEntryComponent> responseEntries = myMapper.createEntries(theLabResults.getEntry());
					int errorFlag = 0;
					String errMessage = "";
					FPerson fPerson = null;
					for (BundleEntryComponent responseEntry : responseEntries) {
						Resource resource = responseEntry.getResource();
						if (resource instanceof Patient) {
							fPerson = new FPerson();
							fPerson.setId(((Patient) resource).getIdElement().getIdPartAsLong());
						}

						if (!responseEntry.getResponse().getStatus().startsWith("201") 
							&& !responseEntry.getResponse().getStatus().startsWith("200")) {
							String jsonResource = StaticValues.serializeIt(resource);
							errMessage += "Failed to create/add " + jsonResource;
							logger.error(errMessage);
							errorFlag = 1;
						}
					}

					if (errorFlag == 1 || fPerson == null) {
						// Error occurred on one of resources.
						if (fPerson == null) {
							errMessage += " Patient resource is REQUIRED";
						}
						ThrowFHIRExceptions.unprocessableEntityException("Failed to create entiry resources: " + errMessage);
					}

					CaseInfo caseInfo = new CaseInfo();
					caseInfo.setPatientIdentifier(patientIdentifier);
					caseInfo.setFPerson(fPerson);
					caseInfo.setStatus(StaticValues.REQUEST);
					caseInfo.setServerHost("https://gt-apps.hdap.gatech.edu/rc-api");
					caseInfo.setServerUrl("/forms/start?asyncFlag=true");
					caseInfo.setCreated(new Date());
					caseInfoService.create(caseInfo);
				}
			}
		}

		return returnBundle;
	}

}
