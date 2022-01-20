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
package edu.gatech.chai.omoponfhir.omopv5.r4.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.MedicationStatement;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryResponseComponent;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;

import edu.gatech.chai.omoponfhir.omopv5.r4.model.USCorePatient;
import edu.gatech.chai.omoponfhir.omopv5.r4.provider.ConditionResourceProvider;
import edu.gatech.chai.omoponfhir.omopv5.r4.provider.MedicationStatementResourceProvider;
import edu.gatech.chai.omoponfhir.omopv5.r4.provider.ObservationResourceProvider;
import edu.gatech.chai.omoponfhir.omopv5.r4.provider.PatientResourceProvider;
import edu.gatech.chai.omoponfhir.omopv5.r4.utilities.ExtensionUtil;
import edu.gatech.chai.omopv5.model.entity.CaseInfo;

public class OmopServerOperations {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OmopServerOperations.class);

	private static OmopServerOperations omopServerOperations = new OmopServerOperations();

	private Map<String, String> referenceIds;

	public OmopServerOperations(WebApplicationContext context) {
		initialize(context);
	}

	public OmopServerOperations() {
		initialize(ContextLoaderListener.getCurrentWebApplicationContext());
	}

	private void initialize(WebApplicationContext context) {
		referenceIds = new HashMap<String, String>();
	}

	public static OmopServerOperations getInstance() {
		return omopServerOperations;
	}

	private void updateReference(Reference reference) {
		if (reference == null || reference.isEmpty())
			return;

		String originalId = reference.getReferenceElement().getValueAsString();
		String newId = referenceIds.get(originalId);

		logger.debug("orginal id: " + originalId + " new id:" + newId);
		if (newId != null && !newId.isEmpty()) {
			String[] resourceId = newId.split("/");
			if (resourceId.length == 2) {
				reference.setReferenceElement(new IdType(resourceId[0], resourceId[1]));
			} else {
				reference.setReferenceElement(new IdType(newId));
			}
		}
	}

	private void updateReferences(List<Reference> references) {
		for (Reference reference : references) {
			updateReference(reference);
		}
	}

	private BundleEntryComponent addResponseEntry(String status, String location) {
		BundleEntryComponent entryBundle = new BundleEntryComponent();
		UUID uuid = UUID.randomUUID();
		entryBundle.setFullUrl("urn:uuid:" + uuid.toString());
		BundleEntryResponseComponent responseBundle = new BundleEntryResponseComponent();
		responseBundle.setStatus(status);
		if (location != null)
			responseBundle.setLocation(location);
		entryBundle.setResponse(responseBundle);

		return entryBundle;
	}

	public List<BundleEntryComponent> createEntries(List<BundleEntryComponent> entries) throws FHIRException {
		return createEntries(entries, null);
	}

	public List<BundleEntryComponent> createEntries(List<BundleEntryComponent> entries, CaseInfo caseInfo) throws FHIRException {
		List<BundleEntryComponent> responseEntries = new ArrayList<BundleEntryComponent>();
		// Map<String, Long> patientMap = new HashMap<String, Long>();

		// do patient first.
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();
			if (resource.getResourceType() == ResourceType.Patient) {
				Long fhirId;
				BundleEntryComponent newEntry;
				USCorePatient patient = ExtensionUtil.usCorePatientFromResource(resource);
				if (caseInfo == null) {
					fhirId = OmopPatient.getInstance().toDbase(patient, null);
					newEntry = addResponseEntry("201 Created", "Patient/" + fhirId);
				} else {
					IdType fhirIdtype = new IdType(PatientResourceProvider.getType(), caseInfo.getFPerson().getId());

					// We have session ID. We should have Person already in the OMOP. So, we update it.
					fhirId = OmopPatient.getInstance().toDbase(patient, fhirIdtype);
					newEntry = addResponseEntry("200 OK", PatientResourceProvider.getType() + "/" + fhirId);
				}

				patient.setId("Patient/" + fhirId);
				newEntry.setResource(patient);
				responseEntries.add(newEntry);

				referenceIds.put(entry.getFullUrl(), PatientResourceProvider.getType() + "/" + fhirId);
				logger.debug("Added patient info to referenceIds " + entry.getFullUrl() + "->" + fhirId);
			}
		}

		// In the bundle, we need to process medications, conditions, etc first before observation as
		// our observation in the bundle will have observation.focus to those resources. We will first
		// store meds and conds. Then, we will update the focus reference with the new values.
		//
		// Process the MedicationStatement
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();
			
			if (resource.getResourceType() == ResourceType.MedicationStatement) {
				logger.debug("Trying to add medication statement: " + entry.getFullUrl());
				MedicationStatement medicationStatement = (MedicationStatement) resource;
				updateReference(medicationStatement.getSubject());

				Long fhirId = OmopMedicationStatement.getInstance().toDbase(medicationStatement, null);
				BundleEntryComponent newEntry;
				if (fhirId == null || fhirId == 0L) {
					newEntry = addResponseEntry("400 Bad Request", null);
					newEntry.setResource(medicationStatement);
				} else {
					referenceIds.put(entry.getFullUrl(), MedicationStatementResourceProvider.getType() + "/" + fhirId);
					newEntry = addResponseEntry("201 Created", "MedicationStatement/" + fhirId);
				}

				responseEntries.add(newEntry);
				logger.debug("Added medication statement info to referenceIds " + entry.getFullUrl() + "->" + fhirId);
			}
		}

		// Process Condition
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();

			if (resource.getResourceType() == ResourceType.Condition) {
				Condition condition = (Condition) resource;
				updateReference(condition.getSubject());

				Long fhirId = OmopCondition.getInstance().toDbase(condition, null);
				BundleEntryComponent newEntry;
				if (fhirId == null || fhirId == 0L) {
					newEntry = addResponseEntry("400 Bad Request", null);
					newEntry.setResource(condition);
				} else {
					referenceIds.put(entry.getFullUrl(), ConditionResourceProvider.getType() + "/" + fhirId);
					newEntry = addResponseEntry("201 Created", "Condition/" + fhirId);
				}

				responseEntries.add(newEntry);
			}
		}

		// Process Observation
		for (BundleEntryComponent entry : entries) {
			Resource resource = entry.getResource();
			
			if (resource.getResourceType() == ResourceType.Observation) {
				Observation observation = (Observation) resource;
				if (observation.getSubject().isEmpty() && caseInfo != null) {
					observation.getSubject().setReferenceElement(new IdType("Patient", caseInfo.getFPerson().getId()));
				} else {
					updateReference(observation.getSubject());
				}
				updateReferences(observation.getFocus());

				Long fhirId = OmopObservation.getInstance().toDbase(observation, null);
				BundleEntryComponent newEntry;
				if (fhirId == null || fhirId == 0L) {
					newEntry = addResponseEntry("400 Bad Request", null);
					newEntry.setResource(observation);
				} else {
					referenceIds.put(entry.getFullUrl(), ObservationResourceProvider.getType() + "/" + fhirId);
					newEntry = addResponseEntry("201 Created", "Observation/" + fhirId);
				}
				responseEntries.add(newEntry);
				logger.debug("Added observation info to referenceIds " + entry.getFullUrl() + "->" + fhirId);
			} 
		}

		return responseEntries;
	}
}
