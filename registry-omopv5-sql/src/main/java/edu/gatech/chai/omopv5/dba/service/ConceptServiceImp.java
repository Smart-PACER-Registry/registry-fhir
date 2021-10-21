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
 *
 *******************************************************************************/
package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

import edu.gatech.chai.omopv5.model.entity.Concept;
import edu.gatech.chai.omopv5.model.entity.IBaseEntity;

// TODO: Auto-generated Javadoc
/**
 * The Class ConceptServiceImp.
 */
@Service
public class ConceptServiceImp extends BaseEntityServiceImp<Concept> implements ConceptService {
	private static final Logger logger = LoggerFactory.getLogger(Concept.class);

	/**
	 * Instantiates a new concept service imp.
	 */
	public ConceptServiceImp() {
		super(Concept.class);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.gatech.chai.omopv5.dba.service.ConceptService#getIngredient(edu.gatech.
	 * chai.omopv5.model.entity.Concept)
	 */
	public List<Concept> getIngredient(Concept concept) {
		List<Concept> concepts = new ArrayList<Concept>();

		if ("Ingredient".equals(concept.getConceptClassId())) {
			// This is ingredient. Just return empty list
			return concepts;
		}

		List<String> parameterList = new ArrayList<String>();
		List<String> valueList = new ArrayList<String>();

		String sql = null;
		if ("NDC".equals(concept.getVocabularyId())) {
			// Use JPQL
			sql = "select concept.concept_id as concept_concept_id, concept.concept_name as concept_concept_name, concept.domain_id as concdpt_domain_id, " 
				+ "concept.vocabulary_id as concept_vocabulary_id, concept.concept_class_id as concept_concept_class_id, "
				+ "concept.standard_concept as concept_standard_concept, concept.concept_code as concept_concept_code, "
				+ "concept.valid_start_date as concept_valid_start_date, concept.valid_end_date as concept_valid_end_date, "
				+ "concept.invalid_reason as concept_invalid_reason "
				+ "FROM concept concept " + "JOIN concept_relationship cr on concept.concept_id = cr.concept_id_1 "
				+ "AND cr.relationship_id = 'Maps to' " + "AND cr.invalid_reason is null "
				+ "JOIN concept tar on cr.concept_id_2 = tar.id " + "AND tar.standard_concept = 'S' "
				+ "AND tar.invalid_reason is null " + "JOIN concept_ancestor ca ON ca.ancestor_concept_id = tar.concept_id "
				+ "JOIN concept c ON ca.ancestor_concept_id = c.concept_id " + "WHERE concept.concept_code = @med_code "
				+ "AND 'NDC' = concpet.vocabulary_id " + "AND c.vocabulary_id = 'RxNorm' "
				+ "AND c.concept_class_id = 'Ingredient' " + "AND concept.invalid_reason is null";
		} else if ("RxNorm".equals(concept.getVocabularyId())) {
			// when RxNorm.
			sql = "select concept.concept_id as concept_concept_id, concept.concept_name as concept_concept_name, concept.domain_id as concdpt_domain_id, " 
				+ "concept.vocabulary_id as concept_vocabulary_id, concept.concept_class_id as concept_concept_class_id, "
				+ "concept.standard_concept as concept_standard_concept, concept.concept_code as concept_concept_code, "
				+ "concept.valid_start_date as concept_valid_start_date, concept.valid_end_date as concept_valid_end_date, "
				+ "concept.invalid_reason as concept_invalid_reason "
				+ "FROM concept concept " + "JOIN concept_ancestor ca ON ca.descendant_concept = concept.concept_id "
				+ "JOIN concept c ON ca.ancestor_concept = c.concept_id " + "WHERE concept.concept_code = @med_code "
				+ "AND 'RxNorm' = concept.vocabulary_id " + "AND c.vocabulary_id = 'RxNorm' "
				+ "AND c.concept_class_id = 'Ingredient' " + "AND concept.invalid_reason is null "
				+ "AND c.invalid_reason is null";
		} else {
			return concepts;
		}

		parameterList.add("med_code");
		valueList.add(concept.getConceptCode());

		sql = renderedSql(sql, parameterList, valueList);

		logger.debug("getIngredient:" + sql);

		Concept entity;
		try {
			if (isBigQuery()) {
				TableResult result = runBigQuery(sql);
				List<String> columns = listOfColumns(sql);
				for (FieldValueList row : result.iterateAll()) {
					entity = construct(row, null, getSqlTableName(), columns);
					if (entity != null) {
						break;
					}
				}
			} else {
				List<Concept> myEntities = runQuery(sql, null, "concept");
				if (!myEntities.isEmpty()) {
					concepts.addAll(myEntities);					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return concepts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.gatech.chai.omopv5.dba.service.ConceptService#getLargestId()
	 */
	@Override
	public Long getLargestId() {
		return null;
	}

	@Override
	public Concept update(Concept entity) {
		return null;
	}

	@Override
	public Concept construct(ResultSet rs, Concept entity, String alias) {
		return ConceptService._construct(rs, entity, alias);
	}

	@Override
	public Concept construct(FieldValueList rowResult, Concept entity, String alias, List<String> columns) {
		return ConceptService._construct(rowResult, entity, alias, columns);
	}

}
