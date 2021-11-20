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
package edu.gatech.chai.omopv5.model.entity;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.gatech.chai.omopv5.model.entity.custom.Column;
import edu.gatech.chai.omopv5.model.entity.custom.Id;
import edu.gatech.chai.omopv5.model.entity.custom.JoinColumn;
import edu.gatech.chai.omopv5.model.entity.custom.Table;

/** 
 * This class maintains case information for Syphilis registry.
 * @author Myung Choi
 */
@Table(name="case_info")
public class FlagInfo extends BaseEntity {
	private static final Logger logger = LoggerFactory.getLogger(FlagInfo.class);

	@Id
	@Column(name = "flag_info_id")
	private Long id;

	@Column(name="case_info_id")
	private CaseInfo caseInfo;

	@JoinColumn(name = "person_id", table="f_person:fPerson,person:person", nullable = false)
	private FPerson fPerson;
	
	@Column(name="domain")
	private String domain;
	
	@Column(name="flag_type")
	private String flagType;
	
	@Column(name="last_updated")
	private Date lastUpdated;

	@Column(name="annotation")
	private String annotation;
	
	public FlagInfo() {
		super();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public CaseInfo getCaseInfo() {
		return caseInfo;
	}

	public void setCaseInfo(CaseInfo caseInfo) {
		this.caseInfo = caseInfo;
	}

	public FPerson getFPerson() {
		return fPerson;
	}

	public void setFPerson(FPerson fPerson) {
		this.fPerson = fPerson;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getFlagType() {
		return flagType;
	}

	public void setFlagType(String flagType) {
		this.flagType = flagType;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getAnnotation() {
		return annotation;
	}

	public void setAnnotation(String annotation) {
		this.annotation = annotation;
	}

	@Override
	public String getColumnName(String columnVariable) {
		return FlagInfo._getColumnName(columnVariable);
	}

    public static String _getColumnName(String columnVariable) {

		try {
			Field field = FlagInfo.class.getDeclaredField(columnVariable);
			if (field != null) {
				Column annotation = field.getDeclaredAnnotation(Column.class);
				if (annotation != null) {
					return FlagInfo._getTableName() + "." + annotation.name();
				} else {
					JoinColumn joinAnnotation = field.getDeclaredAnnotation(JoinColumn.class);
					if (joinAnnotation != null) {
						return FlagInfo._getTableName() + "." + joinAnnotation.name();
					}

					logger.error("annotation is null for field=" + field.toString());
					return null;
				}
			}
		} catch (NoSuchFieldException | SecurityException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
    public String getTableName() {
		return FlagInfo._getTableName();
    }

    public static String _getTableName() {
		Table annotation = FlagInfo.class.getDeclaredAnnotation(Table.class);
		if (annotation != null) {
			return annotation.name();
		}
		return "flag_info";
	}

    @Override
    public String getForeignTableName(String foreignVariable) {
		return FlagInfo._getForeignTableName(foreignVariable);
    }

	public static String _getForeignTableName(String foreignVariable) {
		if ("fPerson".equals(foreignVariable))
			return FPerson._getTableName();

		return null;
	}
    
    @Override
    public String getSqlSelectTableStatement(List<String> parameterList, List<String> valueList) {
		return FlagInfo._getSqlTableStatement(parameterList, valueList);
    }

    public static String _getSqlTableStatement(List<String> parameterList, List<String> valueList) {
		return "select * from flag_info ";
	}
}
