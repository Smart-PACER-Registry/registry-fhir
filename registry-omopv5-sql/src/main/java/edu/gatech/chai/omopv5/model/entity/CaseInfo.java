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
public class CaseInfo extends BaseEntity {
	private static final Logger logger = LoggerFactory.getLogger(CaseInfo.class);

	@Id
	@Column(name = "case_info_id", nullable = false)
	private Long id;

	@Column(name="patient_identifier", nullable = false)
	private String patientIdentifier;

	@JoinColumn(name = "person_id", table="f_person:fPerson,person:person", nullable = false)
	private FPerson fPerson;
	
	@Column(name="job_id")
	private String jobId;
	
	@Column(name="status", nullable = false)
	private String status;

	@Column(name="server_host", nullable = false)
	private String serverHost;
	
	@Column(name="status_url")
	private String statusUrl;
	
	@Column(name="server_url", nullable = false)
	private String serverUrl;

	@Column(name="trigger_at")
	private Date triggerAt;

	@Column(name="last_updated")
	private Date lastUpdated;

	@Column(name="activated")
	private Date activated;
	
	@Column(name="created", nullable = false)
	private Date created;
	
	public CaseInfo() {
		super();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getPatientIdentifier() {
		return patientIdentifier;
	}

	public void setPatientIdentifier(String patientIdentifier) {
		this.patientIdentifier = patientIdentifier;
	}

	public FPerson getFPerson() {
		return fPerson;
	}

	public void setFPerson(FPerson fPerson) {
		this.fPerson = fPerson;
	}

	public String getJodId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getServerHost() {
		return serverHost;
	}

	public void setServerHost(String serverHost) {
		this.serverHost = serverHost;
	}

	public String getStatusUrl() {
		return statusUrl;
	}

	public void setStatusUrl(String statusUrl) {
		this.statusUrl = statusUrl;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public Date getTriggerAt() {
		return triggerAt;
	}

	public void setTriggerAt(Date triggerAt) {
		this.triggerAt = triggerAt;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public Date getActivated() {
		return activated;
	}

	public void setActivated(Date activated) {
		this.activated = activated;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	@Override
	public String getColumnName(String columnVariable) {
		return CaseInfo._getColumnName(columnVariable);
	}

    public static String _getColumnName(String columnVariable) {

		try {
			Field field = CaseInfo.class.getDeclaredField(columnVariable);
			if (field != null) {
				Column annotation = field.getDeclaredAnnotation(Column.class);
				if (annotation != null) {
					return CaseInfo._getTableName() + "." + annotation.name();
				} else {
					JoinColumn joinAnnotation = field.getDeclaredAnnotation(JoinColumn.class);
					if (joinAnnotation != null) {
						return CaseInfo._getTableName() + "." + joinAnnotation.name();
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
		return CaseInfo._getTableName();
    }

    public static String _getTableName() {
		Table annotation = CaseInfo.class.getDeclaredAnnotation(Table.class);
		if (annotation != null) {
			return annotation.name();
		}
		return "case_info";
	}

    @Override
    public String getForeignTableName(String foreignVariable) {
		return CaseInfo._getForeignTableName(foreignVariable);
    }

	public static String _getForeignTableName(String foreignVariable) {
		if ("fPerson".equals(foreignVariable))
			return FPerson._getTableName();

		return null;
	}
    
    @Override
    public String getSqlSelectTableStatement(List<String> parameterList, List<String> valueList) {
		return CaseInfo._getSqlTableStatement(parameterList, valueList);
    }

    public static String _getSqlTableStatement(List<String> parameterList, List<String> valueList) {
		return "select * from case_info ";
	}
}
