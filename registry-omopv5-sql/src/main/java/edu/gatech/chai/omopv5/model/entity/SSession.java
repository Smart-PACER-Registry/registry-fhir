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

import edu.gatech.chai.omopv5.model.entity.custom.Column;
import edu.gatech.chai.omopv5.model.entity.custom.GeneratedValue;
import edu.gatech.chai.omopv5.model.entity.custom.GenerationType;
import edu.gatech.chai.omopv5.model.entity.custom.Id;
import edu.gatech.chai.omopv5.model.entity.custom.JoinColumn;
import edu.gatech.chai.omopv5.model.entity.custom.Table;

/** 
 * This class maintains session information for Syphilis registry.
 * @author Myung Choi
 */
@Table(name="s_session")
public class SSession extends BaseEntity {
	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ssession_seq_gen")
	@Column(name = "session_id")
	private Long id;

	@JoinColumn(name = "person_id", table="f_person:fPerson,person:person", nullable = false)
	private FPerson fPerson;
	
	@Column(name="job_id")
	private Long jobId;
	
	@Column(name="status")
	private String status;
	
	@Column(name="status_url")
	private String statusUrl;
	
	@Column(name="server_url")
	private String serverUrl;

	@Column(name="patient_identifier")
	private String patientIdentifier;

	@Column(name="last_updated")
	private Date lastUpdated;
	
	public SSession() {
		super();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public FPerson getFPerson() {
		return fPerson;
	}

	public void setFPerson(FPerson fPerson) {
		this.fPerson = fPerson;
	}

	public Long getJodId() {
		return jobId;
	}

	public void setJobId(Long jobId) {
		this.jobId = jobId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
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

	public String getPatientIdentifier() {
		return patientIdentifier;
	}

	public void setPatientIdentifier(String patientIdentifier) {
		this.patientIdentifier = patientIdentifier;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	@Override
	public String getColumnName(String columnVariable) {
		return SSession._getColumnName(columnVariable);
	}

    public static String _getColumnName(String columnVariable) {

		try {
			Field field = SSession.class.getDeclaredField(columnVariable);
			if (field != null) {
				Column annotation = field.getDeclaredAnnotation(Column.class);
				if (annotation != null) {
					return SSession._getTableName() + "." + annotation.name();
				} else {
					JoinColumn joinAnnotation = field.getDeclaredAnnotation(JoinColumn.class);
					if (joinAnnotation != null) {
						return SSession._getTableName() + "." + joinAnnotation.name();
					}

					System.out.println("ERROR: annotation is null for field=" + field.toString());
					return null;
				}
			}
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
    public String getTableName() {
		return SSession._getTableName();
    }

    public static String _getTableName() {
		Table annotation = SSession.class.getDeclaredAnnotation(Table.class);
		if (annotation != null) {
			return annotation.name();
		}
		return "s_session";
	}

    @Override
    public String getForeignTableName(String foreignVariable) {
		return SSession._getForeignTableName(foreignVariable);
    }

	public static String _getForeignTableName(String foreignVariable) {
		if ("fPerson".equals(foreignVariable))
			return FPerson._getTableName();

		return null;
	}
    
    @Override
    public String getSqlSelectTableStatement(List<String> parameterList, List<String> valueList) {
		return SSession._getSqlTableStatement(parameterList, valueList);
    }

    public static String _getSqlTableStatement(List<String> parameterList, List<String> valueList) {
		return "select * from s_session ";
	}
}
