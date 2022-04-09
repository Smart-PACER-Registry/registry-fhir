package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.gatech.chai.omopv5.dba.util.SqlUtil;
import edu.gatech.chai.omopv5.model.entity.FPerson;
import edu.gatech.chai.omopv5.model.entity.CaseInfo;

public interface CaseInfoService extends IService<CaseInfo> {
	public static final Logger logger = LoggerFactory.getLogger(CaseInfoService.class);

	public static CaseInfo _construct(ResultSet rs, CaseInfo caseInfo, String alias) {
		if (caseInfo == null) {
			caseInfo = new CaseInfo();
		}

		if (alias == null || alias.isEmpty())
			alias = CaseInfo._getTableName();

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int totalColumnSize = metaData.getColumnCount();
			for (int i = 1; i <= totalColumnSize; i++) {
				String columnInfo = metaData.getColumnName(i);

				if (columnInfo.equalsIgnoreCase(alias + "_case_info_id")) {
					caseInfo.setId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase("fPerson_person_id")) {
					FPerson fPerson = FPersonService._construct(rs, null, "fPerson");
					caseInfo.setFPerson(fPerson);
                } else if (columnInfo.equalsIgnoreCase(alias + "_job_id")) {
					caseInfo.setJobId(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_status")) {
					caseInfo.setStatus(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_server_host")) {
					caseInfo.setServerHost(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_status_url")) {
					caseInfo.setStatusUrl(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_server_url")) {
					caseInfo.setServerUrl(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_patient_identifier")) {
					caseInfo.setPatientIdentifier(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_trigger_at")) {
					caseInfo.setTriggerAt(rs.getDate(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_last_updated")) {
					caseInfo.setLastUpdated(rs.getDate(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_activated")) {
					caseInfo.setActivated(rs.getDate(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_created")) {
					caseInfo.setCreated(rs.getDate(columnInfo));
				}
			}
		} catch (SQLException e) {
			logger.error("CaseInfo construct failed: ");
			e.printStackTrace();
			return null;
		}

		return caseInfo;
	}

	public static CaseInfo _construct(FieldValueList rowResult, CaseInfo caseInfo,
			String alias, List<String> columns) {
		if (caseInfo == null) {
			caseInfo = new CaseInfo();
		}

		if (alias == null || alias.isEmpty())
			alias = CaseInfo._getTableName();

		for (String columnInfo : columns) {
			if (rowResult.get(columnInfo).isNull()) continue;

			if (columnInfo.equalsIgnoreCase(alias + "_case_info_id")) {
				caseInfo.setId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase("fPerson_person_id")) {
				FPerson fPerson = FPersonService._construct(rowResult, null, "fPerson", columns);
				caseInfo.setFPerson(fPerson);
			} else if (columnInfo.equalsIgnoreCase(alias + "_job_id")) {
				caseInfo.setJobId(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_status")) {
				caseInfo.setStatus(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_server_host")) {
				caseInfo.setServerHost(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_status_url")) {
				caseInfo.setStatusUrl(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_server_url")) {
				caseInfo.setServerUrl(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_patient_identifier")) {
				caseInfo.setPatientIdentifier(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_trigger_at")) {
				Date date = SqlUtil.string2DateTime(rowResult.get(columnInfo).getStringValue());
				if (date != null) {
					caseInfo.setTriggerAt(date);
				}
			} else if (columnInfo.equalsIgnoreCase(alias + "_last_updated")) {
				Date date = SqlUtil.string2DateTime(rowResult.get(columnInfo).getStringValue());
				if (date != null) {
					caseInfo.setLastUpdated(date);
				}
			} else if (columnInfo.equalsIgnoreCase(alias + "_activated")) {
				Date date = SqlUtil.string2DateTime(rowResult.get(columnInfo).getStringValue());
				if (date != null) {
					caseInfo.setActivated(date);
				}
			} else if (columnInfo.equalsIgnoreCase(alias + "_created")) {
				Date date = SqlUtil.string2DateTime(rowResult.get(columnInfo).getStringValue());
				if (date != null) {
					caseInfo.setCreated(date);
				}
			}
		}

		return caseInfo;
	}	
}
