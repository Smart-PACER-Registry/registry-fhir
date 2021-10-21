package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import edu.gatech.chai.omopv5.dba.util.SqlUtil;
import edu.gatech.chai.omopv5.model.entity.FPerson;
import edu.gatech.chai.omopv5.model.entity.SSession;

public interface SSessionService extends IService<SSession> {
	public static SSession _construct(ResultSet rs, SSession sSession, String alias) {
		if (sSession == null)
            sSession = new SSession();

		if (alias == null || alias.isEmpty())
			alias = SSession._getTableName();

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int totalColumnSize = metaData.getColumnCount();
			for (int i = 1; i <= totalColumnSize; i++) {
				String columnInfo = metaData.getColumnName(i);

				if (columnInfo.equalsIgnoreCase(alias + "_session_id")) {
					sSession.setId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase("fPerson_person_id")) {
					FPerson fPerson = FPersonService._construct(rs, null, "fPerson");
					sSession.setFPerson(fPerson);
                } else if (columnInfo.equalsIgnoreCase(alias + "_job_id")) {
					sSession.setJobId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_status")) {
					sSession.setStatus(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_status_url")) {
					sSession.setStatusUrl(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_server_url")) {
					sSession.setServerUrl(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_patient_identifier")) {
					sSession.setPatientIdentifier(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_last_updated")) {
					sSession.setLastUpdated(rs.getDate(columnInfo));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return sSession;
	}

	public static SSession _construct(FieldValueList rowResult, SSession sSession,
			String alias, List<String> columns) {
		if (sSession == null)
            sSession = new SSession();

		if (alias == null || alias.isEmpty())
			alias = SSession._getTableName();

		for (String columnInfo : columns) {
			if (rowResult.get(columnInfo).isNull()) continue;

			if (columnInfo.equalsIgnoreCase(alias + "_session_id")) {
				sSession.setId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase("fPerson_person_id")) {
				FPerson fPerson = FPersonService._construct(rowResult, null, "fPerson", columns);
				sSession.setFPerson(fPerson);
			} else if (columnInfo.equalsIgnoreCase(alias + "_job_id")) {
				sSession.setJobId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_status")) {
				sSession.setStatus(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_status_url")) {
				sSession.setStatusUrl(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_server_url")) {
				sSession.setServerUrl(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_patient_identifier")) {
				sSession.setPatientIdentifier(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_last_updated")) {
				String dateString = rowResult.get(columnInfo).getStringValue();
				Date date = SqlUtil.string2DateTime(dateString);
				if (date != null) {
					sSession.setLastUpdated(date);
				}
			}
		}

		return sSession;
	}	
}
