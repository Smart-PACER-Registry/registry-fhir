package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import edu.gatech.chai.omopv5.dba.util.SqlUtil;
import edu.gatech.chai.omopv5.model.entity.CaseInfo;
import edu.gatech.chai.omopv5.model.entity.CaseLog;

public interface CaseLogService extends IService<CaseLog> {
	public static CaseLog _construct(ResultSet rs, CaseLog caseLog, String alias) {
		if (caseLog == null)
        caseLog = new CaseLog();

		if (alias == null || alias.isEmpty())
			alias = CaseLog._getTableName();

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int totalColumnSize = metaData.getColumnCount();
			for (int i = 1; i <= totalColumnSize; i++) {
				String columnInfo = metaData.getColumnName(i);

				if (columnInfo.equalsIgnoreCase(alias + "_case_log_id")) {
					caseLog.setId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase("case_info_id")) {
					CaseInfo caseInfo = CaseInfoService._construct(rs, null, "caseInfo");
					caseLog.setCaseInfo(caseInfo);
				} else if (columnInfo.equalsIgnoreCase(alias + "_log_datetime")) {
					caseLog.setLogDateTime(rs.getDate(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_text")) {
					caseLog.setText(rs.getString(columnInfo));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return caseLog;
	}

	public static CaseLog _construct(FieldValueList rowResult, CaseLog caseLog,
			String alias, List<String> columns) {
		if (caseLog == null)
            caseLog = new CaseLog();

		if (alias == null || alias.isEmpty())
			alias = CaseLog._getTableName();

		for (String columnInfo : columns) {
			if (rowResult.get(columnInfo).isNull()) continue;

			if (columnInfo.equalsIgnoreCase(alias + "_case_log_id")) {
				caseLog.setId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase("case_info_id")) {
				CaseInfo caseInfo = CaseInfoService._construct(rowResult, null, "caseInfo", columns);
				caseLog.setCaseInfo(caseInfo);
			} else if (columnInfo.equalsIgnoreCase(alias + "_log_datetime")) {
				String dateString = rowResult.get(columnInfo).getStringValue();
				Date date = SqlUtil.string2DateTime(dateString);
				if (date != null) {
					caseLog.setLogDateTime(date);
				}
			} else if (columnInfo.equalsIgnoreCase(alias + "_text")) {
				caseLog.setText(rowResult.get(columnInfo).getStringValue());
			}
		}

		return caseLog;
	}    
}
