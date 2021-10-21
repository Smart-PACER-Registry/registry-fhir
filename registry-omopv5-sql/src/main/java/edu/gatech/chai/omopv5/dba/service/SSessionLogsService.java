package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import edu.gatech.chai.omopv5.dba.util.SqlUtil;
import edu.gatech.chai.omopv5.model.entity.SSession;
import edu.gatech.chai.omopv5.model.entity.SSessionLogs;

public interface SSessionLogsService extends IService<SSessionLogs> {
	public static SSessionLogs _construct(ResultSet rs, SSessionLogs sSessionLogs, String alias) {
		if (sSessionLogs == null)
        sSessionLogs = new SSessionLogs();

		if (alias == null || alias.isEmpty())
			alias = SSessionLogs._getTableName();

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int totalColumnSize = metaData.getColumnCount();
			for (int i = 1; i <= totalColumnSize; i++) {
				String columnInfo = metaData.getColumnName(i);

				if (columnInfo.equalsIgnoreCase(alias + "_session_log_id")) {
					sSessionLogs.setId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase("session_id")) {
					SSession session = SSessionService._construct(rs, null, "session");
					sSessionLogs.setSession(session);
				} else if (columnInfo.equalsIgnoreCase(alias + "_datetime")) {
					sSessionLogs.setDateTime(rs.getDate(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_text")) {
					sSessionLogs.setText(rs.getString(columnInfo));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return sSessionLogs;
	}

	public static SSessionLogs _construct(FieldValueList rowResult, SSessionLogs sSessionLogs,
			String alias, List<String> columns) {
		if (sSessionLogs == null)
            sSessionLogs = new SSessionLogs();

		if (alias == null || alias.isEmpty())
			alias = SSessionLogs._getTableName();

		for (String columnInfo : columns) {
			if (rowResult.get(columnInfo).isNull()) continue;

			if (columnInfo.equalsIgnoreCase(alias + "_session_log_id")) {
				sSessionLogs.setId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase("session_id")) {
				SSession session = SSessionService._construct(rowResult, null, "session", columns);
				sSessionLogs.setSession(session);
			} else if (columnInfo.equalsIgnoreCase(alias + "_datetime")) {
				String dateString = rowResult.get(columnInfo).getStringValue();
				Date date = SqlUtil.string2DateTime(dateString);
				if (date != null) {
					sSessionLogs.setDateTime(date);
				}
			} else if (columnInfo.equalsIgnoreCase(alias + "_text")) {
				sSessionLogs.setText(rowResult.get(columnInfo).getStringValue());
			}
		}

		return sSessionLogs;
	}    
}
