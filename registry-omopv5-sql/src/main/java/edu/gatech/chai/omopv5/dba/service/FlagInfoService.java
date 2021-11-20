package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import edu.gatech.chai.omopv5.dba.util.SqlUtil;
import edu.gatech.chai.omopv5.model.entity.CaseInfo;
import edu.gatech.chai.omopv5.model.entity.FPerson;
import edu.gatech.chai.omopv5.model.entity.FlagInfo;

public interface FlagInfoService extends IService<FlagInfo> {
	public static FlagInfo _construct(ResultSet rs, FlagInfo flagInfo, String alias) {
		if (flagInfo == null) {
			flagInfo = new FlagInfo();
		}

		if (alias == null || alias.isEmpty())
			alias = FlagInfo._getTableName();

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int totalColumnSize = metaData.getColumnCount();
			for (int i = 1; i <= totalColumnSize; i++) {
				String columnInfo = metaData.getColumnName(i);

				if (columnInfo.equalsIgnoreCase(alias + "_flag_info_id")) {
					flagInfo.setId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase("caseInfo_case_info_id")) {
					CaseInfo caseInfo = CaseInfoService._construct(rs, null, "caseInfo");
					flagInfo.setCaseInfo(caseInfo);
                } else if (columnInfo.equalsIgnoreCase("fPerson_person_id")) {
					FPerson fPerson = FPersonService._construct(rs, null, "fPerson");
					flagInfo.setFPerson(fPerson);
                } else if (columnInfo.equalsIgnoreCase(alias + "_domain")) {
					flagInfo.setDomain(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_domain_data_id")) {
					flagInfo.setDomainDataId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_flag_type")) {
					flagInfo.setFlagType(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_last_updated")) {
					flagInfo.setLastUpdated(rs.getDate(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_annotation")) {
					flagInfo.setAnnotation(rs.getString(columnInfo));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return flagInfo;
	}

	public static FlagInfo _construct(FieldValueList rowResult, FlagInfo flagInfo,
			String alias, List<String> columns) {
		if (flagInfo == null) {
			flagInfo = new FlagInfo();
		}

		if (alias == null || alias.isEmpty())
			alias = CaseInfo._getTableName();

		for (String columnInfo : columns) {
			if (rowResult.get(columnInfo).isNull()) continue;

			if (columnInfo.equalsIgnoreCase(alias + "_flag_info_id")) {
				flagInfo.setId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase("caseInfo_case_info_id")) {
				CaseInfo caseInfo = CaseInfoService._construct(rowResult, null, "caseInfo", columns);
				flagInfo.setCaseInfo(caseInfo);
			} else if (columnInfo.equalsIgnoreCase("fPerson_person_id")) {
				FPerson fPerson = FPersonService._construct(rowResult, null, "fPerson", columns);
				flagInfo.setFPerson(fPerson);
			} else if (columnInfo.equalsIgnoreCase(alias + "_domain")) {
				flagInfo.setDomain(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_domain_data_id")) {
				flagInfo.setDomainDataId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_flag_type")) {
				flagInfo.setFlagType(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_last_updated")) {
				Date date = SqlUtil.string2DateTime(rowResult.get(columnInfo).getStringValue());
				if (date != null) {
					flagInfo.setLastUpdated(date);
				}
			} else if (columnInfo.equalsIgnoreCase(alias + "_annotation")) {
				flagInfo.setAnnotation(rowResult.get(columnInfo).getStringValue());
			}
		}

		return flagInfo;
	}	
}
