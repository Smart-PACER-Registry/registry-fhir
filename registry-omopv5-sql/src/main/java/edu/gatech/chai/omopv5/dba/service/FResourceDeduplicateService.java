package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import edu.gatech.chai.omopv5.model.entity.FResourceDeduplicate;

public interface FResourceDeduplicateService extends IService<FResourceDeduplicate>{
	public static FResourceDeduplicate _construct(ResultSet rs, FResourceDeduplicate fResourceDeduplicate, String alias) {
		if (fResourceDeduplicate == null)
            fResourceDeduplicate = new FResourceDeduplicate();

		if (alias == null || alias.isEmpty())
			alias = FResourceDeduplicate._getTableName();

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int totalColumnSize = metaData.getColumnCount();
			for (int i = 1; i <= totalColumnSize; i++) {
				String columnInfo = metaData.getColumnName(i);

				if (columnInfo.equalsIgnoreCase(alias + "_id")) {
					fResourceDeduplicate.setId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_domain_id")) {
					fResourceDeduplicate.setDomainId(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_omop_id")) {
					fResourceDeduplicate.setOmopId(rs.getLong(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_fhir_resource_type")) {
					fResourceDeduplicate.setFhirResourceType(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_fhir_identifier_system")) {
					fResourceDeduplicate.setFhirIdentifierSystem(rs.getString(columnInfo));
				} else if (columnInfo.equalsIgnoreCase(alias + "_fhir_identifier_value")) {
					fResourceDeduplicate.setFhirIdentifierValue(rs.getString(columnInfo));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return fResourceDeduplicate;
	}

	public static FResourceDeduplicate _construct(FieldValueList rowResult, FResourceDeduplicate fResourceDeduplicate,
			String alias, List<String> columns) {
		if (fResourceDeduplicate == null)
            fResourceDeduplicate = new FResourceDeduplicate();

		if (alias == null || alias.isEmpty())
			alias = FResourceDeduplicate._getTableName();

		for (String columnInfo : columns) {
			if (rowResult.get(columnInfo).isNull()) continue;

			if (columnInfo.equalsIgnoreCase(alias + "_id")) {
				fResourceDeduplicate.setId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_domain_id")) {
				fResourceDeduplicate.setDomainId(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_omop_id")) {
				fResourceDeduplicate.setOmopId(rowResult.get(columnInfo).getLongValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_fhir_resource_type")) {
				fResourceDeduplicate.setFhirResourceType(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_fhir_identifier_system")) {
				fResourceDeduplicate.setFhirIdentifierSystem(rowResult.get(columnInfo).getStringValue());
			} else if (columnInfo.equalsIgnoreCase(alias + "_fhir_identifier_value")) {
				fResourceDeduplicate.setFhirIdentifierValue(rowResult.get(columnInfo).getStringValue());
			}
		}

		return fResourceDeduplicate;
	}
}
