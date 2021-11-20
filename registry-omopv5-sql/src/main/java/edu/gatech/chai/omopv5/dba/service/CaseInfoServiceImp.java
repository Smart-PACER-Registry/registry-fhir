package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv5.model.entity.CaseInfo;

@Service
public class CaseInfoServiceImp extends BaseEntityServiceImp<CaseInfo> implements CaseInfoService {

    public CaseInfoServiceImp() {
      super(CaseInfo.class);
    }

    @Override
    public CaseInfo construct(ResultSet rs, CaseInfo entity, String alias) {
		  return CaseInfoService._construct(rs, entity, alias);
    }

    @Override
    public CaseInfo construct(FieldValueList rowResult, CaseInfo entity, String alias, List<String> columns) {
		  return CaseInfoService._construct(rowResult, entity, alias, columns);
    }
	
}
