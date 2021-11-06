package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv5.model.entity.CaseLog;

@Service
public class CaseLogServiceImp extends BaseEntityServiceImp<CaseLog> implements CaseLogService {

    public CaseLogServiceImp() {
        super(CaseLog.class);
    }

    @Override
    public CaseLog construct(ResultSet rs, CaseLog entity, String alias) {
		return CaseLogService._construct(rs, entity, alias);
    }

    @Override
    public CaseLog construct(FieldValueList rowResult, CaseLog entity, String alias, List<String> columns) {
		return CaseLogService._construct(rowResult, entity, alias, columns);
    }
    
}
