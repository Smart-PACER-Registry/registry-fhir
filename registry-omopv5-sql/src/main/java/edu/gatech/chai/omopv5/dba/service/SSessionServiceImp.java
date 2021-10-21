package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv5.model.entity.SSession;

@Service
public class SSessionServiceImp extends BaseEntityServiceImp<SSession> implements SSessionService {

    public SSessionServiceImp() {
        super(SSession.class);
    }

    @Override
    public SSession construct(ResultSet rs, SSession entity, String alias) {
		return SSessionService._construct(rs, entity, alias);
    }

    @Override
    public SSession construct(FieldValueList rowResult, SSession entity, String alias, List<String> columns) {
		return SSessionService._construct(rowResult, entity, alias, columns);
    }
	
}
