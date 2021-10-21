package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv5.model.entity.SSessionLogs;

@Service
public class SSessionLogsServiceImp extends BaseEntityServiceImp<SSessionLogs> implements SSessionLogsService {

    public SSessionLogsServiceImp() {
        super(SSessionLogs.class);
    }

    @Override
    public SSessionLogs construct(ResultSet rs, SSessionLogs entity, String alias) {
		return SSessionLogsService._construct(rs, entity, alias);
    }

    @Override
    public SSessionLogs construct(FieldValueList rowResult, SSessionLogs entity, String alias, List<String> columns) {
		return SSessionLogsService._construct(rowResult, entity, alias, columns);
    }
    
}
