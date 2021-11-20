package edu.gatech.chai.omopv5.dba.service;

import java.sql.ResultSet;
import java.util.List;

import com.google.cloud.bigquery.FieldValueList;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv5.model.entity.FlagInfo;

@Service
public class FlagInfoServiceImp extends BaseEntityServiceImp<FlagInfo> implements FlagInfoService {

    public FlagInfoServiceImp() {
        super(FlagInfo.class);
    }

    @Override
    public FlagInfo construct(ResultSet rs, FlagInfo entity, String alias) {
		return FlagInfoService._construct(rs, entity, alias);
    }

    @Override
    public FlagInfo construct(FieldValueList rowResult, FlagInfo entity, String alias, List<String> columns) {
		return FlagInfoService._construct(rowResult, entity, alias, columns);
    }
	
}
