package edu.gatech.chai.omopv6.dba.service;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv6.jpa.dao.SSessionLogsDao;
import edu.gatech.chai.omopv6.model.entity.SSessionLogs;

@Service
public class SSessionLogsServiceImp extends BaseEntityServiceImp<SSessionLogs, SSessionLogsDao> implements SSessionLogsService {

    public SSessionLogsServiceImp() {
        super(SSessionLogs.class);
    }
    
}
