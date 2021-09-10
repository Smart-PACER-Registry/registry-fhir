package edu.gatech.chai.omopv6.dba.service;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv6.jpa.dao.SSessionDao;
import edu.gatech.chai.omopv6.model.entity.SSession;

@Service
public class SSessionServiceImp extends BaseEntityServiceImp<SSession, SSessionDao> implements SSessionService {

    public SSessionServiceImp() {
        super(SSession.class);
    }
	
}
