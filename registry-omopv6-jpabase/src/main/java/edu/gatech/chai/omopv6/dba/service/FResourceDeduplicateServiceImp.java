package edu.gatech.chai.omopv6.dba.service;

import org.springframework.stereotype.Service;

import edu.gatech.chai.omopv6.jpa.dao.FResourceDeduplicateDao;
import edu.gatech.chai.omopv6.model.entity.FResourceDeduplicate;

@Service
public class FResourceDeduplicateServiceImp extends BaseEntityServiceImp<FResourceDeduplicate, FResourceDeduplicateDao> implements FResourceDeduplicateService {

    public FResourceDeduplicateServiceImp() {
        super(FResourceDeduplicate.class);
    }
}
