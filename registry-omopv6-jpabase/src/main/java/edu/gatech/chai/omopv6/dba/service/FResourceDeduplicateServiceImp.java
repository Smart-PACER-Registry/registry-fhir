package edu.gatech.chai.omopv6.dba.service;

import edu.gatech.chai.omopv6.jpa.dao.FResourceDeduplicateDao;
import edu.gatech.chai.omopv6.model.entity.FResourceDeduplicate;

public class FResourceDeduplicateServiceImp extends BaseEntityServiceImp<FResourceDeduplicate, FResourceDeduplicateDao> implements FResourceDeduplicateService {

    public FResourceDeduplicateServiceImp() {
        super(FResourceDeduplicate.class);
    }
}
