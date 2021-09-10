package edu.gatech.chai.omopv6.model.entity;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/** 
 * This class maintains fhir resources to avoid duplications.
 * @author Myung Choi
 */
@Entity
@Table(name="f_resource_deduplicate")
public class FResourceDeduplicate extends BaseEntity {
	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="fresourcededuplicate_seq_gen")
	@SequenceGenerator(name="fresourcededuplicate_seq_gen", sequenceName="fresourcededuplicate_seq", allocationSize=1)
	@Column(name = "id")
	@Access(AccessType.PROPERTY)
	private Long id;
    
    @Column(name = "domain_id", nullable=false)
	private String domainId;

    @Column(name="omop_id", nullable=false)
	private Long omopId;

    @Column(name = "fhir_resource_type", nullable=false)
	private String fhirResourceType;

    @Column(name = "fhir_identifier_system", nullable=false)
	private String fhirIdentifierSystem;

    @Column(name = "fhir_identifier_value", nullable=false)
	private String fhirIdentifierValue;

    public FResourceDeduplicate() {
        super();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDomainId() {
        return domainId;
    }

    public void setDomainId(String domainId) {
        this.domainId = domainId;
    }

    public Long getOmopId() {
        return omopId;
    }

    public void setOmopId(Long omopId) {
        this.omopId = omopId;
    }

    public String getFhirResourceType() {
        return fhirResourceType;
    }

    public void setFhirResourceType(String fhirResourceType) {
        this.fhirResourceType = fhirResourceType;
    }

    public String getFhirIdentifierSystem() {
        return fhirIdentifierSystem;
    }

    public void setFhirIdentifierSystem(String fhirIdentifierSystem) {
        this.fhirIdentifierSystem = fhirIdentifierSystem;
    }

    public String getFhirIdentifierValue() {
        return fhirIdentifierValue;
    }

    public void setFhirIdentifierValue(String fhirIdentifierValue) {
        this.fhirIdentifierValue = fhirIdentifierValue;
    }
}
