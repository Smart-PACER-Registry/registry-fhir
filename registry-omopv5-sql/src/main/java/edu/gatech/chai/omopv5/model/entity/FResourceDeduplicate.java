package edu.gatech.chai.omopv5.model.entity;

import java.lang.reflect.Field;
import java.util.List;

import edu.gatech.chai.omopv5.model.entity.custom.Column;
import edu.gatech.chai.omopv5.model.entity.custom.GeneratedValue;
import edu.gatech.chai.omopv5.model.entity.custom.GenerationType;
import edu.gatech.chai.omopv5.model.entity.custom.Id;
import edu.gatech.chai.omopv5.model.entity.custom.JoinColumn;
import edu.gatech.chai.omopv5.model.entity.custom.Table;

/** 
 * This class maintains fhir resources to avoid duplications.
 * @author Myung Choi
 */
@Table(name="f_resource_deduplicate")
public class FResourceDeduplicate extends BaseEntity {
	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="fresourcededuplicate_seq_gen")
	@Column(name = "id", nullable = false)
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

    @Override
    public String getColumnName(String columnVariable) {
		return FResourceDeduplicate._getColumnName(columnVariable);
    }

    public static String _getColumnName(String columnVariable) {
		try {
			Field field = FResourceDeduplicate.class.getDeclaredField(columnVariable);
			if (field != null) {
				Column annotation = field.getDeclaredAnnotation(Column.class);
				if (annotation != null) {
					return FResourceDeduplicate._getTableName() + "." + annotation.name();
				} else {
					JoinColumn joinAnnotation = field.getDeclaredAnnotation(JoinColumn.class);
					if (joinAnnotation != null) {
						return FResourceDeduplicate._getTableName() + "." + joinAnnotation.name();
					}

					System.out.println("ERROR: annotation is null for field=" + field.toString());
					return null;
				}
			}
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}

		return null;
	}

    @Override
    public String getTableName() {
		return FResourceDeduplicate._getTableName();
    }

    public static String _getTableName() {
		Table annotation = FResourceDeduplicate.class.getDeclaredAnnotation(Table.class);
		if (annotation != null) {
			return annotation.name();
		}
		return "f_resource_deduplicate";
	}

    @Override
    public String getForeignTableName(String foreignVariable) {
		return null;
    }

    @Override
    public String getSqlSelectTableStatement(List<String> parameterList, List<String> valueList) {
		return FResourceDeduplicate._getSqlTableStatement(parameterList, valueList);
    }

    public static String _getSqlTableStatement(List<String> parameterList, List<String> valueList) {
		return "select * from f_resource_deduplicate ";
	}
}
