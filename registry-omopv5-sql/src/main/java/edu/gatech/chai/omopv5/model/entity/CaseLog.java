package edu.gatech.chai.omopv5.model.entity;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

import edu.gatech.chai.omopv5.model.entity.custom.Column;
import edu.gatech.chai.omopv5.model.entity.custom.Id;
import edu.gatech.chai.omopv5.model.entity.custom.JoinColumn;
import edu.gatech.chai.omopv5.model.entity.custom.Table;

/** 
 * This class maintains case information for Syphilis registry.
 * @author Myung Choi
 */
@Table(name="case_log")
public class CaseLog extends BaseEntity {
    @Id
	@Column(name = "case_log_id")
	private Long id;

	@JoinColumn(name = "case_info_id", nullable = false)
	private CaseInfo caseInfo;

    @Column(name = "log_datetime")
	private Date logDatetime;
	
    @Column(name = "text")
    private String text;

    public CaseLog() {
		super();
	}

    public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public CaseInfo getCaseInfo() {
		return caseInfo;
	}

	public void setCaseInfo(CaseInfo caseInfo) {
		this.caseInfo = caseInfo;
	}

    public Date getLogDateTime() {
		return logDatetime;
	}

	public void setLogDateTime(Date logDatetime) {
		this.logDatetime = logDatetime;
	}	

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

	@Override
	public String getColumnName(String columnVariable) {
		return CaseLog._getColumnName(columnVariable);
	}

    public static String _getColumnName(String columnVariable) {
		try {
			Field field = CaseLog.class.getDeclaredField(columnVariable);
			if (field != null) {
				Column annotation = field.getDeclaredAnnotation(Column.class);
				if (annotation != null) {
					return CaseLog._getTableName() + "." + annotation.name();
				} else {
					JoinColumn joinAnnotation = field.getDeclaredAnnotation(JoinColumn.class);
					if (joinAnnotation != null) {
						return CaseLog._getTableName() + "." + joinAnnotation.name();
					}

					System.out.println("ERROR: annotation is null for field=" + field.toString());
					return null;
				}
			}
		} catch (NoSuchFieldException | SecurityException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
    public String getTableName() {
		return CaseLog._getTableName();
    }

    public static String _getTableName() {
		Table annotation = CaseLog.class.getDeclaredAnnotation(Table.class);
		if (annotation != null) {
			return annotation.name();
		}
		return "case_log";
	}

    @Override
    public String getForeignTableName(String foreignVariable) {
		return CaseLog._getForeignTableName(foreignVariable);
    }

	public static String _getForeignTableName(String foreignVariable) {
		if ("caseInfo".equals(foreignVariable))
			return CaseInfo._getTableName();

		return null;
	}
    
    @Override
    public String getSqlSelectTableStatement(List<String> parameterList, List<String> valueList) {
		return CaseLog._getSqlTableStatement(parameterList, valueList);
    }

    public static String _getSqlTableStatement(List<String> parameterList, List<String> valueList) {
		return "select * from case_log ";
	}
}
