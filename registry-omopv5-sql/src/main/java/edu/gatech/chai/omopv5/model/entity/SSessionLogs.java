package edu.gatech.chai.omopv5.model.entity;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

import edu.gatech.chai.omopv5.model.entity.custom.Column;
import edu.gatech.chai.omopv5.model.entity.custom.GeneratedValue;
import edu.gatech.chai.omopv5.model.entity.custom.GenerationType;
import edu.gatech.chai.omopv5.model.entity.custom.Id;
import edu.gatech.chai.omopv5.model.entity.custom.JoinColumn;
import edu.gatech.chai.omopv5.model.entity.custom.Table;

/** 
 * This class maintains session information for Syphilis registry.
 * @author Myung Choi
 */
@Table(name="s_session_logs")
public class SSessionLogs extends BaseEntity {
    @Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ssessionlogs_logs_seq_gen")
	@Column(name = "session_log_id")
	private Long id;

	@JoinColumn(name = "session_id", nullable = false)
	private SSession session;

    @Column(name = "log_datetime")
	private Date logDatetime;
	
    @Column(name = "text")
    private String text;

    public SSessionLogs() {
		super();
	}

    public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public SSession getSession() {
		return session;
	}

	public void setSession(SSession session) {
		this.session = session;
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
		return SSessionLogs._getColumnName(columnVariable);
	}

    public static String _getColumnName(String columnVariable) {
		try {
			Field field = SSessionLogs.class.getDeclaredField(columnVariable);
			if (field != null) {
				Column annotation = field.getDeclaredAnnotation(Column.class);
				if (annotation != null) {
					return SSessionLogs._getTableName() + "." + annotation.name();
				} else {
					JoinColumn joinAnnotation = field.getDeclaredAnnotation(JoinColumn.class);
					if (joinAnnotation != null) {
						return SSessionLogs._getTableName() + "." + joinAnnotation.name();
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
		return SSessionLogs._getTableName();
    }

    public static String _getTableName() {
		Table annotation = SSessionLogs.class.getDeclaredAnnotation(Table.class);
		if (annotation != null) {
			return annotation.name();
		}
		return "s_session_logs";
	}

    @Override
    public String getForeignTableName(String foreignVariable) {
		return SSessionLogs._getForeignTableName(foreignVariable);
    }

	public static String _getForeignTableName(String foreignVariable) {
		if ("session".equals(foreignVariable))
			return SSession._getTableName();

		return null;
	}
    
    @Override
    public String getSqlSelectTableStatement(List<String> parameterList, List<String> valueList) {
		return SSessionLogs._getSqlTableStatement(parameterList, valueList);
    }

    public static String _getSqlTableStatement(List<String> parameterList, List<String> valueList) {
		return "select * from s_session_logs ";
	}
}
