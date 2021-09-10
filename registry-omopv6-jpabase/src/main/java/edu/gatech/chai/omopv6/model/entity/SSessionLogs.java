package edu.gatech.chai.omopv6.model.entity;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/** 
 * This class maintains session information for Syphilis registry.
 * @author Myung Choi
 */
@Entity
@Table(name="s_session_logs")
public class SSessionLogs extends BaseEntity {
    @Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ssession_logs_seq_gen")
	@SequenceGenerator(name="ssession_logs_seq_gen", sequenceName="ssession_log_id_seq", allocationSize=1)
	@Column(name = "session_log_id")
	@Access(AccessType.PROPERTY)
	private Long id;

	@ManyToOne
	@JoinColumn(name = "session_id", nullable = false)
	private SSession session;

    @Column(name = "datetime")
	private Date datetime;
	
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

    public Date getDateTime() {
		return datetime;
	}

	public void setDateTime(Date datetime) {
		this.datetime = datetime;
	}	

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
