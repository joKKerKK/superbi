package com.flipkart.fdp.superbi.entities;

import com.flipkart.fdp.superbi.constants.ConsumableEntityType;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import lombok.Data;
import org.hibernate.annotations.BatchSize;

/**
 * Created by akshaya.sharma on 08/07/19
 */
@Entity
@javax.persistence.Table(name = "ConsumableEntity_neo")
@Inheritance(strategy = InheritanceType.JOINED)
@DiscriminatorColumn(
    name = "resourceType",
    discriminatorType = DiscriminatorType.STRING
)
@Data
public abstract class ConsumableEntity extends AbstractBaseEntity {
  @Column(nullable = false)
  protected String org;

  @Column(nullable = false)
  protected String namespace;

  @Column(nullable = false)
  protected String name;

  @Column(nullable = true)
  protected String displayName;

  @ManyToOne(fetch = FetchType.EAGER)
  @BatchSize(size = 500)
  @JoinColumn(name = "owner_userId", nullable = false)
  protected User owner;

  @Column(nullable = true)
  protected String description;

  @Column(nullable = true)
  protected String message;

  @Column
  @Enumerated(EnumType.STRING)
  protected ConsumableEntityType resourceType;

  @ManyToMany(cascade = {CascadeType.PERSIST}, fetch = FetchType.EAGER)
  @JoinTable(name = "ConsumableEntity_Tag_neo", joinColumns = {
      @JoinColumn(name = "ConsumableEntity_neo_Id")}, inverseJoinColumns = {
      @JoinColumn(name = "Tag_neo_Id")})
  private Set<Tag> tags = new HashSet<Tag>();


  @Column(name = "isDisabled", columnDefinition = "BIT", length = 1)
  protected boolean disabled=false;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "disabledOn")
  protected Date disabledOn;

  @Column(name = "configVersion")
  protected Integer configVersion;

  @Transient
  @Override
  public Serializable getPK() {
    return this.getId();
  }
}
