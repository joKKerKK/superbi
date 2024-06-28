package com.flipkart.fdp.superbi.entities;


import java.io.Serializable;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by niket.dixit on 28/04/23
 */
@Entity
@Inheritance(strategy = InheritanceType.JOINED)
@Table(name = "native_expression")
@Data
@NoArgsConstructor
@AllArgsConstructor
@IdClass(value = NativeExpression.IdClass.class)
public class NativeExpression {
  @Column(name = "native_expression_name", nullable = false)
  @Id
  private String nativeExpressionName;

  @Column(name = "native_expression_logic",columnDefinition = "MEDIUMTEXT", nullable = false)
  private String nativeExpressionLogic;

  @Column(name = "native_expression_description", nullable = true)
  private String nativeExpressionDescription;

  @Column(name = "fact_name", nullable = false)
  @Id
  private String factName;

  @Column(name = "native_expression_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private NativeExpressionType nativeExpressionType;

  @Column(name = "isDisabled", columnDefinition = "BIT", length = 1)
  protected boolean disabled=false;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "created_at", nullable = false)
  private Date created;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "updated_at", nullable = false)
  private Date updated;

  @Column(name = "updated_by", nullable = false)
  private String updatedBy;

  @Column(name = "owner_type", nullable = false)
  @Enumerated(EnumType.STRING)
  private OwnerType ownerType;

  @PrePersist
  protected void onCreate() {
    created = (created == null) ? new Date() : created;
    updated = (updated == null) ? created : new Date();
  }

  @PreUpdate
  protected void onUpdate() {
    updated = new Date();
  }

  public enum NativeExpressionType
  {
    SELECT, FILTER
  }

  public enum OwnerType
  {
    ADMIN, USER
  }

  public String getNativeExpressionType() {
    return this.nativeExpressionType.name();
  }

  public void setNativeExpressionType(String nativeExpressionType) {
    this.nativeExpressionType = NativeExpressionType.valueOf(nativeExpressionType);
  }

  public NativeExpression(String nativeExpressionName, String nativeExpressionLogic,
                          String nativeExpressionDescription, String factName) {
    this.nativeExpressionName = nativeExpressionName;
    this.nativeExpressionLogic = nativeExpressionLogic;
    this.nativeExpressionDescription = nativeExpressionDescription;
    this.factName = factName;
  }

  public void markDisabled() {
    disabled = true;
  }

  @Data
  public static class IdClass implements Serializable {
    private String nativeExpressionName;
    private String factName;
  }
}
