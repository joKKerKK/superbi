package com.flipkart.fdp.superbi.entities;

import com.flipkart.fdp.superbi.converter.MapPropConverter;
import java.io.Serializable;
import java.util.Map;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by mansi.jain on 04/03/22
 */
@Entity
@Table(name = "gcp_table_federation")
@Data
@NoArgsConstructor
@AllArgsConstructor
@IdClass(value = TableFederation.IdClass.class)
public class TableFederation {

  @Column(name = "table_name", nullable = false)
  @Id
  private String tableName;

  @Column(name = "overriding_store_identifier")
  @Id
  private String overridingStoreIdentifier;

  @Convert(converter = MapPropConverter.class)
  @Column(name = "table_properties")
  private Map<String, String> tableProperties;

  @Data
  public static class IdClass implements Serializable {

    private String tableName;
    private String overridingStoreIdentifier;
  }
}
