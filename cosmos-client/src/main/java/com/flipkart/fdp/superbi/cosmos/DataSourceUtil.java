package com.flipkart.fdp.superbi.cosmos;

import com.flipkart.fdp.mmg.cosmos.api.TableEnhancementSTO;
import com.flipkart.fdp.mmg.cosmos.entities.Column;
import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.flipkart.fdp.mmg.cosmos.entities.Dimension;
import com.flipkart.fdp.mmg.cosmos.entities.Fact;
import com.flipkart.fdp.mmg.cosmos.sto.impl.TableEnhancementSTOFactoryImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

public class DataSourceUtil {

  public static final TableEnhancementSTOFactoryImpl TABLE_ENHANCEMENT_STO_FACTORY = new TableEnhancementSTOFactoryImpl();
  @NotNull
  public static ArrayList<String> getArrayColumns(
      com.flipkart.fdp.mmg.cosmos.entities.Table table) {
    ArrayList<String> arrayColumns =  new ArrayList<>();
    List<Column> columnList = table.getColumns();
    for(Column col: columnList){
      if(col.isArray())
        arrayColumns.add(col.getName());
    }
    return arrayColumns;
  }

  public static ArrayList<String> getDateTimeColsList(
      com.flipkart.fdp.mmg.cosmos.entities.Table table) {
    ArrayList<String> datetimeCols =  new ArrayList<>();
    for(Column column : table.getColumns()){
      if(column.getType().equals("DATETIME")){
        datetimeCols.add(column.getName());
      }
    }
    return datetimeCols;
  }

  public static List<String> getDateTimeColsList(DataSource dataSource) {
    return getDateTimeColsList(getTable(dataSource));
  }

  public static Optional<TableEnhancementSTO> getTableEnhancement(
      com.flipkart.fdp.mmg.cosmos.entities.Table table) {
    return Objects.isNull(table) ? Optional.empty() : Optional.ofNullable(
        TABLE_ENHANCEMENT_STO_FACTORY.toSTO(table.getTableEnhancement()));
  }

  public static com.flipkart.fdp.mmg.cosmos.entities.Table getTable(DataSource dataSource) {
    com.flipkart.fdp.mmg.cosmos.entities.Table table = null;
    if (dataSource instanceof Fact) {
      table = ((Fact) dataSource).getTable();
    } else if (dataSource instanceof Dimension) {
      table = ((Dimension) dataSource).getTable();
    } else if (dataSource instanceof com.flipkart.fdp.mmg.cosmos.entities.Table) {
      table = (com.flipkart.fdp.mmg.cosmos.entities.Table) dataSource;
    }
    return table;
  }
}
