package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
public class GsheetInfo {

  public enum SaveOption{NEW_FILE,OVERWRITE}

  private final String spreadsheetId;
  private final String sheetName;
  private final String startCell;
  private final SaveOption saveOption;

  @Builder
  @JsonCreator
  public GsheetInfo(@JsonProperty("spreadsheetId") String spreadsheetId, @JsonProperty("sheetName") String sheetName,
                    @JsonProperty("startCell") String startCell, @JsonProperty("saveOption") SaveOption saveOption) {
    this.spreadsheetId = spreadsheetId;
    this.sheetName = sheetName;
    this.startCell = startCell;
    this.saveOption = saveOption;
  }
}