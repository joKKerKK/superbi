package com.flipkart.fdp.superbi.subscription.delivery;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.subscription.configurations.GsheetConfig;
import com.flipkart.fdp.superbi.subscription.exceptions.GsheetException;
import com.flipkart.fdp.superbi.subscription.model.EmailDelivery;
import com.flipkart.fdp.superbi.subscription.model.GsheetInfo;
import com.flipkart.fdp.superbi.subscription.model.RawQueryResultWithSchema;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.Color;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.MergeCellsRequest;
import com.google.api.services.sheets.v4.model.RepeatCellRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.TextFormat;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.inject.Inject;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GsheetUtil {
  private static final String DEFAULT_RANGE = "A3";
  private static final String DEFAULT_ALERT_RANGE = "A1";
  private static final int DEFAULT_HEADER_ROW = 3;
  private static final int DEFAULT_ALERT_ROW = 1;
  private static final int DEFAULT_ALERT_COLUMN = 1;
  private static final String DEFAULT_VALUE_INPUT_OPTION = "USER_ENTERED";
  private static final String DEFAULT_SHEET_NAME = "Sheet1";
  private static final String DEFAULT_MIME_TYPE_GSHEET = "application/vnd.google-apps.spreadsheet";
  private static final String APPLICATION_NAME_SHEET = "Google Sheets Subscription";
  private static final String DEFAULT_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/";
  private static final String APPLICATION_NAME_DRIVE = "Google Drive Subscription";
  private static final String DEFAULT_ALERT_MESSAGE = "THIS IS TEMPORARY COPY, LINK WILL EXPIRE "
      + "ON ";
  private static final List<String> SCOPES = Arrays.asList(SheetsScopes.SPREADSHEETS,
      SheetsScopes.DRIVE, SheetsScopes.DRIVE_FILE);
  private static Sheets sheetsService;
  private static Drive driveService;
  private static String folderId;
  private static long gsheetExpiryInSeconds;
  private final CircuitBreaker circuitBreaker;
  private final Retry retry;
  private final MetricRegistry metricRegistry;

  @Inject
  public GsheetUtil(GsheetConfig gsheetConfig, CircuitBreaker circuitBreaker, Retry retry,
                    MetricRegistry metricRegistry) throws IOException, GeneralSecurityException {
    this.circuitBreaker = circuitBreaker;
    this.retry = retry;
    this.metricRegistry = metricRegistry;
    this.sheetsService = getSheetsService();
    this.driveService = getDriveService();
    this.folderId = gsheetConfig.getFolderId();
    this.gsheetExpiryInSeconds = gsheetConfig.getGsheetExpiryInSeconds();
  }

  @SneakyThrows
  public String uploadGsheetWithCircuitBreaker(RawQueryResultWithSchema queryResult,
                                               String reportName,
                                               ScheduleInfo scheduleInfo, GsheetInfo gsheetInfo) {
    log.info("uploadGsheetWithCircuitBreaker called");
    try {
      log.info("QueryResult: {}", queryResult);
    } catch (Exception e) {
      log.error("Error uploading Gsheet with QueryResult: {}. Exception: {}", queryResult, e.getMessage(), e);
      throw e;
    }
    String spreadsheetUrl = Retry.decorateSupplier(retry,
        circuitBreaker.decorateSupplier(()-> uploadGsheet(queryResult, reportName, scheduleInfo,
            gsheetInfo))).get();

    log.info("spreadsheetUrl : " + spreadsheetUrl);
    return spreadsheetUrl;
  }

  @SneakyThrows
  public String uploadGsheet(RawQueryResultWithSchema queryResult, String reportName,
                             ScheduleInfo scheduleInfo, GsheetInfo gsheetInfo) {
    log.info("uploadGsheet started");
    String fileName = reportName + '_' + scheduleInfo.getOwnerId() + '_' + new Date().getTime();
    log.info("fileName : " + fileName);
    List<Object> headers =
        queryResult.getSchema().columns.stream().filter(i->i.isVisible() == true).map(i->i.getAlias()).collect(
        Collectors.toList());
    log.info("Headers:");
    for (Object header : headers) {
      log.info(header.toString());
    }

    List<List<Object>> dataEntry = queryResult.getData();
    dataEntry.add(0, headers);
    log.info("Data Entry:");
    for (List<Object> entry : dataEntry) {
      log.info(entry.toString());
    }

    UpdateValuesResponse updateValuesResponse = null;
    List<String> subscribers = ((EmailDelivery)scheduleInfo.getDeliveryData()).getSubscribers();

    log.info("gsheetInfo.getSaveOption() : " + gsheetInfo.getSaveOption().toString());

    switch(gsheetInfo.getSaveOption()){
      case NEW_FILE:
        log.info("NEW_FILE");
        String spreadsheetId = createSpreadsheet(fileName, folderId);
        log.info("spreadsheetId : " + spreadsheetId);

        try {
          updateValuesResponse = updateValues(spreadsheetId, DEFAULT_RANGE,
              DEFAULT_VALUE_INPUT_OPTION, dataEntry);
          formatHeaderRow(spreadsheetId, DEFAULT_HEADER_ROW, DEFAULT_SHEET_NAME);
          addAlertMessage(spreadsheetId, DEFAULT_ALERT_ROW, DEFAULT_ALERT_COLUMN);
        } catch (IOException e) {
          deleteSpreadsheet(spreadsheetId);
          throw new GsheetException("Error occurred while creating the sheet.\n" + e);
        }
        List<String> permissions = new ArrayList<>();
        subscribers.forEach(subscriber ->
        {
          try {
            permissions.add(shareFile(spreadsheetId, subscriber));
          } catch (IOException e) {
            throw new GsheetException("Error occurred while sharing the sheet.\n" + e);
          }
        });
        log.info("Permissions given" + permissions);
        break;
      case OVERWRITE:
        spreadsheetId = gsheetInfo.getSpreadsheetId();
        try {
          updateValuesResponse = updateValues(spreadsheetId,
              gsheetInfo.getSheetName() + "!" + gsheetInfo.getStartCell(),
              DEFAULT_VALUE_INPUT_OPTION, dataEntry);
          int headerRow = Integer.parseInt(gsheetInfo.getStartCell().replaceAll("[^0-9]", ""));
          formatHeaderRow(spreadsheetId, headerRow, gsheetInfo.getSheetName());
        } catch (IOException e) {
          throw new GsheetException("Error occurred while editing the sheet.\n" + e);
        }
        break;
    }

    log.info("returning from uploadGsheet");
    return getSpreadSheetUrl(updateValuesResponse.getSpreadsheetId());
  }

  private static void formatHeaderRow(String spreadsheetId, int row, String sheetName) throws IOException {
    log.info("formatHeaderRow called");
    List<Request> requests = new ArrayList<>();
    try {
      Integer sheetId =
          sheetsService.spreadsheets().get(spreadsheetId).execute()
              .getSheets().stream().filter(sheet ->
              sheet.getProperties().getTitle().equals(sheetName)).findFirst()
              .get().getProperties().getSheetId();
      requests.add(new Request()
          .setRepeatCell(new RepeatCellRequest()
              .setRange(new GridRange()
                  .setSheetId(sheetId)
                  .setStartRowIndex(row - 1)
                  .setEndRowIndex(row))
              .setCell(new CellData()
                  .setUserEnteredFormat(new CellFormat()
                      .setBackgroundColor(new Color()
                          .setBlue((float) 0.7)
                          .setGreen((float) 0.7)
                          .setRed((float) 0.7))
                      .setTextFormat(new TextFormat()
                          .setBold(true))))
              .setFields("userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)")));

      BatchUpdateSpreadsheetRequest body =
          new BatchUpdateSpreadsheetRequest().setRequests(requests);
      sheetsService.spreadsheets().batchUpdate(spreadsheetId, body).execute();
    } catch (GoogleJsonResponseException e) {
      GoogleJsonError error = e.getDetails();
      if (error.getCode() == 404) {
        log.error("Spreadsheet not found with id '%s'.\n" + spreadsheetId);
      } else {
        throw new GsheetException(e.getMessage());
      }
    }
  }

  private static void addAlertMessage(String spreadsheetId, int row, int column) throws IOException {
    List<Request> requests = new ArrayList<>();
    try {
      long expiryTime = new Date().getTime() + gsheetExpiryInSeconds * 1000;
      final List<List<Object>> message =
          Arrays.asList(Arrays.asList(DEFAULT_ALERT_MESSAGE + new Date(expiryTime).toString()));
      updateValues(spreadsheetId, DEFAULT_ALERT_RANGE, DEFAULT_VALUE_INPUT_OPTION, message);
      requests.add(new Request()
          .setRepeatCell(new RepeatCellRequest()
              .setRange(new GridRange()
                  .setStartRowIndex(row - 1)
                  .setEndRowIndex(row)
                  .setStartColumnIndex(column - 1)
                  .setEndColumnIndex(column + 6))
              .setCell(new CellData()
                  .setUserEnteredFormat(new CellFormat()
                      .setBackgroundColor(new Color()
                          .setBlue((float) 0.6)
                          .setGreen((float) 1.0)
                          .setRed((float) 1.0))
                      .setTextFormat(new TextFormat()
                          .setBold(true)
                          .setForegroundColor(new Color()
                              .setBlue((float) 0.0)
                              .setGreen((float) 0.0)
                              .setRed((float) 1.0)))))
              .setFields("userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)")));

      requests.add(new Request()
          .setMergeCells(new MergeCellsRequest()
              .setRange(new GridRange()
                  .setStartRowIndex(row - 1)
                  .setEndRowIndex(row)
                  .setStartColumnIndex(column - 1)
                  .setEndColumnIndex(column + 6))
              .setMergeType("MERGE_ALL")));

      BatchUpdateSpreadsheetRequest body =
          new BatchUpdateSpreadsheetRequest().setRequests(requests);
      sheetsService.spreadsheets().batchUpdate(spreadsheetId, body).execute();
    } catch (GoogleJsonResponseException e) {
      GoogleJsonError error = e.getDetails();
      if (error.getCode() == 404) {
        log.error("Spreadsheet not found with id '%s'.\n" + spreadsheetId);
      } else {
        throw new GsheetException(e.getMessage());
      }
    }
  }

  private String createSpreadsheet(String sheetTitle, String folderId) {
    log.info("createSpreadsheet called");
    try {
      File fileSpec = new File();
      fileSpec.setName(sheetTitle);
      fileSpec.setParents(Collections.singletonList(folderId));
      fileSpec.setMimeType(DEFAULT_MIME_TYPE_GSHEET);
      log.info("File specification created: {}", fileSpec);

      File sheetFile = driveService.files()
          .create(fileSpec)
          .setSupportsAllDrives(true)
          .execute();
      log.info("Spreadsheet created with ID: {}", sheetFile.getId());

      sheetFile.setViewersCanCopyContent(true);
      sheetFile.setCopyRequiresWriterPermission(false);
      sheetFile.setWritersCanShare(true);

      log.info("Updated spreadsheet permissions: viewersCanCopyContent={}, copyRequiresWriterPermission={}, writersCanShare={}",
              sheetFile.getViewersCanCopyContent(),
              sheetFile.getCopyRequiresWriterPermission(),
              sheetFile.getWritersCanShare());

      driveService.files().update(sheetFile.getId(), sheetFile);
      log.info("Spreadsheet ID: " + sheetFile.getId());
      return sheetFile.getId();
    } catch (IOException e) {
      log.info("Error occurred while creating the spreadsheet with sheetTitle: {}, folderId: {}", sheetTitle, folderId, e);
      throw new GsheetException("Error occurred while creating the sheet.\n" + e);
    }
  }

  private static void deleteSpreadsheet(String fileId) {
    log.info("deleteSpreadsheet called");
    try {
       driveService.files()
          .delete(fileId)
          .execute();

      log.info("Spreadsheet ID: " + fileId + " deleted");
    } catch (IOException e) {
      throw new GsheetException("Error occurred while deleting the sheet.\n" + e);
    }
  }

  private String getSpreadSheetUrl(String spreadsheetId) {
    log.info("getSpreadSheetUrl called");
    StringBuilder spreadsheetUrl = new StringBuilder();
    spreadsheetUrl.append(DEFAULT_SPREADSHEET_URL).append(spreadsheetId);
    log.info("spreadsheetUrl.toString() : " + spreadsheetUrl.toString());
    return spreadsheetUrl.toString();
  }

  private static UpdateValuesResponse updateValues(String spreadsheetId,
                                                  String range,
                                                  String valueInputOption,
                                                  List<List<Object>> values)
      throws IOException {
    log.info("updateValues called");

    UpdateValuesResponse result = null;
    try {
      ValueRange body = new ValueRange()
          .setValues(values);
      result = sheetsService.spreadsheets().values().update(spreadsheetId, range, body)
          .setValueInputOption(valueInputOption)
          .execute();
      log.info("%d cells updated. " + result.getUpdatedCells());
    } catch (GoogleJsonResponseException e) {
      GoogleJsonError error = e.getDetails();
      if (error.getCode() == 404) {
        log.error("Spreadsheet not found with id '%s'.\n" + spreadsheetId);
      } else {
        throw new GsheetException("Error occurred while updating the sheet.\n" + e);
      }
    }
    return result;
  }

  private static String shareFile(String realFileId, String realUser)
      throws IOException {

    try {
      Permission userPermission = new Permission();
      userPermission.setRole("writer");
      userPermission.setType("user");
      userPermission.setEmailAddress(realUser);

      Permission permission = driveService.permissions()
          .create(realFileId, userPermission)
          .setSendNotificationEmail(false)
          .setSupportsAllDrives(true)
          .execute();

      return permission.getId();
    } catch (GoogleJsonResponseException e) {
      log.error("Unable to modify permission: " + e.getDetails());
      throw new GsheetException("Error occurred while sharing the sheet.\n" + e);
    }
  }

  private static HttpRequestInitializer authorize() throws IOException, GeneralSecurityException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault()
        .createScoped(SCOPES);
    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(
        credentials);

    return requestInitializer;
  }

  private static Sheets getSheetsService() throws IOException, GeneralSecurityException {
    HttpRequestInitializer requestInitializer = authorize();
    return new Sheets.Builder(new NetHttpTransport(),
        GsonFactory.getDefaultInstance(),
        requestInitializer)
        .setApplicationName(APPLICATION_NAME_SHEET)
        .build();
  }

  private static Drive getDriveService() throws IOException, GeneralSecurityException {
    HttpRequestInitializer requestInitializer = authorize();
    return new Drive.Builder(new NetHttpTransport(),
        GsonFactory.getDefaultInstance(),
        requestInitializer)
        .setApplicationName(APPLICATION_NAME_DRIVE)
        .build();
  }
  }