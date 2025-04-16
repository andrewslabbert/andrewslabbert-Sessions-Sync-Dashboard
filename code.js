/*******************************************************************************
 * Sessions Sync Dashboard (Phase 1: Sheet Sync) - v1.1 Simple Lookup
 * Description: Fetches data from the Airtable 'Sessions' table, relying on
 *              Airtable LOOKUP fields for linked data display names. Performs
 *              an incremental sync to a dedicated Google Sheet ('sessions').
 *              Change detection uses the 'publish_timestamp' field.
 *              Uses Airtable's native Record ID as the unique identifier.
 * Based On:    Partners Sync Dashboard v2.0 structure.
 * Version:     1.1 - Sessions (Lookup Field Strategy)
 *******************************************************************************/

/********************************************************
 * Global Configuration
 ********************************************************/

// --- Retrieve Airtable API Token from Script Properties ---
const AIRTABLE_API_TOKEN = PropertiesService.getScriptProperties().getProperty('AIRTABLE_API_TOKEN');
if (!AIRTABLE_API_TOKEN) {
  Logger.log("ERROR: AIRTABLE_API_TOKEN not found in Script Properties. Please run storeSecrets() or set it manually.");
  throw new Error("Missing required Script Property: AIRTABLE_API_TOKEN");
}

// --- Airtable Configuration ---
const AIRTABLE_BASE_ID_SESSIONS = 'apphmPNiTdmcknmfs'; // Your Sessions Base ID
const AIRTABLE_TABLE_SESSIONS_ID = 'tblnvqOHWXe5VQanu'; // Your Sessions Table ID

// --- Google Sheet Configuration ---
const SESSIONS_SHEET_NAME = "sessions";

// --- Script Configuration ---
const MAX_FETCH_RETRIES = 3;
const BASE_RETRY_DELAY_MS = 500;
const INTER_PAGE_DELAY_MS = 200;

/********************************************************
 * Workflow Configuration Object (Sessions Only - Simple)
 ********************************************************/
const CONFIG = {
  sessions: {
    type: 'sessions',
    airtable: {
      baseId: AIRTABLE_BASE_ID_SESSIONS,
      tableId: AIRTABLE_TABLE_SESSIONS_ID,
      viewName: 'Web', // Optional: Specify an Airtable view name if needed
      fields: [
         // **** ACTION: Verify/Edit this list to match desired sheet columns ****
         'title',
         'session_sku',
         'id', // Assuming this is a specific ID field from Airtable, not the native one
         'slug',
         'session_description',
         'excerpt',
         'date_short',
         'date_long',
         'featured_image_link',
         'listing_image_link',
         'no_words_image_link',
         'banner_image_link',
         'series_category', // Keep if needed, ensure it's text or a lookup if needed
         'topics_title',      // INCLUDE the Lookup field for Topics
         'speaker_title',     // INCLUDE the Lookup field for Speaker/Author
         'youtube_link',
         'pdf_image_1',
         'pdf_title_1',
         'pdf_title_2',
         'spotify_podcast',
         'apple_podcast',
         'series_title',      // INCLUDE the Lookup field for Series
         'series_sku',
         '_aioseo_description',
         'permalink',
         'website_status',
         'last_modified',     // Keep if needed
         'publish_timestamp', // Timestamp field MUST be listed here
         'publish_status',
         'session_description_cleanup',
         'series_type',
         'global_categories', // Keep if needed, ensure it's text or a lookup if needed
         'alt_link',
         'pdf_link_1',
         'pdf_image_2',
         'pdf_link_2'
         // REMOVED: 'topics', 'speaker', 'series' (Original Linked Record fields)
         // Assuming you only want the titles via the Lookup fields above.
         // Add them back ONLY if you need the raw Airtable Record IDs (e.g., "rec...") in the sheet.
      ],
      // recordIdField: null, // Set to null/remove as we use the native Airtable ID
      timestampField: 'publish_timestamp', // Field used for detecting changes
      // linkedFields: {} // REMOVED - Not needed with Lookup fields
    },
    sheetName: SESSIONS_SHEET_NAME,
    syncFunctionName: 'syncAirtableToSheet' // Uses the generic sync function
  }
};

/********************************************************
 * Secret Management (Run Once)
 * Stores sensitive information in Script Properties.
 ********************************************************/
function storeSecrets() {
  try {
    // !! IMPORTANT !! Replace with your actual API Token before running MANUALLY from the editor!
    const tokenToStore = 'PASTE_YOUR_AIRTABLE_API_TOKEN_HERE'; // <--- PASTE TOKEN HERE FOR MANUAL RUN

    if (tokenToStore === 'PASTE_YOUR_AIRTABLE_API_TOKEN_HERE' || !tokenToStore) {
        Logger.log("WARN: Please paste your actual Airtable API token into the script before running storeSecrets().");
        return;
    }

    PropertiesService.getScriptProperties().setProperty('AIRTABLE_API_TOKEN', tokenToStore);
    Logger.log("Secret 'AIRTABLE_API_TOKEN' stored successfully in Script Properties.");

  } catch (e) {
    Logger.log("ERROR storing secrets: " + e);
    throw new Error("Failed to store secrets. Check logs and script permissions.");
  }
}

/********************************************************
 * Top-Level Function (Runs sync for all configured types - currently just Sessions)
 * Calls the main sync function for each configured entity.
 ********************************************************/
function runFullSync() {
  Logger.log("--- Starting Full Airtable to Sheets Sync ---");
  var overallStatus = true;
  var masterLog = ["Sync Run Started: " + new Date().toLocaleString()];
  var recentItemsCollector = []; // Collects summary actions across all syncs

  // Simple logger function for this run
  function addLog(msg) {
    masterLog.push(msg);
    Logger.log(msg); // Log to standard GAS logger
  }

  // Loop through each configuration in CONFIG (currently only 'sessions')
  for (var key in CONFIG) {
    if (CONFIG.hasOwnProperty(key)) {
      var config = CONFIG[key];
      var syncFunctionName = config.syncFunctionName || 'syncAirtableToSheet'; // Default to generic

      addLog("\n--- Processing: " + config.type.toUpperCase() + " (Sheet: " + config.sheetName + ") ---");

      try {
        // Dynamically call the function specified in config
        var result = this[syncFunctionName](config, addLog, recentItemsCollector);

        addLog("Result for " + config.type.toUpperCase() + ": " + (result.success ? "SUCCESS" : "FAILED"));
        if (result.error) {
          addLog("ERROR: " + result.error); // Log the specific error reported by the sync function
          overallStatus = false;
        }
        addLog("Counters: " + JSON.stringify(result.counters)); // Log the summary counters

      } catch (e) {
        // Catch unexpected errors during the execution of the sync function itself
        addLog("FATAL ERROR during " + config.type + " sync: " + e.message + (e.stack ? "\nStack: " + e.stack : ""));
        overallStatus = false;
      }
    }
  }

  Logger.log("\n--- Recent Actions Summary (" + recentItemsCollector.length + " items) ---");
  recentItemsCollector.forEach(item => Logger.log("- " + item)); // Log summary actions

  Logger.log("--- Full Sync Run Complete ---");
  Logger.log("Overall Status: " + (overallStatus ? "SUCCESS" : "FAILED (Check logs for details)"));
  // Optional: Could write masterLog array to a dedicated log sheet here if needed
}


/********************************************************
 * Helper: Fetch Airtable Data with Retries
 * Fetches all records for a given table/view/fields config.
 ********************************************************/
function fetchAirtableData_(apiUrl, fieldsToFetch, viewName) {
  // Uses global AIRTABLE_API_TOKEN defined at the top
  var allRecords = [];
  var offset = null;
  var urlParams = ['pageSize=100'];
  var apiEndpoint = apiUrl; // Keep original API URL for logging

  if (viewName) {
     urlParams.push('view=' + encodeURIComponent(viewName));
  }
  // Ensure fieldsToFetch is an array and has elements before adding
  if (Array.isArray(fieldsToFetch) && fieldsToFetch.length > 0) {
    // Filter out any potential RecordID formula field names if they were accidentally included
    const fieldsParam = fieldsToFetch.filter(f => f && !f.toLowerCase().includes('recordid'));
    if (fieldsParam.length > 0) {
        // Correctly format fields for URL query using fields[]=...
        fieldsParam.forEach(field => {
            urlParams.push('fields[]=' + encodeURIComponent(field));
        });
    }
  }

  var options = {
    method: 'get',
    contentType: 'application/json',
    headers: { 'Authorization': 'Bearer ' + AIRTABLE_API_TOKEN },
    muteHttpExceptions: true // Allows capturing 4xx/5xx responses without throwing an immediate error
  };

  var pageCount = 0;

  do {
    var currentUrl = apiUrl + '?' + urlParams.join('&');
    if (offset) {
      // Ensure offset is added correctly
      currentUrl += '&offset=' + encodeURIComponent(offset);
    }
    pageCount++;

    var response, jsonData, responseCode, responseText;
    var retryCount = 0;
    var success = false;

    while (!success && retryCount < MAX_FETCH_RETRIES) {
      try {
        response = UrlFetchApp.fetch(currentUrl, options);
        responseCode = response.getResponseCode();
        responseText = response.getContentText();

        if (responseCode === 200) {
          jsonData = JSON.parse(responseText);
          success = true;
        } else if (responseCode === 429) { // Rate Limit
          var delay = BASE_RETRY_DELAY_MS * Math.pow(2, retryCount);
          Logger.log("WARN [fetchAirtableData_]: Rate limited (429) fetching page " + pageCount + ". Waiting " + delay + "ms before retry " + (retryCount + 1));
          Utilities.sleep(delay);
          retryCount++;
        } else { // Other HTTP errors
          var delay = BASE_RETRY_DELAY_MS * Math.pow(2, retryCount);
          Logger.log("WARN [fetchAirtableData_]: HTTP Error " + responseCode + " fetching page " + pageCount + ". Waiting " + delay + "ms before retry " + (retryCount + 1) + ". Response snippet: " + responseText.substring(0, 200));
          Utilities.sleep(delay);
          retryCount++;
        }
      } catch (err) { // Network or parsing errors
        var delay = BASE_RETRY_DELAY_MS * Math.pow(2, retryCount);
         Logger.log("WARN [fetchAirtableData_]: Network/Fetch Error on page " + pageCount + ". Retrying in "+delay+"ms... Error: " + err.message);
        if (retryCount < MAX_FETCH_RETRIES - 1) {
           Utilities.sleep(delay);
          retryCount++;
        } else {
          // Throw error only after all retries fail
          Logger.log("ERROR [fetchAirtableData_]: Failed to fetch data after " + MAX_FETCH_RETRIES + " attempts for " + apiEndpoint + ". Last error: " + err.message);
          throw new Error("Failed to fetch data after " + MAX_FETCH_RETRIES + " attempts. URL: " + currentUrl + " Last error: " + err.message);
        }
      }
    } // End retry loop

    if (!success) {
      // If loop finishes without success (e.g., persistent non-429 error after retries)
      Logger.log("ERROR [fetchAirtableData_]: Failed definitively fetching data for " + apiEndpoint + ". Last code: " + responseCode + " URL: " + currentUrl);
      throw new Error("Failed definitively fetching data. Last code: " + responseCode + " URL: " + currentUrl);
    }

    if (jsonData.records && jsonData.records.length > 0) {
      allRecords = allRecords.concat(jsonData.records);
    }
    offset = jsonData.offset;
    // Pause between pages to be kind to the API
    if (offset) Utilities.sleep(INTER_PAGE_DELAY_MS);

  } while (offset);

  Logger.log("INFO [fetchAirtableData_]: Successfully fetched " + allRecords.length + " total records for " + apiEndpoint + (viewName ? " (view: " + viewName + ")" : "") + ".");
  return allRecords;
}

/********************************************************
 * Helper: Format Field Value Consistently (SIMPLE VERSION)
 * Handles basic type conversions. No special linked field logic needed.
 * Relies on Airtable Lookups providing text values directly.
 ********************************************************/
function formatFieldValue_(value) {
    if (value === null || typeof value === 'undefined') {
        return '';
    }
    // Handle Arrays (e.g., Lookups returning multiple values, Multi-selects, Attachments, Collaborators)
    if (Array.isArray(value)) {
        // Handle Attachments (array of objects with url)
        if (value.length > 0 && typeof value[0] === 'object' && value[0] !== null && value[0].hasOwnProperty('url')) {
            return value.map(att => att.url).join(', '); // Join attachment URLs
        }
        // Handle other arrays (simple values, collaborators, lookups returning multiple)
        else if (value.length > 0) {
             return value.map(item => {
               // Handle collaborator objects within the array
               if(typeof item === 'object' && item !== null) {
                  if (item.hasOwnProperty('name')) return item.name; // Collaborator name
                  if (item.hasOwnProperty('email')) return item.email; // Collaborator email
                  // Fallback for other unexpected objects in array
                  try { return JSON.stringify(item); } catch(e) { return '[Object]'; }
               }
               // Convert simple values (strings, numbers) to string
               return String(item);
             }).join(','); // Join array elements with a comma
        } else {
             return ''; // Return empty string for empty array
        }
    }
    // Handle Booleans
    else if (typeof value === 'boolean') {
        return value ? 'TRUE' : 'FALSE'; // Sheet compatible boolean
    }
    // Handle single Collaborator object (if not in an array)
    else if (typeof value === 'object') {
         if (value.hasOwnProperty('name')) { return value.name; }
         if (value.hasOwnProperty('email')) { return value.email; }
         // Fallback for other unexpected objects
         try { return JSON.stringify(value); } catch (e) { return '[Object]'; }
    }
    // Handle ISO Date String Formatting
    else if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) {
       try {
         return Utilities.formatDate(new Date(value), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
       } catch (e) {
         Logger.log("WARN [formatFieldValue_]: Date formatting error for value '" + value + "'. Returning original. Error: " + e);
         return value; // Keep original if formatting fails
       }
    }
    // Default: convert anything else to string
    return String(value);
}

/********************************************************
 * Helper: Standardize Timestamp for Comparison
 * Converts various inputs (Date object, ISO string, potentially sheet formatted string)
 * into a consistent "yyyy-MM-dd HH:mm:ss" format or empty string if invalid/null.
 ********************************************************/
function standardizeTimestampForComparison_(timestampValue, recordIdForLog, sourceInfo) {
    var formattedTimestamp = '';
    const scriptTimeZone = Session.getScriptTimeZone();

    if (!timestampValue) { return ''; } // Handle null, undefined, empty string

    if (timestampValue instanceof Date) {
        if (!isNaN(timestampValue.valueOf())) { // Check valid Date object
             try {
                formattedTimestamp = Utilities.formatDate(timestampValue, scriptTimeZone, "yyyy-MM-dd HH:mm:ss");
            } catch (ex) {
                Logger.log(`WARN [standardizeTimestamp]: Error formatting Date object from ${sourceInfo} (ID: ${recordIdForLog}). Value: ${timestampValue}. Error: ${ex.message}`);
            }
        } else {
             Logger.log(`WARN [standardizeTimestamp]: Invalid Date object encountered from ${sourceInfo} (ID: ${recordIdForLog}).`);
        }
    } else if (typeof timestampValue === 'string' && timestampValue.trim() !== '') {
        let trimmedValue = timestampValue.trim();
        try {
            // Check if already in target format (e.g., from previous script run or formatted date string)
            if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(trimmedValue)) {
                formattedTimestamp = trimmedValue;
            }
             // Check for ISO format (output by formatFieldValue_)
            else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(trimmedValue)) {
                var parsedDate = new Date(trimmedValue);
                 if (!isNaN(parsedDate.valueOf())) {
                     formattedTimestamp = Utilities.formatDate(parsedDate, scriptTimeZone, "yyyy-MM-dd HH:mm:ss");
                 } else {
                     Logger.log(`WARN [standardizeTimestamp]: Failed to parse potential ISO date string from ${sourceInfo} (ID: ${recordIdForLog}). Value: '${trimmedValue}'.`);
                     formattedTimestamp = ''; // Treat as non-comparable
                 }
            } else {
                // Attempt to parse other common formats if necessary
                var parsedDate = new Date(trimmedValue);
                if (!isNaN(parsedDate.valueOf())) {
                    formattedTimestamp = Utilities.formatDate(parsedDate, scriptTimeZone, "yyyy-MM-dd HH:mm:ss");
                } else {
                   // Log failure to parse, timestamp comparison might trigger update unnecessarily
                   Logger.log(`WARN [standardizeTimestamp]: Failed to parse date string from ${sourceInfo} (ID: ${recordIdForLog}). Value: '${trimmedValue}'.`);
                   formattedTimestamp = ''; // Treat as non-comparable / different
                }
            }
        } catch (ex) {
             Logger.log(`WARN [standardizeTimestamp]: Error parsing/formatting date string from ${sourceInfo} (ID: ${recordIdForLog}). Value: '${trimmedValue}'. Error: ${ex.message}`);
             formattedTimestamp = ''; // Treat as non-comparable / different
        }
    } else if (typeof timestampValue === 'number') {
        // Handle potential epoch timestamps or Google Sheets date numbers if necessary
        Logger.log(`WARN [standardizeTimestamp]: Unexpected numeric timestamp from ${sourceInfo} (ID: ${recordIdForLog}). Value: ${timestampValue}. Attempting direct conversion.`);
        try {
            let dateFromNum = new Date(timestampValue); // Might be epoch ms or need conversion if Sheets date number
             if (!isNaN(dateFromNum.valueOf())) {
                  formattedTimestamp = Utilities.formatDate(dateFromNum, scriptTimeZone, "yyyy-MM-dd HH:mm:ss");
             }
        } catch (ex) {
             Logger.log(`WARN [standardizeTimestamp]: Error converting numeric timestamp. Error: ${ex.message}`);
        }
    }
    return formattedTimestamp; // Returns "yyyy-MM-dd HH:mm:ss" or ''
}

/********************************************************
 * Helper: Capitalize First Letter
 * Utility function for formatting log messages.
 ********************************************************/
function capitalizeFirstLetter(string) {
  if (!string) return '';
  return string.charAt(0).toUpperCase() + string.slice(1);
}


/*******************************************************************************
 * syncAirtableToSheet (CORE FUNCTION - Simplified - v1.3 Empty Sheet Fix)
 * Fetches data, processes using simple formatting, and performs incremental
 * sync to the target Google Sheet using native Airtable Record ID.
 * Correctly handles writing data to an empty sheet.
 * PRIORITIZES 'title' field for user-facing logs (recentItems).
 *******************************************************************************/
function syncAirtableToSheet(config, addLog, recentItems) {
    var counters = { updated: 0, skipped: 0, created: 0, deleted: 0 };
    var logArray = [];

    // --- Internal logging helper --- (No changes needed)
    function logEntry(msg) {
        var timeStamped = "[" + new Date().toLocaleTimeString() + "] ";
        var prefix = "[" + (config.type || 'SYNC').toUpperCase() + "] ";
        var fullMsg = timeStamped + prefix + msg;
        logArray.push(fullMsg);
        if (addLog && typeof addLog === 'function') {
            addLog(fullMsg);
        } else {
            Logger.log(fullMsg);
        }
    }

    // --- Configuration Validation --- (No changes needed)
    const primaryTitleField = 'title'; // <-- Ensure this matches your title field name
    if (!config || !config.airtable || !config.airtable.baseId || !config.airtable.tableId || !config.airtable.timestampField || !config.sheetName || !Array.isArray(config.airtable.fields)) { /* ... */ return { success: false, error: "Config error...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }
    if (!config.airtable.fields.includes(config.airtable.timestampField)) { /* ... */ return { success: false, error: "Timestamp field missing...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }
    if (!config.airtable.fields.includes(primaryTitleField)) { logEntry(`WARN: The primary title field '${primaryTitleField}' is not listed in config.airtable.fields...`); }

    logEntry("INFO: Starting sync to sheet '" + config.sheetName + "'...");
    var airtableRecords = [];

    try {
        // --- 1. Fetch Airtable Data --- (No changes needed)
        logEntry("INFO: Fetching data from Airtable table: " + config.airtable.tableId);
        var apiUrl = 'https://api.airtable.com/v0/' + config.airtable.baseId + '/' + encodeURIComponent(config.airtable.tableId);
        airtableRecords = fetchAirtableData_(apiUrl, config.airtable.fields, config.airtable.viewName);
    } catch (fetchErr) { /* ... Error handling ... */ return { success: false, error: "Fetch error...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }

    // --- 2. Define Target Header and Process Data --- (No changes needed)
    logEntry("INFO: Processing " + airtableRecords.length + " fetched records...");
    var newData = [];
    var targetHeader = ["AirtableRecordID"];
    var configuredFieldsSet = new Set(config.airtable.fields);
    const orderedFields = config.airtable.fields.filter(f => configuredFieldsSet.has(f));
    targetHeader = targetHeader.concat(orderedFields); // Concatenate in the config order
    newData.push(targetHeader);
    logEntry("INFO: Target header defined with " + targetHeader.length + " columns: " + targetHeader.join(', '));
    const targetHeaderIndexMap = targetHeader.reduce((map, header, index) => { map[header] = index; return map; }, {});
    const recordIdColIndex_Target = 0;
    const timestampColIndex_Target = targetHeaderIndexMap[config.airtable.timestampField];
    const titleColIndex_Target = targetHeaderIndexMap[primaryTitleField];
    if (timestampColIndex_Target === undefined) { /* ... Validation ... */ return { success: false, error: "Timestamp index error...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }
    if (titleColIndex_Target === undefined) { logEntry(`WARN: Cannot find index for primary title field '${primaryTitleField}' in target header...`); }
    airtableRecords.forEach(function (record, index) { /* ... Processing loop ... */ var fields = record.fields || {}; var airtableNativeId = record.id; if (!airtableNativeId) { return; } var newRowArray = targetHeader.map(headerName => { if (headerName === "AirtableRecordID") return airtableNativeId; else return formatFieldValue_(fields[headerName]); }); newData.push(newRowArray); });
    logEntry("INFO: Finished processing. New data structure has " + (newData.length - 1) + " data rows.");

    // --- 3. Sync with Google Sheet ---
    if (newData.length <= 1) { /* ... No data check ... */ return { success: true, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }

    // Get sheet handle, create if needed
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName(config.sheetName);
    var sheetCreated = false;
    if (!sheet) { try { sheet = ss.insertSheet(config.sheetName); sheetCreated = true; logEntry("INFO: Created new sheet: " + config.sheetName); } catch (e) { /*...*/ return { success: false, error: "Sheet creation error...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; } }

    // Read existing data
    var existingData = [];
    var existingHeader = [];
    if (!sheetCreated && sheet.getLastRow() > 0) {
        try {
            existingData = sheet.getDataRange().getValues();
            existingHeader = existingData[0].map(String); // Get header only if data exists
            logEntry("INFO: Fetched " + existingData.length + " existing rows (incl. header) from sheet '" + config.sheetName + "'.");
        } catch (e) { /*...*/ return { success: false, error: "Sheet read error...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }
    } else if (!sheetCreated) {
        logEntry("INFO: Sheet '" + config.sheetName + "' exists but is empty.");
    }

    // --- Determine Sync Strategy: Full Write or Incremental ---
    var performFullWrite = false;
    if (sheetCreated || existingData.length === 0) {
        performFullWrite = true;
        logEntry("INFO: Sheet is new or empty. Performing full data write.");
    } else {
        // Compare headers if sheet wasn't empty
        var newHeader = targetHeader.map(String);
        if (JSON.stringify(existingHeader) !== JSON.stringify(newHeader)) {
            performFullWrite = true;
            logEntry("WARN: Headers differ. Performing full data rewrite.");
            logEntry("DEBUG Existing Header: " + existingHeader.join(', '));
            logEntry("DEBUG New Header:      " + newHeader.join(', '));
            counters.deleted = existingData.length - 1; // Log conceptual deletion
        } else {
            logEntry("INFO: Headers match. Performing incremental sync...");
        }
    }

    // --- Execute Full Write (if needed) ---
    if (performFullWrite) {
        try {
            // Clear existing content and resize before writing
            sheet.clearContents();
            sheet.setFrozenRows(0); // Unfreeze before resizing
            SpreadsheetApp.flush(); // Try to ensure clear completes

            const requiredRows = newData.length;
            const requiredCols = targetHeader.length;

            // Adjust rows
            const currentMaxRows = sheet.getMaxRows();
            if (currentMaxRows < requiredRows) {
                sheet.insertRowsAfter(currentMaxRows, requiredRows - currentMaxRows);
            } else if (currentMaxRows > requiredRows && requiredRows > 0) {
                 // Check if requiredRows > 0 before deleting
                 // Avoid error if newData only has header row (requiredRows = 1)
                 if (currentMaxRows > 1 || requiredRows == 0) { // Ensure we don't delete the only row if requiredRows is 1 or 0
                     sheet.deleteRows(requiredRows + 1, currentMaxRows - requiredRows);
                 } else if (requiredRows === 1 && currentMaxRows > 1){
                     // If new data has only header, delete all rows after 1
                      sheet.deleteRows(2, currentMaxRows - 1);
                 }
            } else if (requiredRows == 0 && currentMaxRows > 0){
                 // If new data is empty, clear everything
                  sheet.deleteRows(1, currentMaxRows);
            }


            // Adjust columns
            const currentMaxCols = sheet.getMaxColumns();
            if (currentMaxCols < requiredCols) {
                sheet.insertColumnsAfter(currentMaxCols, requiredCols - currentMaxCols);
            } else if (currentMaxCols > requiredCols && requiredCols > 0) {
                sheet.deleteColumns(requiredCols + 1, currentMaxCols - requiredCols);
            } else if (requiredCols == 0 && currentMaxCols > 0){
                 sheet.deleteColumns(1, currentMaxCols);
            }


            // Write new data (only if there's data to write)
            if (requiredRows > 0 && requiredCols > 0) {
                sheet.getRange(1, 1, requiredRows, requiredCols).setValues(newData);
                sheet.setFrozenRows(1); // Re-freeze header
            } else {
                 logEntry("INFO: No data to write after clearing/resizing.");
            }

            counters.created = newData.length - 1; // All non-header rows are new
            const writeAction = (sheetCreated || existingData.length === 0) ? "written to new/empty sheet" : "rewritten sheet";
            logEntry(`INFO: Data ${writeAction} successfully. ${counters.created} records created.`);
            if (recentItems && typeof recentItems.push === 'function') recentItems.push(`${sheetCreated ? "Created" : (existingData.length === 0 ? "Populated empty" : "Rewrote")} sheet '${config.sheetName}' with ${counters.created} records.`);

        } catch (e) {
            logEntry(`ERROR ${performFullWrite && !sheetCreated && existingData.length > 0 ? 'rewriting sheet' : 'writing to new/empty sheet'} '${config.sheetName}': ${e.message}${e.stack ? " | Stack: " + e.stack : ""}`);
            if (recentItems && typeof recentItems.push === 'function') recentItems.push(`Sync Error (${config.type}): Failed ${performFullWrite ? 'rewriting' : 'writing'} sheet.`);
            return { success: false, error: `Failed ${performFullWrite ? 'rewriting' : 'writing'} data to '${config.sheetName}': ${e.message}`, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
        }
        // If full write was successful, return
        return { success: true, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }

    // --- Execute Incremental Update (only if not a full write) ---
    const sheetRecordIdIndex = 0;
    const sheetTimestampIndex = existingHeader.indexOf(config.airtable.timestampField);
    const sheetTitleIndex = existingHeader.indexOf(primaryTitleField);
    if (sheetTimestampIndex === -1) { /* ... Validation ... */ return { success: false, error: "Timestamp index error in sheet...", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; }
    if (sheetTitleIndex === -1) { logEntry(`WARN: Cannot find index for primary title field '${primaryTitleField}' in sheet header...`); }

    // Build Map of Existing Records
    var existingMap = {};
    for (var i = 1; i < existingData.length; i++) { var existingRow = existingData[i]; if (existingRow.length <= sheetRecordIdIndex || existingRow.length <= sheetTimestampIndex) continue; var recID = existingRow[sheetRecordIdIndex]; if (recID && String(recID).trim() !== '') { var standardizedExistingTimestamp = standardizeTimestampForComparison_(existingRow[sheetTimestampIndex], recID, `sheet row ${i+1}`); existingMap[recID] = { rowIndex: i + 1, timestamp: standardizedExistingTimestamp }; } }
    logEntry("INFO: Built map of " + Object.keys(existingMap).length + " existing records from sheet for comparison.");

    // Compare New Data and Prepare Batches
    var rowsToUpdate = []; var rowsToAppend = []; var recordIdsToKeep = new Set();
    const entityTypeCapitalized = capitalizeFirstLetter(config.type);
    for (var i = 1; i < newData.length; i++) { /* ... Comparison loop - Uses titleColIndex_Target for logging ... */ var newRow = newData[i]; var newRecordID = newRow[recordIdColIndex_Target]; if (!newRecordID || String(newRecordID).trim() === '') continue; recordIdsToKeep.add(newRecordID); var standardizedNewTimestamp = standardizeTimestampForComparison_(newRow[timestampColIndex_Target], newRecordID, `new data row ${i}`); var recordTitleForLog = (titleColIndex_Target !== undefined && newRow.length > titleColIndex_Target && newRow[titleColIndex_Target]) ? newRow[titleColIndex_Target] : newRecordID; var existingRecord = existingMap[newRecordID]; if (existingRecord) { if (existingRecord.timestamp !== standardizedNewTimestamp) { var rangeNotation = sheet.getRange(existingRecord.rowIndex, 1, 1, targetHeader.length).getA1Notation(); rowsToUpdate.push({ range: rangeNotation, values: [newRow] }); counters.updated++; if (recentItems && recentItems.length < 150) recentItems.push(`Updated ${entityTypeCapitalized}: '${recordTitleForLog}'`); logEntry(`INFO: Marked row ${existingRecord.rowIndex} (${newRecordID} - ${recordTitleForLog}) for update...`); } else { counters.skipped++; } } else { rowsToAppend.push(newRow); counters.created++; if (recentItems && recentItems.length < 150) recentItems.push(`Added ${entityTypeCapitalized}: '${recordTitleForLog}'`); logEntry(`INFO: Marked record ${newRecordID} (${recordTitleForLog}) for creation.`); } }

    // Determine Rows to Delete
    var rowsToDeleteIndices = [];
    for (var recID in existingMap) { /* ... Delete logic - Uses sheetTitleIndex for logging ... */ if (!recordIdsToKeep.has(recID)) { let existingInfo = existingMap[recID]; rowsToDeleteIndices.push(existingInfo.rowIndex); counters.deleted++; var deletedTitle = recID; try { if (existingInfo.rowIndex > 0 && existingInfo.rowIndex <= sheet.getLastRow() && sheetTitleIndex !== -1 && (sheetTitleIndex < sheet.getLastColumn())) { var titleValue = sheet.getRange(existingInfo.rowIndex, sheetTitleIndex + 1).getValue(); if (titleValue && String(titleValue).trim() !== '') deletedTitle = String(titleValue).trim(); } } catch(fetchErr) { /* log warning */ } if (recentItems && recentItems.length < 150) recentItems.push(`Removed ${entityTypeCapitalized}: '${deletedTitle}'`); logEntry(`INFO: Marked row ${existingInfo.rowIndex} (${recID} - ${deletedTitle}) for deletion...`); } }
    rowsToDeleteIndices.sort((a, b) => b - a);
    logEntry("INFO: Sync Analysis complete. Update: " + counters.updated + ", Create: " + counters.created + ", Delete: " + counters.deleted + ", Skipped: " + counters.skipped);

    // Perform Batch Operations
    var updateError = null, appendError = null, deleteError = null;
    var operationsPerformed = false;
    /* ... Batch update logic ... */ if (rowsToUpdate.length > 0) { operationsPerformed = true; try { rowsToUpdate.forEach(update => { sheet.getRange(update.range).setValues(update.values); }); logEntry("INFO: " + rowsToUpdate.length + " updates applied."); } catch (e) { logEntry("ERROR updates: " + e.message); updateError = e; if (recentItems) recentItems.push("Sync Error (Updates)"); } }
    /* ... Batch append logic ... */ if (rowsToAppend.length > 0) { operationsPerformed = true; try { var startRow = sheet.getLastRow() + 1; let requiredEndRow = startRow + rowsToAppend.length - 1; if(sheet.getMaxRows() < requiredEndRow ) sheet.insertRowsAfter(sheet.getMaxRows(), requiredEndRow - sheet.getMaxRows()); if (sheet.getMaxColumns() < targetHeader.length) sheet.insertColumnsAfter(sheet.getMaxColumns(), targetHeader.length - sheet.getMaxColumns()); sheet.getRange(startRow, 1, rowsToAppend.length, targetHeader.length).setValues(rowsToAppend); logEntry("INFO: " + rowsToAppend.length + " appends applied."); } catch (e) { logEntry("ERROR appends: " + e.message); appendError = e; if (recentItems) recentItems.push("Sync Error (Appends)"); } }
    /* ... Batch delete logic ... */ if (rowsToDeleteIndices.length > 0) { operationsPerformed = true; try { rowsToDeleteIndices.forEach(function(rowIndex) { if (rowIndex > 0 && rowIndex <= sheet.getLastRow()) { sheet.deleteRow(rowIndex); } else { logEntry("WARN: Skipped deletion index " + rowIndex); } }); logEntry("INFO: " + rowsToDeleteIndices.length + " deletes applied."); } catch (e) { logEntry("ERROR deletes: " + e.message); deleteError = e; if (recentItems) recentItems.push("Sync Error (Deletes)"); } }

    // Final Logging & Return
    if (counters.skipped > 0) logEntry("INFO: Skipped " + counters.skipped + " records (timestamp matched).");
    if (!operationsPerformed && counters.skipped > 0) { logEntry("INFO: No changes needed for sheet '" + config.sheetName + "'."); if (recentItems && recentItems.length < 150) recentItems.push(config.type + ": No changes detected (" + counters.skipped + " checked)."); }
    if (recentItems && recentItems.length >= 150 && !recentItems.some(item => item.startsWith("..."))) recentItems.push("... (Action summary list truncated)");
    logEntry("INFO: Sync completed. Final Counts: " + JSON.stringify(counters));
    var overallSuccess = !updateError && !appendError && !deleteError;
    var errorMessages = [updateError, appendError, deleteError].filter(Boolean).map(e => config.type + ": " + e.message);
    var combinedErrorMessage = errorMessages.join('; ');

    return { success: overallSuccess, error: overallSuccess ? null : combinedErrorMessage, counters: counters, recentItems: recentItems, log: logArray.join("\n") };
}

/********************************************************
 * Web App Interface & Dashboard Functions (Keep for Phase 2)
 * These functions provide the backend for a web-based UI.
 ********************************************************/

/**
 * Serves the HTML for the Web App (Phase 2).
 * Needs a file named 'dashboard.html' in the project.
 * @param {Object} e The event parameter for a web app doGet request.
 * @return {HtmlOutput} The HTML service object.
 */
function doGet(e) {
  try {
    Logger.log("doGet triggered for Dashboard.");
    // Assumes you will create a dashboard.html file later for Phase 2
    var template = HtmlService.createTemplateFromFile('dashboard');
    // You could pass initial data to the template here if needed:
    // template.initialData = getDashboardData();
    var htmlOutput = template.evaluate()
      .setTitle('Sessions Sync Dashboard') // Updated Title for this project
      .setSandboxMode(HtmlService.SandboxMode.IFRAME) // Recommended sandbox mode
      .addMetaTag('viewport', 'width=device-width, initial-scale=1'); // For mobile responsiveness

    Logger.log("Successfully created HTML output for Dashboard.");
    return htmlOutput;

  } catch (error) {
    Logger.log("ERROR in doGet: " + error + ". Did you create dashboard.html?");
    // Provide a fallback error page if dashboard.html is missing or fails
    return HtmlService.createHtmlOutput(
        "<h1>Error</h1><p>The dashboard UI could not be loaded.</p><p>Ensure the file 'dashboard.html' exists in this Apps Script project.</p><p>Error: " +
        Utilities.encodeHtml(error.message) + "</p>")
      .setTitle('Dashboard Error');
  }
}

/**
 * Retrieves the last sync status and results for the dashboard UI (Phase 2).
 * Reads data stored in User Properties.
 * @return {Object} An object containing the last sync details or a default state.
 */
function getDashboardData() {
  Logger.log("getDashboardData called.");
  // Use a unique property key for this project to avoid conflicts
  const storedData = PropertiesService.getUserProperties().getProperty('lastSyncDashboardData_Sessions');
  if (storedData) {
    try {
      Logger.log("Found stored dashboard data.");
      return JSON.parse(storedData);
    } catch (e) {
      Logger.log("Error parsing stored dashboard data: " + e);
      // Fall through to return default state on parsing error
    }
  }

  Logger.log("No valid stored data found, returning default state.");
  // Return a default structure if no data is stored yet
  return {
    lastSyncTimestamp: null,
    lastSyncStatus: 'Never Run',
    lastSyncDuration: 0,
    lastSyncResults: {
      typeCounters: {}, // Will only contain 'sessions' key after first run
      totalErrors: 0,
      // Log format assumes dashboard.html uses Feather Icons
      logs: ["<li><i data-feather='info' class='icon status-icon-info'></i> No previous sync data available.</li>"],
      recentItems: ["No previous sync actions recorded."]
    }
  };
}

/**
 * Runs the full sync process when triggered by the dashboard UI (Phase 2).
 * Stores the results and returns them to the UI.
 * @return {Object} The final results object formatted for the dashboard.
 */
function runFullSync_Dashboard() {
  Logger.log("runFullSync_Dashboard called.");
  const startTime = new Date();
  let overallStatus = 'Unknown';
  let aggregatedResults = {
      typeCounters: {}, // Will hold { sessions: { created: x, updated: y, ... } }
      totalErrors: 0,
      logs: [], // Collect formatted HTML log strings
      recentItems: [] // Collect summary strings
  };
  let finalErrorMessage = null;

  try {
      Logger.log("--- DASHBOARD TRIGGER: Starting Full Airtable to Sheets Sync ---");
      var masterLogCollector = [];
      var recentItemsCollector = []; // Passed to sync function

      // Logger function for this specific run
      function addMasterLog(msg) {
          masterLogCollector.push(msg);
          Logger.log(msg); // Also log to standard GAS logger
      }

      let hasSyncErrors = false;

      // Loop through CONFIG (will only be 'sessions' currently)
      for (var key in CONFIG) {
          if (CONFIG.hasOwnProperty(key)) {
              var config = CONFIG[key];
              var syncFunctionName = config.syncFunctionName || 'syncAirtableToSheet';
              addMasterLog(`\n--- Processing: ${config.type.toUpperCase()} (Sheet: ${config.sheetName}) ---`);
              let result;
              try {
                  // Call the actual sync function, passing log/recent item collectors
                  result = this[syncFunctionName](config, addMasterLog, recentItemsCollector);

                  aggregatedResults.typeCounters[config.type] = result.counters; // Store counters by type

                  addMasterLog(`Result for ${config.type.toUpperCase()}: ${result.success ? "SUCCESS" : "FAILED"}`);
                  if (!result.success) {
                      hasSyncErrors = true;
                      aggregatedResults.totalErrors += 1; // Count sync tasks that failed
                      if (result.error) {
                          addMasterLog(`ERROR: ${result.error}`);
                           finalErrorMessage = (finalErrorMessage ? finalErrorMessage + "; " : "") + config.type + ": " + result.error;
                      }
                  }
                   addMasterLog("Counters: " + JSON.stringify(result.counters));

              } catch (e) {
                  // Catch fatal errors during the sync function call itself
                  addMasterLog(`FATAL ERROR during ${config.type} sync: ${e.message}${e.stack ? "\nStack: " + e.stack : ""}`);
                  hasSyncErrors = true;
                  aggregatedResults.totalErrors += 1;
                  finalErrorMessage = (finalErrorMessage ? finalErrorMessage + "; " : "") + `Fatal error in ${config.type}: ${e.message}`;
                   // Ensure counters object exists even on fatal error for this type
                   if (!aggregatedResults.typeCounters[config.type]) {
                       aggregatedResults.typeCounters[config.type] = { updated: 0, skipped: 0, created: 0, deleted: 0 }; // Default counters
                   }
              }
          }
      } // End loop through CONFIG

      addMasterLog("\n--- Recent Actions Summary (" + recentItemsCollector.length + " items) ---");
      recentItemsCollector.forEach(item => addMasterLog("- " + item)); // Log summary actions

      addMasterLog("--- Full Sync Run Complete ---");
      overallStatus = hasSyncErrors ? 'Failed' : 'Success';
      addMasterLog("Overall Status: " + overallStatus);


      // --- Format Logs for Dashboard (Basic HTML list items with icons) ---
       aggregatedResults.logs = masterLogCollector.map(logString => {
           let icon = 'chevrons-right'; // Default icon
           let iconClass = 'status-icon-info'; // Default CSS class
           const lowerLog = logString.toLowerCase();

           // Determine icon and class based on log content keywords
           if (lowerLog.includes('error') || lowerLog.includes('failed') || lowerLog.includes('fatal')) { icon = 'alert-triangle'; iconClass = 'status-icon-error'; }
           else if (lowerLog.includes('warn')) { icon = 'alert-circle'; iconClass = 'status-icon-warning'; }
           else if (lowerLog.includes('success') || lowerLog.includes('complete')) { icon = 'check-circle'; iconClass = 'status-icon-complete'; }
           else if (lowerLog.includes('info:') || lowerLog.includes('processing') || lowerLog.includes('starting')) { icon = 'info'; iconClass = 'status-icon-info'; }
           else if (lowerLog.includes('created') || lowerLog.includes('added ')) { icon = 'plus-circle'; iconClass = 'status-icon-new'; }
           else if (lowerLog.includes('updated')) { icon = 'edit-2'; iconClass = 'status-icon-updated'; }
           else if (lowerLog.includes('deleted') || lowerLog.includes('removed ')) { icon = 'trash-2'; iconClass = 'status-icon-error'; } // Use error color for delete
           else if (lowerLog.includes('skipped') || lowerLog.includes('no changes')) { icon = 'skip-forward'; iconClass = 'status-icon-skipped'; }

            // Basic HTML escaping for the message itself
            const message = logString.replace(/</g, "<").replace(/>/g, ">");
           // Return list item HTML (assumes dashboard.html includes Feather Icons library)
           return `<li><i data-feather='${icon}' class='icon ${iconClass}'></i><span class="log-message">${message}</span></li>`;
       });

       // Use the already collected recent items
       aggregatedResults.recentItems = recentItemsCollector;


  } catch (outerError) {
      // Catch errors in the dashboard wrapper/aggregation logic itself
      Logger.log("FATAL ERROR in runFullSync_Dashboard wrapper: " + outerError + (outerError.stack ? "\nStack: " + outerError.stack : ""));
      overallStatus = 'Failed';
      finalErrorMessage = "Dashboard wrapper error: " + outerError.message;
      aggregatedResults.totalErrors += 1;
      // Add a fatal error message to the logs array
       aggregatedResults.logs.push(`<li><i data-feather='x-octagon' class='icon status-icon-error'></i><span class="log-message">FATAL WRAPPER ERROR: ${Utilities.encodeHtml(outerError.message)}</span></li>`);
  }

  // --- Prepare Final Result Object for the Dashboard ---
  const endTime = new Date();
  const duration = endTime.getTime() - startTime.getTime(); // Duration in milliseconds

  const finalResult = {
      lastSyncTimestamp: startTime.toISOString(), // Use ISO format for easy JS parsing
      lastSyncStatus: overallStatus,
      lastSyncDuration: duration,
      lastSyncResults: aggregatedResults // Contains typeCounters, totalErrors, logs (HTML), recentItems
  };

   // --- Store Result in User Properties for next dashboard load ---
   try {
       // Use a unique property key for this project
       // Use JSON.stringify, be mindful of potential size limits (~9KB per value, ~500KB total)
       PropertiesService.getUserProperties().setProperty('lastSyncDashboardData_Sessions', JSON.stringify(finalResult));
       Logger.log("Stored latest sync results for dashboard.");
   } catch (e) {
       Logger.log("ERROR storing dashboard data: " + e);
       // Attempt to store truncated data if size is the issue
       try {
            let truncatedResult = JSON.parse(JSON.stringify(finalResult)); // Deep copy
            truncatedResult.lastSyncResults.logs = ["<li>Log truncated due to storage limits. Check execution logs.</li>"];
            truncatedResult.lastSyncResults.recentItems = ["Recent items truncated due to storage limits."];
            PropertiesService.getUserProperties().setProperty('lastSyncDashboardData_Sessions', JSON.stringify(truncatedResult));
            Logger.log("Stored TRUNCATED sync results for dashboard.");
       } catch (e2) {
            // If even truncated fails, log error and update status
            Logger.log("ERROR storing even truncated dashboard data: " + e2);
            finalResult.lastSyncResults.logs.push(`<li><i data-feather='alert-triangle' class='icon status-icon-warning'></i><span class="log-message">Warning: Failed to store sync results.</span></li>`);
            finalResult.lastSyncStatus = 'Failed'; // Mark as failed if storage fails critically
       }
   }

  Logger.log("runFullSync_Dashboard finished. Status: " + overallStatus + ", Duration: " + duration + "ms");
  // Return the results object to the dashboard's client-side success handler
  return finalResult;
}