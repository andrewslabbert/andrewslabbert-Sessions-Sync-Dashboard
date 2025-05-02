/*******************************************************************************
 * Sessions Sync Dashboard (Phase 2: WP Import Control) - v1.2 WP Integration
 * Description: Fetches data from Airtable, syncs to Google Sheet, triggers
 *              WP All Import, monitors status via cache, and allows cache clearing.
 * Based On:    Sessions Sync v1.1 & Events Sync Dashboard v2.0 structure.
 * Version:     1.3 - Sessions (WP Import Integration)
 *******************************************************************************/

/********************************************************
 * Global Configuration
 ********************************************************/

// --- Retrieve Secrets from Script Properties ---
const AIRTABLE_API_TOKEN = PropertiesService.getScriptProperties().getProperty('AIRTABLE_API_TOKEN');
const WP_IMPORT_KEY = PropertiesService.getScriptProperties().getProperty('WP_IMPORT_KEY'); // <-- ADDED

// --- Check if properties were retrieved successfully ---
if (!AIRTABLE_API_TOKEN) {
  Logger.log("ERROR: AIRTABLE_API_TOKEN not found in Script Properties. Please run storeSecrets() or set it manually.");
  throw new Error("Missing required Script Property: AIRTABLE_API_TOKEN");
}
if (!WP_IMPORT_KEY) { // <-- ADDED CHECK
  Logger.log("ERROR: WP_IMPORT_KEY not found in Script Properties. Please run storeSecrets() or set it manually.");
  throw new Error("Missing required Script Property: WP_IMPORT_KEY");
}

// --- Airtable Configuration (Existing) ---
const AIRTABLE_BASE_ID_SESSIONS = 'apphmPNiTdmcknmfs';
const AIRTABLE_TABLE_SESSIONS_ID = 'tblnvqOHWXe5VQanu';

// --- Google Sheet Configuration ---
const SESSIONS_SHEET_NAME = "sessions";
const SESSIONS_IMPORT_LOG_SHEET_NAME = "sessions_wp_import_logs"; // <-- ADDED (Specific Name)
const SESSIONS_CALLBACK_VERIFICATION_SHEET_NAME = "sessions_wp_callback_data"; // <-- ADDED (Specific Name)

// --- WordPress Configuration --- // <-- ADDED SECTION
const WP_IMPORT_BASE_URL = 'https://wordpress-1204105-4784464.cloudwaysapps.com/wp-load.php'; // !! ACTION: Replace with your actual WP site URL (e.g., https://four12global.com/wp-load.php) !!
// const SESSIONS_WP_IMPORT_ID = 'YOUR_IMPORT_ID'; // !! ACTION: Replace with the WP All Import ID for Sessions (Removed from here - Will be passed from UI) !!
const WP_ACTION_TIMEOUT = 45; // Seconds for actions like cache clear, initiate call

// --- Script Configuration (Existing + Cache) ---
const MAX_FETCH_RETRIES = 3;
const BASE_RETRY_DELAY_MS = 500;
const INTER_PAGE_DELAY_MS = 200;
const CACHE_EXPIRATION_SECONDS = 21600; // 6 hours for WP Import status cache


/********************************************************
 * Workflow Configuration Object (Sessions Only - Updated)
 * Includes WP Import ID for potential future use server-side, though currently passed from UI
 ********************************************************/
const CONFIG = {
  sessions: {
    type: 'sessions',
    airtable: {
      baseId: AIRTABLE_BASE_ID_SESSIONS,
      tableId: AIRTABLE_TABLE_SESSIONS_ID,
      viewName: 'Google Apps Script',
      fields: [
         // **** ACTION: Verify/Edit this list to match desired sheet columns ****
         'title', 'session_sku', 'id', 'slug', 'session_description', 'excerpt',
         'date_short', 'date_long', 'featured_image_link', 'listing_image_link',
         'no_words_image_link', 'banner_image_link', 'series_category',
         'topics_title', 'speaker_title', 'youtube_link', 'pdf_image_1',
         'pdf_title_1', 'pdf_title_2', 'spotify_podcast', 'apple_podcast',
         'series_title', 'series_sku', '_aioseo_description', 'permalink',
         'website_status', 'last_modified', 'publish_timestamp', 'publish_status',
         'session_description_cleanup', 'series_type', 'global_categories',
         'alt_link', 'pdf_link_1', 'pdf_image_2', 'pdf_link_2'
      ],
      timestampField: 'publish_timestamp',
    },
    sheetName: SESSIONS_SHEET_NAME,
    syncFunctionName: 'syncAirtableToSheet', // Uses the generic sync function
    wpImportId: '31' // !! ACTION: Replace with the WP All Import ID for Sessions (Used by UI) !!
  }
};


// =======================================================
//             AIRTABLE & SHEET SYNC FUNCTIONS
//        (fetchAirtableData_, formatFieldValue_,
//     standardizeTimestampForComparison_, syncAirtableToSheet)
//
//          --- NO CHANGES NEEDED IN THIS SECTION ---
//     Keep the existing functions from your previous version.
// =======================================================

/********************************************************
 * Helper: Fetch Airtable Data with Retries
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

/*******************************************************************************
 * syncAirtableToSheet (CORE SHEET SYNC FUNCTION - v1.3 Incremental + Targeted Delete)
 * Fetches data from a specified Airtable View, performs incremental updates/appends
 * based on timestamp, and deletes rows from the sheet that are NO LONGER in the view.
 * Provides simplified summary feedback.
 *******************************************************************************/
 function syncAirtableToSheet(config, addLog, recentItemsCollector) { // Renamed recentItems param for clarity
    var counters = { updated: 0, skipped: 0, created: 0, deleted: 0 };
    var logArray = [];

    // --- Internal logging helper ---
    function logEntry(msg) {
        var timeStamped = "[" + new Date().toLocaleTimeString() + "] ";
        var prefix = "[" + (config.type || 'SYNC').toUpperCase() + "] ";
        var fullMsg = timeStamped + prefix + msg;
        logArray.push(fullMsg);
        if (addLog && typeof addLog === 'function') { addLog(fullMsg); }
        else { Logger.log(fullMsg); }
    }

    // --- Configuration Validation ---
    // (Keep existing validation logic - checks for airtable props, timestampField, sheetName, etc.)
    const primaryTitleField = 'title';
    if (!config || !config.airtable || !config.airtable.baseId || !config.airtable.tableId || !config.airtable.timestampField || !config.sheetName || !Array.isArray(config.airtable.fields)) { /* ... Error Handling ... */ return { success: false, error: "Config Error...", counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
    if (!config.airtable.fields.includes(config.airtable.timestampField)) { /* ... Error Handling ... */ return { success: false, error: "Timestamp field missing...", counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
    if (!config.airtable.fields.includes(primaryTitleField)) { logEntry(`WARN: Primary title field '${primaryTitleField}' not in config.airtable.fields.`); }
    if (!config.airtable.viewName) { // ** Crucial Check for this logic **
        logEntry("ERROR: This sync function requires a specific Airtable 'viewName' in the configuration to perform targeted deletions.");
        return { success: false, error: "Configuration Error: Airtable 'viewName' is required.", counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector };
    }

    logEntry("INFO: Starting sync to sheet '" + config.sheetName + "' from Airtable View '" + config.airtable.viewName + "'.");
    var airtableRecords = [];
    var viewRecordIds = new Set(); // To store Record IDs from the Airtable View

    // --- 1. Fetch Airtable Data from the Specific View ---
    try {
        logEntry("INFO: Fetching data from Airtable table: " + config.airtable.tableId + " (View: " + config.airtable.viewName + ")");
        var apiUrl = 'https://api.airtable.com/v0/' + config.airtable.baseId + '/' + encodeURIComponent(config.airtable.tableId);
        airtableRecords = fetchAirtableData_(apiUrl, config.airtable.fields, config.airtable.viewName);
         // Populate the Set of IDs from the view
         airtableRecords.forEach(record => { if(record.id) viewRecordIds.add(record.id); });
         logEntry(`INFO: Fetched ${airtableRecords.length} records from Airtable view. Found ${viewRecordIds.size} unique Record IDs.`);
    } catch (fetchErr) { /* ... Error Handling ... */ return { success: false, error: "Airtable Fetch Error: " + fetchErr.message, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }

    // --- 2. Define Target Header and Process Fetched Data ---
    // (Keep existing logic for defining targetHeader and processing airtableRecords into newData array)
    // ... (Ensure this part creates the newData array with [targetHeader, [rowData1], [rowData2], ...]) ...
    // --- [EXISTING CODE FOR THIS SECTION GOES HERE] ---
    var newData = [];
    var targetHeader = ["AirtableRecordID"];
    var configuredFieldsSet = new Set(config.airtable.fields);
    const orderedFields = config.airtable.fields.filter(f => configuredFieldsSet.has(f));
    targetHeader = targetHeader.concat(orderedFields);
    newData.push(targetHeader);
    logEntry("INFO: Target header defined: " + targetHeader.join(', '));
    const targetHeaderIndexMap = targetHeader.reduce((map, header, index) => { map[header] = index; return map; }, {});
    const recordIdColIndex_Target = 0;
    const timestampColIndex_Target = targetHeaderIndexMap[config.airtable.timestampField];
    const titleColIndex_Target = targetHeaderIndexMap[primaryTitleField]; // Not used for recentItems anymore, but kept for standardization
    airtableRecords.forEach(function (record) {
        var fields = record.fields || {}; var airtableNativeId = record.id; if (!airtableNativeId) return;
        var newRowArray = targetHeader.map(headerName => {
            if (headerName === "AirtableRecordID") return airtableNativeId;
            else return formatFieldValue_(fields[headerName]); });
        newData.push(newRowArray); });
    logEntry("INFO: Processed fetched Airtable data into " + (newData.length - 1) + " data rows.");
    // --- [END OF EXISTING CODE FOR THIS SECTION] ---


    // --- 3. Interact with Google Sheet ---
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName(config.sheetName);
    var sheetCreated = false;

    // Handle Sheet Creation or Empty Sheet
    if (!sheet) {
        try {
            sheet = ss.insertSheet(config.sheetName); sheetCreated = true; logEntry("INFO: Created new sheet: " + config.sheetName);
            // Write data directly
            if (newData.length > 0) {
                sheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
                sheet.setFrozenRows(1);
                counters.created = newData.length - 1;
                logEntry(`INFO: Wrote ${counters.created} records to new sheet.`);
            }
            // ** Add summary message for new sheet **
            if (recentItemsCollector && typeof recentItemsCollector.push === 'function') {
                 recentItemsCollector.push(`${capitalizeFirstLetter(config.type)}: Created sheet '${config.sheetName}' with ${counters.created} records.`);
            }
            return { success: true, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector };
        } catch (e) { /* ... Error Handling ... */ return { success: false, error: "Sheet Creation/Write Error: " + e.message, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
    }

    // Get existing data from sheet
    var existingData = []; var existingHeader = []; var sheetIsEmpty = true;
    var lastRow = sheet.getLastRow(); var lastCol = sheet.getLastColumn();

    if (lastRow > 0 && lastCol > 0) {
         try {
            existingData = sheet.getDataRange().getValues();
            existingHeader = existingData.length > 0 ? existingData[0].map(String) : [];
            sheetIsEmpty = existingData.length <= 1; // Empty if only header or less
            logEntry(`INFO: Fetched ${existingData.length} existing rows (incl. header: ${existingHeader.length > 0}) from sheet '${config.sheetName}'. Sheet is ${sheetIsEmpty ? 'effectively empty' : 'not empty'}.`);
         } catch (e) { /* ... Error Handling ... */ return { success: false, error: "Sheet Read Error: " + e.message, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
    } else {
         logEntry("INFO: Sheet '" + config.sheetName + "' exists but has no data (getLastRow/Col <= 0).");
         sheetIsEmpty = true;
         // Ensure we try to write the header later
    }


    // --- 4. Header Check and Full Rewrite (Only if headers mismatch drastically) ---
    var newHeader = targetHeader.map(String); // Header from Airtable data structure
    if (!sheetIsEmpty && JSON.stringify(existingHeader) !== JSON.stringify(newHeader)) {
        logEntry("WARN: Headers differ significantly. Rewriting entire sheet '" + config.sheetName + "'. This might happen if columns were added/removed.");
        logEntry("DEBUG Existing Header: " + (existingHeader.join(', ') || 'None'));
        logEntry("DEBUG New Header:      " + newHeader.join(', '));
        try {
            sheet.clearContents(); sheet.setFrozenRows(0); SpreadsheetApp.flush();
            // Resize sheet (optional but good practice)
            // ... [Add resizing logic if needed, similar to previous full rewrite] ...
            if (newData.length > 0) {
                 sheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
                 sheet.setFrozenRows(1);
                 counters.created = newData.length - 1;
                 counters.deleted = existingData.length -1; // Conceptual count
                 logEntry(`INFO: Sheet rewritten successfully with ${counters.created} records.`);
            }
             // ** Add summary message for rewrite **
            if (recentItemsCollector && typeof recentItemsCollector.push === 'function') {
                 recentItemsCollector.push(`${capitalizeFirstLetter(config.type)}: Rewrote sheet '${config.sheetName}' with ${counters.created} records (header change).`);
            }
        } catch (e) { /* ... Error Handling ... */ return { success: false, error: "Sheet Rewrite Error: " + e.message, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
        return { success: true, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; // Exit after rewrite
    } else if (sheetIsEmpty && newData.length > 0) {
        // Handle case where sheet was empty but exists
        logEntry("INFO: Sheet was empty. Writing new data including header.");
         try {
             // Ensure sheet dimensions are sufficient (minimal check)
             if (sheet.getMaxColumns() < newHeader.length) sheet.insertColumnsAfter(sheet.getMaxColumns(), newHeader.length - sheet.getMaxColumns());
              sheet.getRange(1, 1, newData.length, newData[0].length).setValues(newData);
              sheet.setFrozenRows(1);
              counters.created = newData.length - 1;
              logEntry(`INFO: Wrote ${counters.created} records to empty sheet.`);
               // ** Add summary message for populating empty sheet **
                if (recentItemsCollector && typeof recentItemsCollector.push === 'function') {
                     recentItemsCollector.push(`${capitalizeFirstLetter(config.type)}: Populated empty sheet '${config.sheetName}' with ${counters.created} records.`);
                }
         } catch (e) { /* ... Error Handling ... */ return { success: false, error: "Sheet Write Error (Empty): " + e.message, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
         return { success: true, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector };
    } else if (newData.length <= 1 && !sheetIsEmpty) {
         // Handle case where Airtable view is now empty, but sheet has data
         logEntry("WARN: Airtable view returned no data rows. Clearing sheet '" + config.sheetName + "' to match.");
         try {
              sheet.getDataRange().offset(1, 0).clearContents(); // Clear data rows, keep header
              counters.deleted = existingData.length - 1;
              logEntry(`INFO: Cleared ${counters.deleted} data rows from sheet.`);
               // ** Add summary message for clearing sheet **
               if (recentItemsCollector && typeof recentItemsCollector.push === 'function') {
                    recentItemsCollector.push(`${capitalizeFirstLetter(config.type)}: Cleared sheet '${config.sheetName}' (${counters.deleted} rows removed) as Airtable view is empty.`);
               }
         } catch (e) { /* ... Error Handling ... */ return { success: false, error: "Sheet Clear Error: " + e.message, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }
         return { success: true, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector };
    } else if (newData.length <= 1 && sheetIsEmpty){
        logEntry("INFO: Airtable view and sheet are both empty. Nothing to do.");
         // ** Add summary message for no data **
         if (recentItemsCollector && typeof recentItemsCollector.push === 'function') {
              recentItemsCollector.push(`${capitalizeFirstLetter(config.type)}: No data found in Airtable view or sheet.`);
         }
        return { success: true, counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector };
    }


    // --- 5. Incremental Sync Logic ---
    logEntry("INFO: Performing incremental sync (Update/Append/Delete)...");
    const sheetHeader = sheetIsEmpty ? newHeader : existingHeader; // Use new header if sheet was empty initially
    const sheetRecordIdIndex = sheetHeader.indexOf("AirtableRecordID");
    const sheetTimestampIndex = sheetHeader.indexOf(config.airtable.timestampField);

    if (sheetRecordIdIndex === -1 || sheetTimestampIndex === -1) { /* ... Error Handling for missing columns ... */ return { success: false, error: "Internal Error: Column index mapping failed.", counters: counters, log: logArray.join("\n"), recentItems: recentItemsCollector }; }

    // Build Map of Existing Sheet Records (ID -> {rowIndex, timestamp})
    var existingSheetMap = {};
    // *** ADDED CHECK: Only attempt partial read if there are actual data rows (lastRow > 1) ***
    if (!sheetIsEmpty && lastRow > 1) {
        logEntry("INFO: Attempting efficient read of ID/Timestamp columns...");
        // Get 1-based column index numbers
        const idCol = sheetRecordIdIndex + 1;
        const tsCol = sheetTimestampIndex + 1;

        try {
            // Read column data starting from row 2 up to the last data row using R1C1 indexing internally
            // Parameters: startRow, startCol, numRows, numCols
            const idValues = sheet.getRange(2, idCol, lastRow - 1, 1).getValues();
            const tsValues = sheet.getRange(2, tsCol, lastRow - 1, 1).getValues();

            for (let i = 0; i < idValues.length; i++) {
                const recID = idValues[i][0];
                if (recID && String(recID).trim() !== '') {
                    const tsValue = tsValues[i][0];
                    const rowIndex = i + 2; // +1 for 0-based loop 'i', +1 because sheet data starts at row 2 here
                    existingSheetMap[recID] = {
                        rowIndex: rowIndex,
                        timestamp: standardizeTimestampForComparison_(tsValue, recID, `sheet row ${rowIndex}`)
                    };
                }
            }
             logEntry("INFO: Built map of " + Object.keys(existingSheetMap).length + " existing records from sheet (ID/Timestamp only).");
        } catch (readErr){
            logEntry("WARN: Error reading partial sheet data: " + readErr + ". Falling back to full read (slower).");
            // Fallback to reading full data if partial read fails
             existingSheetMap = {}; // Reset map
             // Use the existingData array if it was successfully read earlier
             if (existingData && existingData.length > 1) {
                 for (var i = 1; i < existingData.length; i++) { // Start i=1 to skip header in existingData array
                     var row = existingData[i];
                     // Ensure row has enough columns before accessing indices
                     if (row.length > sheetRecordIdIndex && row.length > sheetTimestampIndex) {
                        var recID = row[sheetRecordIdIndex];
                        if (recID && String(recID).trim() !== '') {
                            existingSheetMap[recID] = {
                                rowIndex: i + 1, // 1-based index from the existingData array
                                timestamp: standardizeTimestampForComparison_(row[sheetTimestampIndex], recID, `sheet row ${i+1}`)
                            };
                        }
                     } else {
                          logEntry(`WARN [Fallback]: Skipping row ${i+1} due to insufficient columns (${row.length}).`);
                     }
                 }
                 logEntry("INFO: Built map of " + Object.keys(existingSheetMap).length + " existing records from sheet (Full Read Fallback).");
             } else {
                  logEntry("WARN [Fallback]: Cannot perform full read fallback as existingData is empty or missing.");
             }
        }
    } else if (sheetIsEmpty || lastRow <= 1) {
         logEntry("INFO: Sheet is empty or only contains a header. Skipping build of existing record map.");
         // existingSheetMap remains empty {}
    }


    // --- Compare New Data (from View) and Prepare Batches ---
    var rowsToUpdate = []; // { range: 'A1Notation', values: [[...]] }
    var rowsToAppend = []; // [[...], [...]]
    // Note: recordIdsToKeep was already populated when fetching from view (viewRecordIds)

    for (var i = 1; i < newData.length; i++) { // Start i=1 to skip header in newData
        var newRow = newData[i];
        var newRecordID = newRow[recordIdColIndex_Target];
        if (!newRecordID) continue; // Should have ID based on earlier logic

        var standardizedNewTimestamp = standardizeTimestampForComparison_(newRow[timestampColIndex_Target], newRecordID, `new data row ${i+1}`);
        var existingRecordInfo = existingSheetMap[newRecordID];

        if (existingRecordInfo) {
            // Exists in Sheet: Check timestamp
            if (existingRecordInfo.timestamp !== standardizedNewTimestamp) {
                var rangeNotation = sheet.getRange(existingRecordInfo.rowIndex, 1, 1, sheetHeader.length).getA1Notation();
                rowsToUpdate.push({ range: rangeNotation, values: [newRow] }); // newRow is already in correct target order
                counters.updated++;
            } else {
                counters.skipped++;
            }
        } else {
            // Does not exist in Sheet: Append
            rowsToAppend.push(newRow); // newRow is already in correct target order
            counters.created++;
        }
    }

    // --- Identify Rows to Delete (In Sheet Map, but NOT in View Data) ---
    var rowsToDeleteIndices = [];
    for (var sheetRecID in existingSheetMap) {
        if (!viewRecordIds.has(sheetRecID)) { // Check against the Set of IDs from the Airtable view
            rowsToDeleteIndices.push(existingSheetMap[sheetRecID].rowIndex);
            counters.deleted++;
        }
    }
    rowsToDeleteIndices.sort((a, b) => b - a); // Sort descending for deletion
    logEntry("INFO: Sync Analysis complete. Update: " + counters.updated + ", Create: " + counters.created + ", Delete: " + counters.deleted + ", Skipped: " + counters.skipped);

    // --- Perform Batch Operations ---
    var updateError = null, appendError = null, deleteError = null;
    var operationsPerformed = (rowsToUpdate.length + rowsToAppend.length + rowsToDeleteIndices.length) > 0;

    // --- Batch Updates ---
    if (rowsToUpdate.length > 0) {
        logEntry("INFO: Applying " + rowsToUpdate.length + " updates...");
        try { rowsToUpdate.forEach(update => sheet.getRange(update.range).setValues(update.values)); }
        catch (e) { logEntry("ERROR applying updates: " + e); updateError = e; }
    }

    // --- Batch Appends ---
    if (rowsToAppend.length > 0) {
        logEntry("INFO: Appending " + rowsToAppend.length + " new rows...");
        try {
            var startAppendRow = sheet.getLastRow() + 1;
            // Ensure sheet dimensions if needed (can be less critical for append)
            sheet.getRange(startAppendRow, 1, rowsToAppend.length, sheetHeader.length).setValues(rowsToAppend);
        } catch (e) { logEntry("ERROR applying appends: " + e); appendError = e; }
    }

    // --- Batch Deletes (Individually, but loop is batch) ---
    if (rowsToDeleteIndices.length > 0) {
        logEntry("INFO: Deleting " + rowsToDeleteIndices.length + " rows...");
        try {
            rowsToDeleteIndices.forEach(function(rowIndex) {
                 if (rowIndex > 0 && rowIndex <= sheet.getMaxRows()) { // Check validity
                     sheet.deleteRow(rowIndex);
                 } else { logEntry("WARN: Skipped deletion of invalid row index " + rowIndex); }
            });
        } catch (e) { logEntry("ERROR deleting rows: " + e); deleteError = e; }
    }

    // --- Final Logging & Summary Message ---
    var overallSuccess = !updateError && !appendError && !deleteError;
    var summaryMessage = "";
    const entityName = capitalizeFirstLetter(config.type);

    if (overallSuccess) {
        if (operationsPerformed) {
            let parts = [];
            if (counters.created > 0) parts.push(`${counters.created} created`);
            if (counters.updated > 0) parts.push(`${counters.updated} updated`);
            if (counters.deleted > 0) parts.push(`${counters.deleted} removed`); // Changed from 'deleted' for user clarity
            summaryMessage = `${entityName}: Sync complete. ${parts.join(', ')}.`;
            if (counters.skipped > 0) summaryMessage += ` (${counters.skipped} skipped)`;
        } else if (counters.skipped > 0) {
            summaryMessage = `${entityName}: Sync complete. No changes detected (${counters.skipped} checked).`;
        } else {
             summaryMessage = `${entityName}: Sync complete. No changes detected.`; // Should ideally not happen if fetched data had rows
        }
         logEntry("INFO: Sync completed successfully.");
    } else {
         let errorSummary = [updateError, appendError, deleteError].filter(Boolean).map(e => e.message).join('; ');
         summaryMessage = `${entityName}: Sync failed. Error(s): ${errorSummary}`;
         logEntry("ERROR: Sync completed with errors.");
    }

    // Add the single summary message to recent items
    if (recentItemsCollector && typeof recentItemsCollector.push === 'function') {
        recentItemsCollector.push(summaryMessage);
    }

    return {
        success: overallSuccess,
        error: overallSuccess ? null : summaryMessage, // Return summary message as error on failure
        counters: counters,
        log: logArray.join("\n"),
        recentItems: recentItemsCollector
    };
}

/********************************************************
 * Helper: Capitalize First Letter
 ********************************************************/
function capitalizeFirstLetter(string) {
  if (!string) return '';
  return string.charAt(0).toUpperCase() + string.slice(1);
}


// =======================================================
//            WORDPRESS IMPORT & CACHE FUNCTIONS
//               --- NEW SECTION FOR PHASE 2 ---
// =======================================================

// =======================================================
//            WORDPRESS IMPORT & CACHE FUNCTIONS
//               --- NEW SECTION FOR PHASE 2 ---
// =======================================================

/********************************************************
 * initiateWordPressImport
 * Triggers WP All Import via URL, stores pending status in cache.
 * @param {string} importId - The WP All Import numerical ID.
 * @return {object} Result object { success: boolean, status: string, message: string, runId: number|null, log: string }
 ********************************************************/
function initiateWordPressImport(importId) {
    var startTime = new Date();
    var logCollector = [];
    var triggerDetails = null;
    var processingDetails = null;
    var triggerSuccess = false; // Tracks if trigger was successful AND confirmed by WP
    var processingAttempted = false;
    var overallStatus = 'error';
    var overallMessage = 'Import initiation failed.';
    var runId = startTime.getTime(); // Unique ID for this specific run attempt

    function logWP(msg) {
        var timeStamped = "[" + startTime.toLocaleTimeString() + "] ";
        logCollector.push(timeStamped + msg);
        // Use specific prefix for easy filtering in logs
        Logger.log("WP_INITIATE [" + importId + "]: " + msg);
    }

    // --- Validate Input ---
    if (!importId) {
        logWP("ERROR: Import ID is missing. Cannot initiate WordPress import.");
        return {
             success: false, status: 'error_missing_id',
             message: 'Import ID was not provided.', runId: null,
             log: logCollector.join("\n")
        };
    }
    importId = String(importId); // Ensure string
    var cacheKey = 'import_status_' + importId;
    logWP("INFO: Initiating WordPress Import. Import ID: " + importId + ", Run ID: " + runId);

    // --- Check Cache for Pending Status ---
    try {
        var existingStatusRaw = cache.get(cacheKey);
        if (existingStatusRaw) {
            var existingStatus = JSON.parse(existingStatusRaw);
            if (existingStatus && existingStatus.status === 'pending') {
                 logWP("WARN: Import ID " + importId + " already 'pending' in cache. Preventing new initiation.");
                 return {
                     success: false, status: 'already_pending',
                     message: 'Import ' + importId + ' appears to be already running or pending. Please wait or Cancel if stuck.',
                     runId: null, log: logCollector.join("\n")
                 };
            }
        }
    } catch (cacheReadErr) {
        logWP("WARN: Could not read existing cache status: " + cacheReadErr.message + ". Proceeding.");
    }

    // --- Store Pending Status in Cache ---
    try {
      var pendingStatus = {
          status: 'pending', runId: runId, startTime: startTime.toISOString(),
          importId: importId, message: "Import triggered, awaiting completion..."
      };
      cache.put(cacheKey, JSON.stringify(pendingStatus), CACHE_EXPIRATION_SECONDS);
      logWP("INFO: Stored 'pending' status in cache (Key: " + cacheKey + ")");
    } catch (cacheErr) {
        logWP("ERROR: Failed to store pending status in cache: " + cacheErr.message + ". Proceeding anyway.");
        overallMessage = "Failed to store initial status in cache, but proceeding.";
    }

    // --- Construct URLs ---
    // Using constants defined at the top of the script
    var baseUrl = WP_IMPORT_BASE_URL +
                  '?import_key=' + encodeURIComponent(WP_IMPORT_KEY) +
                  '&rand=' + Math.random();
    var triggerUrl = baseUrl + '&import_id=' + encodeURIComponent(importId) + '&action=trigger';
    var processingUrl = baseUrl + '&import_id=' + encodeURIComponent(importId) + '&action=processing';

    // --- Set Fetch Options ---
    var options = {
        method: 'get', muteHttpExceptions: true,
        // Update User-Agent for this specific project
        headers: { 'User-Agent': 'GoogleAppsScript-SessionsSyncDashboard/1.2-Initiate' },
        deadline: WP_ACTION_TIMEOUT // Apply timeout
    };

    // --- 1. Trigger the Import ---
    logWP("INFO: Sending trigger request...");
    Logger.log("DEBUG: Trigger URL: " + triggerUrl);
    try {
        var triggerResponse = UrlFetchApp.fetch(triggerUrl, options);
        var triggerResponseCode = triggerResponse.getResponseCode();
        var triggerResponseText = triggerResponse.getContentText() || '(No response body)';
        triggerDetails = { code: triggerResponseCode, text_snippet: triggerResponseText.substring(0, 250) + "..." };
        logWP("INFO: WP Trigger Response Code: " + triggerResponseCode);
        Logger.log("DEBUG: WP Trigger Response Body: " + triggerResponseText);

        if (triggerResponseCode === 200) {
            var triggerResponseJson = {};
            try { triggerResponseJson = JSON.parse(triggerResponseText); } catch (e) { /* Check text instead */ }

            // Check for WP success confirmation (JSON or text)
            if (triggerResponseJson.status === 200 || (triggerResponseText.toLowerCase().includes("triggered") && !triggerResponseText.toLowerCase().includes("already processing"))) {
                triggerSuccess = true;
                logWP("INFO: Trigger successful (HTTP 200 + WP confirmation).");

                // --- 2. Attempt ONE Processing Run ---
                processingAttempted = true;
                logWP("INFO: Sending initial processing request...");
                Logger.log("DEBUG: Processing URL: " + processingUrl);
                Utilities.sleep(1000); // Short delay

                try {
                    var processingResponse = UrlFetchApp.fetch(processingUrl, options);
                    var processingResponseCode = processingResponse.getResponseCode();
                    var processingResponseText = processingResponse.getContentText() || '(No response body)';
                    processingDetails = { code: processingResponseCode, text_snippet: processingResponseText.substring(0, 250) + "..." };
                    logWP("INFO: WP Processing Response Code: " + processingResponseCode);
                    Logger.log("DEBUG: WP Processing Response Body: " + processingResponseText);

                    if (processingResponseCode === 200) {
                        overallStatus = 'initiated';
                        overallMessage = 'Import ' + importId + ' initiated. Waiting for completion callback.';
                        logWP("INFO: Initial processing call successful (HTTP 200).");
                    } else {
                        overallStatus = 'initiated_processing_http_error';
                        overallMessage = 'Import triggered, but initial processing call failed (HTTP Status: ' + processingResponseCode + '). Import might still run.';
                        logWP("ERROR: " + overallMessage);
                    }
                } catch (procErr) {
                    processingDetails = { error: procErr.message };
                    if (procErr.message.includes("Timeout") || procErr.message.includes("timed out")) {
                         overallStatus = 'initiated_processing_timeout';
                         overallMessage = 'Import triggered, but initial processing call timed out. Import might still run.';
                         logWP("WARN: Initial processing call timed out. " + procErr.message);
                     } else {
                        overallStatus = 'initiated_processing_fetch_error';
                        overallMessage = 'Import triggered, but error calling processing URL: ' + procErr.message;
                        logWP("EXCEPTION during processing call: " + overallMessage);
                     }
                }
            } else {
                 triggerSuccess = false;
                 overallStatus = 'trigger_wp_error';
                 var wpErrorMsg = triggerResponseJson.message || (triggerResponseText.length < 200 ? triggerResponseText : "(See logs)");
                 overallMessage = 'WP reported issue triggering Import ' + importId + ': ' + wpErrorMsg;
                 logWP("WARN: Trigger HTTP 200, but WP reported issue: " + overallMessage);
                 try { cache.remove(cacheKey); logWP("INFO: Removed pending status from cache (WP trigger issue)."); } catch(rmErr){ logWP("WARN: Failed removing pending status: "+ rmErr.message);}
            }
        } else {
            triggerSuccess = false;
            overallStatus = 'trigger_http_error';
            overallMessage = 'WP trigger failed (HTTP Status: ' + triggerResponseCode + ')';
            logWP("ERROR: " + overallMessage);
            try { cache.remove(cacheKey); logWP("INFO: Removed pending status from cache (HTTP trigger failure)."); } catch(rmErr){ logWP("WARN: Failed removing pending status: "+ rmErr.message);}
        }
    } catch (triggerErr) {
         triggerSuccess = false;
         triggerDetails = { error: triggerErr.message };
         if (triggerErr.message.includes("Timeout") || triggerErr.message.includes("timed out")) {
             overallStatus = 'trigger_timeout_error';
             overallMessage = 'Request to trigger import timed out. Status unknown.';
             logWP("WARN: Trigger call timed out. " + triggerErr.message);
         } else {
            overallStatus = 'trigger_fetch_error';
            overallMessage = 'Error making trigger request: ' + triggerErr.message;
            logWP("EXCEPTION during trigger call: " + overallMessage);
         }
         try { cache.remove(cacheKey); logWP("INFO: Removed pending status from cache (Fetch trigger failure)."); } catch(rmErr){ logWP("WARN: Failed removing pending status: "+ rmErr.message);}
    }

    // --- Prepare Result ---
    var finalElapsedTime = ((new Date().getTime() - startTime.getTime()) / 1000).toFixed(1);
    logWP("INFO: Initiation Function Finished. Elapsed: " + finalElapsedTime + "s. Status: " + overallStatus);

    // Return success: true only if trigger was HTTP 200 AND WP confirmed it triggered ok
    // AND the overall status indicates it moved to an initiated phase.
    let isSuccess = triggerSuccess && overallStatus.startsWith('initiated');

    return {
        success: isSuccess,
        status: overallStatus,
        message: overallMessage,
        runId: isSuccess ? runId : null, // Only return runId if successfully initiated
        log: logCollector.join("\n")
    };
}


/********************************************************
 * getImportStatus - Reads status via the ImportHandlerLib Library.
 *                   Calls the central handler script to get status from its cache.
 * @param {string} importId - The WP All Import numerical ID.
 * @return {object|null} Parsed status object from the Handler's cache
 *                       (e.g., {status:'complete', importId:'31', ...})
 *                       or null if not found/expired/invalid,
 *                       or an error object {status:'error', ...} on failure.
 ********************************************************/
function getImportStatus(importId) {
    const logPrefix = "DASHBOARD [getImportStatus]: ";

    if (!importId) {
        Logger.log(logPrefix + "Called without importId.");
        // Return an object indicating the error, consistent with other error returns
        return { status: 'error_local', message: 'Import ID missing in dashboard call.', importId: null };
    }
    importId = String(importId); // Ensure it's a string

    Logger.log(logPrefix + "Requesting status for Import ID '" + importId + "' from ImportHandlerLib.");

    try {
        // --- Call the Library Function ---
        // This executes the getImportStatusFromCache function within the Handler script's context
        var statusDataFromLib = ImportHandlerLib.getImportStatusFromCache(importId);
        // --- End Library Call ---

        if (statusDataFromLib) {
            // The library function returns the parsed object on success
            Logger.log(logPrefix + "Received status object from Library for ID '" + importId + "'. Status: " + statusDataFromLib.status);
            // Add timestamp for freshness check if needed by UI later
            statusDataFromLib.libraryCheckTime = new Date().toISOString();
            return statusDataFromLib; // Return the valid status object received from the library

        } else {
            // Library returned null (means not found, expired, or parse error within the library)
            Logger.log(logPrefix + "Library returned null for Import ID '" + importId + "' (Not found, expired, or invalid data in Handler cache).");
            // Return null to indicate status is unavailable via the library
            return null;
        }

    } catch (e) {
        // --- Handle errors during the library call itself ---
        Logger.log(logPrefix + "ERROR calling ImportHandlerLib.getImportStatusFromCache for ID '" + importId + "': " + e);
        if (e.message.includes("ImportHandlerLib is not defined")) {
             Logger.log(logPrefix + "This likely means the Library Identifier is incorrect or the library wasn't added properly.");
             return { status: 'error_library_setup', message: "Library configuration error: " + e.message, importId: importId };
        } else if (e.message.includes("You do not have permission")) {
             Logger.log(logPrefix + "This likely means the Handler library needs re-authorization or permissions changed.");
             return { status: 'error_library_permission', message: "Library permission error: " + e.message, importId: importId };
        } else {
             // Generic error calling the library
             return { status: 'error_library_call', message: "Error calling status library: " + e.message, importId: importId };
        }
    }
}


/********************************************************
 * cancelWordPressImport - Attempts to signal WP to cancel an import
 * @param {string} importId - The WP All Import numerical ID.
 * @return {object} Result object { success: boolean, status: string, message: string, log: string }
 ********************************************************/
function cancelWordPressImport(importId) {
    var startTime = new Date();
    var logCollector = [];
    var success = false;
    var message = "Cancellation request failed.";
    var status = "error";

    function logCancel(msg) {
        var timeStamped = "[" + startTime.toLocaleTimeString() + "] ";
        logCollector.push(timeStamped + msg);
        Logger.log("WP_CANCEL [" + importId + "]: " + msg);
    }

    // --- Validate Input ---
    if (!importId) {
        logCancel("ERROR: Import ID missing.");
        return { success: false, status: 'error_missing_id', message: 'Import ID required.', log: logCollector.join("\n") };
    }
    importId = String(importId);
    var cacheKey = 'import_status_' + importId;

    logCancel("INFO: Received request to cancel Import ID: " + importId);

    // --- Construct URL ---
    var cancelUrl = WP_IMPORT_BASE_URL +
                '?import_key=' + encodeURIComponent(WP_IMPORT_KEY) +
                '&import_id=' + encodeURIComponent(importId) +
                '&action=cancel' + // WP All Import standard action
                '&rand=' + Math.random();

    logCancel("INFO: Calling WP cancellation endpoint...");
    Logger.log("DEBUG: Cancel URL: " + cancelUrl);

    // --- Set Fetch Options ---
    var options = {
        method: 'get', muteHttpExceptions: true,
        headers: { 'User-Agent': 'GoogleAppsScript-SessionsSyncDashboard/1.2-Cancel' },
        deadline: WP_ACTION_TIMEOUT
    };

    // --- Make Request ---
    try {
        var response = UrlFetchApp.fetch(cancelUrl, options);
        var responseCode = response.getResponseCode();
        var responseText = response.getContentText() || '(No response body)';
        logCancel("INFO: WP Cancel Response Code: " + responseCode);
        Logger.log("DEBUG: WP Cancel Response Body: " + responseText);

        // WP All Import 'cancel' might just return simple text confirmation on success
        if (responseCode === 200 && (responseText.toLowerCase().includes("cancelled") || responseText.toLowerCase().includes("stopped"))) {
            success = true; status = "success";
            message = "Cancellation request sent. Import should stop.";
            logCancel("✅ SUCCESS: WP acknowledged cancellation request.");

            // --- Remove pending status from cache ---
            try {
                var currentStatus = cache.get(cacheKey);
                if (currentStatus) {
                   var currentData = JSON.parse(currentStatus);
                   // Update status to cancelled in cache before removing? Optional.
                   // currentData.status = 'cancelled';
                   // currentData.message = 'Cancelled via dashboard.';
                   // cache.put(cacheKey, JSON.stringify(currentData), 600); // Store cancelled state briefly
                   cache.remove(cacheKey); // Remove the key
                   logCancel("INFO: Removed status from cache for cancelled Import ID " + importId);
                }
            } catch (cacheErr) {
                logCancel("WARN: Failed removing status from cache after cancellation: " + cacheErr.message);
                message += " (Cache cleanup issue)";
            }
        } else if (responseCode === 200) {
             success = false; status = "wp_error";
             message = "WP response indicates cancellation not needed/failed. Msg: " + (responseText.length < 200 ? responseText : "(See logs)");
             logCancel("WARN: WP indicated cancellation issue: " + message);
        } else {
            success = false; status = "http_error";
            message = "WP cancellation endpoint failed (HTTP Status: " + responseCode + ")";
            logCancel("❌ ERROR: " + message);
        }
    } catch (fetchErr) {
        success = false;
         if (fetchErr.message.includes("Timeout") || fetchErr.message.includes("timed out")) {
            status = "timeout_error";
            message = "Request to cancel import timed out. Status uncertain.";
             logCancel("⚠️ WARN: Cancel request timed out.");
         } else {
            status = "fetch_error";
            message = "Error sending cancellation request: " + fetchErr.message;
            logCancel("❌ EXCEPTION during cancel call: " + message);
         }
    }

    // --- Prepare Result ---
    var finalElapsedTime = ((new Date().getTime() - startTime.getTime()) / 1000).toFixed(1);
    logCancel("INFO: Cancel Import Finished. Elapsed: " + finalElapsedTime + "s. Status: " + status);

    return { success: success, status: status, message: message, log: logCollector.join("\n") };
}

// =======================================================
//                WEB APP & DASHBOARD FUNCTIONS
//            (Including doPost for WP Callbacks)
// =======================================================



/********************************************************
 * clearBreezeCache - Calls the WP plugin to clear Breeze cache
 * @return {object} Result object { success: boolean, status: string, message: string, log: string }
 ********************************************************/
function clearBreezeCache() {
    var startTime = new Date();
    var logCollector = [];
    var success = false;
    var message = "Cache clear failed.";
    var status = "error";

    function logCache(msg) {
        var timeStamped = "[" + startTime.toLocaleTimeString() + "] ";
        logCollector.push(timeStamped + msg);
        Logger.log("CACHE_CLEAR: " + msg);
    }

    logCache("INFO: Received request to clear Breeze cache.");

    // --- Construct URL ---
    var clearCacheUrl = WP_IMPORT_BASE_URL +
                        '?import_key=' + encodeURIComponent(WP_IMPORT_KEY) +
                        '&action=clear_breeze_cache' + // Ensure this matches PHP plugin
                        '&rand=' + Math.random();

    logCache("INFO: Calling WordPress cache clear endpoint...");
    Logger.log("DEBUG: Cache Clear URL: " + clearCacheUrl);

    // --- Set Fetch Options ---
    var options = {
        method: 'get',
        muteHttpExceptions: true,
        headers: { 'User-Agent': 'GoogleAppsScript-SessionsSyncDashboard/1.2-CacheClear' },
        deadline: WP_ACTION_TIMEOUT // Set a specific timeout
    };

    // --- Make Request ---
    try {
        var response = UrlFetchApp.fetch(clearCacheUrl, options);
        var responseCode = response.getResponseCode();
        var responseText = response.getContentText() || '(No response body)';
        var responseJson = null;

        logCache("INFO: WP Cache Clear Response Code: " + responseCode);
        Logger.log("DEBUG: WP Cache Clear Response Body: " + responseText);

        try {
            responseJson = JSON.parse(responseText);
        } catch (parseErr) {
             logCache("WARN: Could not parse JSON response from WP cache clear: " + parseErr.message + ". Response Text: " + responseText.substring(0, 200));
        }

        // Check response based on expected JSON structure from PHP plugin helpers
        if (responseCode === 200 && responseJson && responseJson.success === true) {
            success = true;
            status = "success";
            message = (responseJson.data && responseJson.data.message) ? responseJson.data.message : "Cache cleared successfully (No details from WP).";
            logCache("✅ SUCCESS: WordPress confirmed cache clear.");
        } else if (responseCode === 200 && responseJson && responseJson.success === false) {
            success = false;
            status = "wp_error";
            message = (responseJson.data && responseJson.data.message) ? responseJson.data.message : "WordPress reported an error during cache clear.";
             logCache("❌ ERROR: WordPress reported failure: " + message);
             if (responseJson.data && responseJson.data.output) { logCache("DEBUG: WP Error Output: " + responseJson.data.output); }
        } else if (responseCode >= 400) {
            success = false;
            status = "http_error";
            message = "WordPress cache clear endpoint failed (HTTP Status: " + responseCode + ")";
            if (responseText.length < 300 && responseText.trim() !== '') { message += ". Response: " + responseText.trim(); }
            logCache("❌ ERROR: " + message);
        } else {
             success = false;
             status = "unexpected_response";
             message = "Received unexpected response from WP cache clear. Code: " + responseCode + ". Body: " + responseText.substring(0, 200);
             logCache("❌ ERROR: " + message);
        }

    } catch (fetchErr) {
        if (fetchErr.message.includes("Timeout") || fetchErr.message.includes("timed out")) {
            status = "timeout_error";
            message = "Request to clear cache timed out after " + WP_ACTION_TIMEOUT + " seconds. Status uncertain.";
             logCache("⚠️ WARN: Cache clear request timed out.");
         } else {
            status = "fetch_error";
            message = "Error contacting WP cache clear endpoint: " + fetchErr.message;
            logCache("❌ EXCEPTION during cache clear call: " + message);
         }
         success = false; // Ensure success is false on any fetch error
    }

    // --- Prepare and Return Result ---
    var finalElapsedTime = ((new Date().getTime() - startTime.getTime()) / 1000).toFixed(1);
    logCache("INFO: Cache Clear Function Finished in " + finalElapsedTime + "s. Final Status: " + status);

    return {
        success: success,
        status: status,
        message: message,
        log: logCollector.join("\n")
    };
}


// =======================================================
//              TOP LEVEL & DASHBOARD WRAPPERS
// =======================================================

/********************************************************
 * Top-Level Function (Runs sheet sync only) - Kept Simple
 * This function is primarily for scheduled triggers or manual runs of *just* the sheet sync.
 * The dashboard uses runFullSync_Dashboard.
 ********************************************************/
function runFullSync() {
  Logger.log("--- Starting Full Airtable to Sheets Sync (runFullSync) ---");
  var overallStatus = true;
  var masterLog = ["Sync Run Started: " + new Date().toLocaleString()];
  var recentItemsCollector = []; // Collects summary actions

  function addLog(msg) {
    masterLog.push(msg);
    Logger.log(msg);
  }

  // Only sync sessions sheet in this basic runner
  var config = CONFIG.sessions;
  if (config) {
      var syncFunctionName = config.syncFunctionName || 'syncAirtableToSheet';
      addLog("\n--- Processing: " + config.type.toUpperCase() + " (Sheet: " + config.sheetName + ") ---");
      try {
        var result = this[syncFunctionName](config, addLog, recentItemsCollector); // `this` refers to global scope
        addLog("Result for " + config.type.toUpperCase() + ": " + (result.success ? "SUCCESS" : "FAILED"));
        if (!result.success) {
          addLog("ERROR: " + result.error);
          overallStatus = false;
        }
        addLog("Counters: " + JSON.stringify(result.counters));
      } catch (e) {
        addLog("FATAL ERROR during " + config.type + " sync: " + e.message + (e.stack ? "\nStack: " + e.stack : ""));
        overallStatus = false;
      }
  } else {
      addLog("ERROR: Configuration for 'sessions' not found.");
      overallStatus = false;
  }

  Logger.log("\n--- Recent Actions Summary (" + recentItemsCollector.length + " items) ---");
  recentItemsCollector.forEach(item => Logger.log("- " + item));

  Logger.log("--- Full Sync Run Complete (runFullSync) ---");
  Logger.log("Overall Status: " + (overallStatus ? "SUCCESS" : "FAILED"));
}


/********************************************************
 * doGet(e) - Serves the HTML dashboard UI
 ********************************************************/
function doGet(e) {
  try {
    Logger.log("doGet triggered for Dashboard.");
    var template = HtmlService.createTemplateFromFile('dashboard'); // Assumes 'dashboard.html' exists

    // Pass configuration details needed by the client-side JS
    template.sessionsImportId = CONFIG.sessions.wpImportId || ''; // Pass the specific import ID

    var htmlOutput = template.evaluate()
      .setTitle('Sessions Sync Dashboard')
      .setSandboxMode(HtmlService.SandboxMode.IFRAME)
      .addMetaTag('viewport', 'width=device-width, initial-scale=1');

    Logger.log("Successfully created HTML output for Dashboard.");
    return htmlOutput;

  } catch (error) {
    Logger.log("ERROR in doGet: " + error + ". Did you create dashboard.html? Error details: " + error.stack);
    return HtmlService.createHtmlOutput(
        "<h1>Error</h1><p>The dashboard UI could not be loaded.</p><p>Error: " +
        Utilities.encodeHtml(error.message) + "</p>")
      .setTitle('Dashboard Error');
  }
}


/********************************************************
 * getDashboardData - Retrieves last sync status for the dashboard UI
 * Reads data stored in User Properties. Includes initial WP status check.
 * @return {Object} An object containing the last sync details + initial WP status.
 ********************************************************/
function getDashboardData() {
  Logger.log("getDashboardData called.");
  const storedDataKey = 'lastSyncDashboardData_Sessions'; // Unique key
  const storedData = PropertiesService.getUserProperties().getProperty(storedDataKey);
  let initialData = null;

  // --- Load stored Sheet Sync data ---
  if (storedData) {
    try {
      initialData = JSON.parse(storedData);
      Logger.log("Found and parsed stored dashboard data.");
    } catch (e) {
      Logger.log("Error parsing stored dashboard data: " + e + ". Using default.");
      initialData = null; // Reset on parse error
    }
  }

  // --- Set default structure if no valid stored data ---
  if (!initialData) {
    initialData = {
      lastSyncTimestamp: null,
      lastSyncStatus: 'Never Run',
      lastSyncDuration: 0,
      lastSyncResults: {
        typeCounters: {}, // e.g., { sessions: { created: 0, ... } }
        totalErrors: 0,
        logs: ["<li><i data-feather='info' class='icon status-icon-info'></i> No previous sheet sync data available.</li>"],
        recentItems: ["No previous sync actions recorded."]
      }
      // wpImportStatus will be added below
    };
  }

  // --- Get initial WP Import status (if configured) ---
  let initialWpStatus = null;
  const wpImportId = CONFIG.sessions.wpImportId;
  if (wpImportId) {
      Logger.log("Attempting to get initial status for WP Import ID: " + wpImportId);
      initialWpStatus = getImportStatus(wpImportId); // Use the existing function
       if (!initialWpStatus) {
          Logger.log("No initial status found in cache for WP Import ID: " + wpImportId);
          // Set a default 'unknown' or 'idle' status if not found
           initialWpStatus = { status: 'unknown', importId: wpImportId, message: 'No status found in cache.' };
       }
  } else {
       Logger.log("WARN: Sessions WP Import ID not configured in CONFIG. Cannot fetch initial WP status.");
       initialWpStatus = { status: 'not_configured', importId: null, message: 'WP Import ID not set up.' };
  }

  // Add the WP status to the data object being returned
  initialData.wpImportStatus = initialWpStatus;

  Logger.log("Returning initial dashboard data including WP Status:", JSON.stringify(initialData).substring(0, 500) + "...");
  return initialData;
}

/********************************************************
 * runSheetSync_Dashboard - Runs ONLY the sheet sync process for the dashboard UI.
 * Stores the results and returns them to the UI.
 * @return {Object} The final sheet sync results object formatted for the dashboard.
 ********************************************************/
function runSheetSync_Dashboard() {
  Logger.log("runSheetSync_Dashboard called.");
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
      Logger.log("--- DASHBOARD TRIGGER: Starting Airtable to SHEETS Sync ---");
      var masterLogCollector = [];
      var recentItemsCollector = []; // Passed to sync function

      function addMasterLog(msg) {
          masterLogCollector.push(msg);
          Logger.log(msg);
      }

      let hasSyncErrors = false;
      const config = CONFIG.sessions; // Only run for sessions

      if (config) {
          var syncFunctionName = config.syncFunctionName || 'syncAirtableToSheet';
          addMasterLog(`\n--- Processing: ${config.type.toUpperCase()} (Sheet: ${config.sheetName}) ---`);
          let result;
          try {
              // Call the actual sync function
              result = this[syncFunctionName](config, addMasterLog, recentItemsCollector);

              aggregatedResults.typeCounters[config.type] = result.counters;

              addMasterLog(`Result for ${config.type.toUpperCase()}: ${result.success ? "SUCCESS" : "FAILED"}`);
              if (!result.success) {
                  hasSyncErrors = true;
                  aggregatedResults.totalErrors += 1;
                  if (result.error) {
                      addMasterLog(`ERROR: ${result.error}`);
                       finalErrorMessage = (finalErrorMessage ? finalErrorMessage + "; " : "") + config.type + ": " + result.error;
                  }
              }
               addMasterLog("Counters: " + JSON.stringify(result.counters));

          } catch (e) {
              addMasterLog(`FATAL ERROR during ${config.type} sync: ${e.message}${e.stack ? "\nStack: " + e.stack : ""}`);
              hasSyncErrors = true;
              aggregatedResults.totalErrors += 1;
              finalErrorMessage = (finalErrorMessage ? finalErrorMessage + "; " : "") + `Fatal error in ${config.type}: ${e.message}`;
               if (!aggregatedResults.typeCounters[config.type]) {
                   aggregatedResults.typeCounters[config.type] = { updated: 0, skipped: 0, created: 0, deleted: 0 }; // Default counters
               }
          }
      } else {
           addMasterLog("ERROR: Configuration for 'sessions' not found.");
           hasSyncErrors = true;
           aggregatedResults.totalErrors += 1;
           finalErrorMessage = "Sessions configuration missing.";
      }

      addMasterLog("\n--- Recent Actions Summary (" + recentItemsCollector.length + " items) ---");
      recentItemsCollector.forEach(item => addMasterLog("- " + item));

      addMasterLog("--- Sheet Sync Run Complete ---");
      overallStatus = hasSyncErrors ? 'Failed' : 'Success';
      addMasterLog("Overall Sheet Sync Status: " + overallStatus);

      // --- Format Logs for Dashboard ---
       aggregatedResults.logs = formatLogsForDashboard_(masterLogCollector);
       aggregatedResults.recentItems = recentItemsCollector; // Use collected items

  } catch (outerError) {
      Logger.log("FATAL ERROR in runSheetSync_Dashboard wrapper: " + outerError + (outerError.stack ? "\nStack: " + outerError.stack : ""));
      overallStatus = 'Failed';
      finalErrorMessage = "Dashboard wrapper error: " + outerError.message;
      aggregatedResults.totalErrors += 1;
      aggregatedResults.logs.push(`<li><i data-feather='x-octagon' class='icon status-icon-error'></i><span class="log-message">FATAL WRAPPER ERROR: ${Utilities.encodeHtml(outerError.message)}</span></li>`);
  }

  // --- Prepare Final Result Object ---
  const endTime = new Date();
  const duration = endTime.getTime() - startTime.getTime();

  const finalResult = {
      lastSyncTimestamp: startTime.toISOString(),
      lastSyncStatus: overallStatus,
      lastSyncDuration: duration,
      lastSyncResults: aggregatedResults,
      success: overallStatus === 'Success', // Add explicit success boolean for client
      error: overallStatus === 'Success' ? null : finalErrorMessage // Ensure error is null on success
  };

   // --- Store Result in User Properties ---
   storeDashboardData_(finalResult); // Use helper function

  Logger.log("runSheetSync_Dashboard finished. Status: " + overallStatus + ", Duration: " + duration + "ms");
  return finalResult; // Return results to client-side handler
}

/********************************************************
 * Helper: Format Log Array for Dashboard HTML
 ********************************************************/
function formatLogsForDashboard_(logArray) {
    if (!Array.isArray(logArray)) return [];
    return logArray.map(logString => {
        let icon = 'chevrons-right'; let iconClass = 'status-icon-info';
        const lowerLog = String(logString).toLowerCase(); // Ensure string conversion

        if (lowerLog.includes('error') || lowerLog.includes('failed') || lowerLog.includes('fatal')) { icon = 'alert-triangle'; iconClass = 'status-icon-error'; }
        else if (lowerLog.includes('warn')) { icon = 'alert-circle'; iconClass = 'status-icon-warning'; }
        else if (lowerLog.includes('success') || lowerLog.includes('complete')) { icon = 'check-circle'; iconClass = 'status-icon-complete'; }
        else if (lowerLog.includes('info:') || lowerLog.includes('processing') || lowerLog.includes('starting')) { icon = 'info'; iconClass = 'status-icon-info'; }
        else if (lowerLog.includes('created') || lowerLog.includes('added ')) { icon = 'plus-circle'; iconClass = 'status-icon-new'; }
        else if (lowerLog.includes('updated')) { icon = 'edit-2'; iconClass = 'status-icon-updated'; }
        else if (lowerLog.includes('deleted') || lowerLog.includes('removed ')) { icon = 'trash-2'; iconClass = 'status-icon-error'; }
        else if (lowerLog.includes('skipped') || lowerLog.includes('no changes')) { icon = 'skip-forward'; iconClass = 'status-icon-skipped'; }
        else if (lowerLog.includes('trigger') || lowerLog.includes('initiat')) { icon = 'play'; iconClass = 'status-icon-info'; }
        else if (lowerLog.includes('cache')) { icon = 'database'; iconClass = 'status-icon-info'; }
        else if (lowerLog.includes('cancel')) { icon = 'stop-circle'; iconClass = 'status-icon-warning'; }

        const message = String(logString).replace(/</g, "<").replace(/>/g, ">"); // Basic HTML escaping
        return `<li><i data-feather='${icon}' class='icon ${iconClass}'></i><span class="log-message">${message}</span></li>`;
    });
}

/********************************************************
 * Helper: Store Dashboard Data with Truncation Fallback
 ********************************************************/
function storeDashboardData_(resultObject) {
   const dataKey = 'lastSyncDashboardData_Sessions';
   try {
       PropertiesService.getUserProperties().setProperty(dataKey, JSON.stringify(resultObject));
       Logger.log("Stored latest dashboard results in User Properties.");
   } catch (e) {
       Logger.log("WARN: Failed to store full dashboard data (might exceed size limits): " + e);
       try {
            let truncatedResult = JSON.parse(JSON.stringify(resultObject)); // Deep copy
            // Truncate potentially large fields
            if (truncatedResult.lastSyncResults && truncatedResult.lastSyncResults.logs) {
               truncatedResult.lastSyncResults.logs = ["<li>Log truncated due to storage limits. Check execution logs.</li>"];
            }
            if (truncatedResult.lastSyncResults && truncatedResult.lastSyncResults.recentItems) {
                truncatedResult.lastSyncResults.recentItems = ["Recent items truncated due to storage limits."];
            }
            PropertiesService.getUserProperties().setProperty(dataKey, JSON.stringify(truncatedResult));
            Logger.log("Stored TRUNCATED dashboard results in User Properties.");
       } catch (e2) {
            // If even truncated fails, log the error but don't stop the script
            Logger.log("ERROR: Failed to store even truncated dashboard data: " + e2);
            // Optionally add a warning to the logs being returned to the UI if possible
       }
   }
}

// --- Add onFailure (Optional but good practice) ---
function onFailure(error) {
  Logger.log("Client-side Script Error: " + error + " | Stack: " + (error.stack ? error.stack : '(no stack)'));
}



/********************************************************
 * clearSpecificImportCache - Requests the Import Handler library to clear
 *                            the cache for a specific import ID.
 * Intended as a recovery tool if an import gets stuck in 'pending'.
 * @param {string} importId - The WP All Import numerical ID to clear.
 * @return {object} Result object { success: boolean, message: string } from the library call.
 ********************************************************/
function clearSpecificImportCache(importId) {
  const logPrefix = "DASHBOARD [clearSpecificImportCache]: ";

  if (!importId) {
    Logger.log(logPrefix + "ERROR - No Import ID provided.");
    return { success: false, message: "Import ID missing. Cannot request cache clear." };
  }
  importId = String(importId); // Ensure string

  Logger.log(logPrefix + "Requesting Handler Library to clear cache for Import ID: '" + importId + "'");

  try {
    // --- Call the new Library Function ---
    var clearResult = ImportHandlerLib.clearImportStatusInCache(importId);
    // --- End Library Call ---

    // Log the result received from the library
    Logger.log(logPrefix + "Received response from Library clear request: Success=" + clearResult.success + ", Message=" + clearResult.message);

    // Return the result object directly from the library
    return clearResult;

  } catch (e) {
    // Handle errors during the library call itself
    Logger.log(logPrefix + "ERROR calling ImportHandlerLib.clearImportStatusInCache for ID '" + importId + "': " + e);
    let message = "Error requesting cache clear via library: " + e.message;
    // Add specific checks if needed (like in getImportStatus error handling)
    if (e.message.includes("ImportHandlerLib is not defined")) { message = "Library configuration error: " + e.message; }
    else if (e.message.includes("You do not have permission")) { message = "Library permission error: " + e.message; }

    // Return an error object
    return { success: false, message: message };
  }
}
