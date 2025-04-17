/*******************************************************************************
 * Sessions Sync Dashboard (Phase 2: WP Import Control) - v1.2 WP Integration
 * Description: Fetches data from Airtable, syncs to Google Sheet, triggers
 *              WP All Import, monitors status via cache, and allows cache clearing.
 * Based On:    Sessions Sync v1.1 & Events Sync Dashboard v2.0 structure.
 * Version:     1.2 - Sessions (WP Import Integration)
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
const WP_IMPORT_BASE_URL = 'https://wordpress-1204105-4784464.cloudwaysapps.com//wp-load.php'; // !! ACTION: Replace with your actual WP site URL (e.g., https://four12global.com/wp-load.php) !!
// const SESSIONS_WP_IMPORT_ID = 'YOUR_IMPORT_ID'; // !! ACTION: Replace with the WP All Import ID for Sessions (Removed from here - Will be passed from UI) !!
const WP_ACTION_TIMEOUT = 45; // Seconds for actions like cache clear, initiate call

// --- Script Configuration (Existing + Cache) ---
const MAX_FETCH_RETRIES = 3;
const BASE_RETRY_DELAY_MS = 500;
const INTER_PAGE_DELAY_MS = 200;
const CACHE_EXPIRATION_SECONDS = 21600; // 6 hours for WP Import status cache

// --- Cache Service Reference --- // <-- ADDED
var cache = CacheService.getScriptCache();

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

/********************************************************
 * Secret Management (Run Once - Updated)
 * Stores sensitive information in Script Properties.
 ********************************************************/
function storeSecrets() {
  try {
    // !! IMPORTANT !! Replace with your actual API Token and WP Key before running MANUALLY from the editor!
    const tokenToStore = 'patuJ7uJDTaYRV1d9.91bb89245dfe18f65a7fb47ec75ddcc7c6a0a0fe1aeded3af7fda6b5578f556a'; // <--- PASTE TOKEN HERE
    const wpKeyToStore = 'DF4J01r';   // <--- PASTE WP KEY HERE (e.g., DF4J01r)

    if (tokenToStore === 'PASTE_YOUR_AIRTABLE_API_TOKEN_HERE' || !tokenToStore) {
        Logger.log("WARN: Please paste your actual Airtable API token into the script before running storeSecrets().");
        return;
    }
    if (wpKeyToStore === 'PASTE_YOUR_WP_IMPORT_KEY_HERE' || !wpKeyToStore) { // <-- ADDED CHECK
        Logger.log("WARN: Please paste your actual WP Import Key into the script before running storeSecrets().");
        return;
    }

    const scriptProperties = PropertiesService.getScriptProperties();
    scriptProperties.setProperty('AIRTABLE_API_TOKEN', tokenToStore);
    scriptProperties.setProperty('WP_IMPORT_KEY', wpKeyToStore); // <-- ADDED

    Logger.log("Secret 'AIRTABLE_API_TOKEN' stored successfully.");
    Logger.log("Secret 'WP_IMPORT_KEY' stored successfully."); // <-- ADDED

  } catch (e) {
    Logger.log("ERROR storing secrets: " + e);
    throw new Error("Failed to store secrets. Check logs and script permissions.");
  }
}


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
 * syncAirtableToSheet (CORE SHEET SYNC FUNCTION)
 * Fetches data, processes using simple formatting, and performs incremental
 * sync to the target Google Sheet using native Airtable Record ID.
 *******************************************************************************/
 function syncAirtableToSheet(config, addLog, recentItems) {
    var counters = { updated: 0, skipped: 0, created: 0, deleted: 0 };
    var logArray = [];

    // --- Internal logging helper ---
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

    // --- Configuration Validation ---
    const primaryTitleField = 'title'; // <-- Ensure this matches your title field name
    if (!config || !config.airtable || !config.airtable.baseId || !config.airtable.tableId || !config.airtable.timestampField || !config.sheetName || !Array.isArray(config.airtable.fields)) {
        logEntry("ERROR: Invalid configuration provided to syncAirtableToSheet.");
        return { success: false, error: "Configuration Error: Missing required properties.", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }
    if (!config.airtable.fields.includes(config.airtable.timestampField)) {
        logEntry("ERROR: Timestamp field '" + config.airtable.timestampField + "' must be included in airtable.fields config.");
        return { success: false, error: "Configuration Error: Timestamp field missing from fields list.", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }
    if (!config.airtable.fields.includes(primaryTitleField)) {
        logEntry(`WARN: The primary title field '${primaryTitleField}' is not listed in config.airtable.fields. Log messages for recent items might use Record IDs instead of titles.`);
    }

    logEntry("INFO: Starting sync to sheet '" + config.sheetName + "'...");
    var airtableRecords = [];

    // --- 1. Fetch Airtable Data ---
    try {
        logEntry("INFO: Fetching data from Airtable table: " + config.airtable.tableId + (config.airtable.viewName ? " (View: " + config.airtable.viewName + ")" : ""));
        var apiUrl = 'https://api.airtable.com/v0/' + config.airtable.baseId + '/' + encodeURIComponent(config.airtable.tableId);
        airtableRecords = fetchAirtableData_(apiUrl, config.airtable.fields, config.airtable.viewName);
    } catch (fetchErr) {
        logEntry("ERROR: Failed to fetch data from Airtable: " + fetchErr.message + (fetchErr.stack ? "\nStack: " + fetchErr.stack : ""));
        if (recentItems && typeof recentItems.push === 'function') recentItems.push(`Sync Error (${config.type}): Failed to fetch data from Airtable.`);
        return { success: false, error: "Airtable Fetch Error: " + fetchErr.message, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }

    // --- 2. Define Target Header and Process Data ---
    logEntry("INFO: Processing " + airtableRecords.length + " fetched records...");
    var newData = [];
    var targetHeader = ["AirtableRecordID"]; // Always include the native ID column first
    var configuredFieldsSet = new Set(config.airtable.fields); // Use Set for efficient lookup

    // Ensure target header follows the order specified in config.airtable.fields
    const orderedFields = config.airtable.fields.filter(f => configuredFieldsSet.has(f));
    targetHeader = targetHeader.concat(orderedFields); // Concatenate in the config order

    newData.push(targetHeader); // Add header row
    logEntry("INFO: Target header defined with " + targetHeader.length + " columns: " + targetHeader.join(', '));

    // Create index maps for faster lookups later
    const targetHeaderIndexMap = targetHeader.reduce((map, header, index) => { map[header] = index; return map; }, {});
    const recordIdColIndex_Target = 0; // AirtableRecordID is always at index 0 in target data
    const timestampColIndex_Target = targetHeaderIndexMap[config.airtable.timestampField];
    const titleColIndex_Target = targetHeaderIndexMap[primaryTitleField]; // Index of 'title' in the new data structure

    if (timestampColIndex_Target === undefined) {
        logEntry("ERROR: Could not find index for timestamp field '" + config.airtable.timestampField + "' in target header.");
        return { success: false, error: "Internal Error: Timestamp index mapping failed.", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }
    if (titleColIndex_Target === undefined) {
        logEntry(`WARN: Cannot find index for primary title field '${primaryTitleField}' in target header. Log messages may use Record IDs.`);
    }

    // Process each Airtable record into the target row format
    airtableRecords.forEach(function (record, index) {
        var fields = record.fields || {};
        var airtableNativeId = record.id;

        if (!airtableNativeId) {
            logEntry("WARN: Skipping Airtable record at index " + index + " due to missing native Record ID.");
            return; // Skip record if it doesn't have a native ID (shouldn't happen)
        }

        // Create the new row based on the targetHeader order
        var newRowArray = targetHeader.map(headerName => {
            if (headerName === "AirtableRecordID") {
                return airtableNativeId;
            } else {
                // Use the simple formatFieldValue_ as linked records are handled by Lookups
                return formatFieldValue_(fields[headerName]);
            }
        });

        newData.push(newRowArray);
    });
    logEntry("INFO: Finished processing. New data structure has " + (newData.length - 1) + " data rows.");

    // --- 3. Sync with Google Sheet ---
    if (newData.length <= 1) { // Check if only header row exists
        logEntry("INFO: No data rows processed from Airtable. Sync finished.");
        if (recentItems && typeof recentItems.push === 'function') recentItems.push(`${config.type}: No data found in Airtable source.`);
        return { success: true, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") }; // Considered success as the process ran
    }

    // Get sheet handle, create if needed
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName(config.sheetName);
    var sheetCreated = false;
    if (!sheet) {
        try {
            sheet = ss.insertSheet(config.sheetName);
            sheetCreated = true;
            logEntry("INFO: Created new sheet: " + config.sheetName);
        } catch (e) {
            logEntry("ERROR: Failed to create sheet '" + config.sheetName + "': " + e.message);
            if (recentItems && typeof recentItems.push === 'function') recentItems.push(`Sync Error (${config.type}): Failed to create sheet.`);
            return { success: false, error: "Sheet Creation Error: " + e.message, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
        }
    }

    // Read existing data
    var existingData = [];
    var existingHeader = [];
    if (!sheetCreated && sheet.getLastRow() > 0) {
        try {
            existingData = sheet.getDataRange().getValues();
            existingHeader = existingData[0].map(String); // Get header only if data exists
            logEntry("INFO: Fetched " + existingData.length + " existing rows (incl. header) from sheet '" + config.sheetName + "'.");
        } catch (e) {
            logEntry("ERROR: Failed to read existing data from sheet '" + config.sheetName + "': " + e.message);
            if (recentItems && typeof recentItems.push === 'function') recentItems.push(`Sync Error (${config.type}): Failed to read sheet.`);
            return { success: false, error: "Sheet Read Error: " + e.message, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
        }
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
        var newHeader = targetHeader.map(String); // Ensure comparison is string-based
        if (JSON.stringify(existingHeader) !== JSON.stringify(newHeader)) {
            performFullWrite = true;
            logEntry("WARN: Headers differ between Airtable data and sheet '" + config.sheetName + "'. Performing full data rewrite.");
            logEntry("DEBUG Existing Header: " + existingHeader.join(', '));
            logEntry("DEBUG New Header:      " + newHeader.join(', '));
            counters.deleted = existingData.length - 1; // Log conceptual deletion of old rows
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
            const requiredCols = targetHeader.length; // newData[0] should always exist here

            // Adjust rows
            const currentMaxRows = sheet.getMaxRows();
            if (currentMaxRows < requiredRows) {
                sheet.insertRowsAfter(currentMaxRows, requiredRows - currentMaxRows);
                 logEntry(`DEBUG: Added ${requiredRows - currentMaxRows} rows.`);
            } else if (currentMaxRows > requiredRows && requiredRows > 0) {
                // Delete extra rows only if there are rows left
                 sheet.deleteRows(requiredRows + 1, currentMaxRows - requiredRows);
                 logEntry(`DEBUG: Deleted ${currentMaxRows - requiredRows} rows.`);
            } else if (requiredRows == 0 && currentMaxRows > 0) {
                // If new data is empty, delete all existing rows
                sheet.deleteRows(1, currentMaxRows);
                logEntry(`DEBUG: Deleted all ${currentMaxRows} rows as new data is empty.`);
            }


            // Adjust columns
            const currentMaxCols = sheet.getMaxColumns();
             if (currentMaxCols < requiredCols) {
                sheet.insertColumnsAfter(currentMaxCols, requiredCols - currentMaxCols);
                 logEntry(`DEBUG: Added ${requiredCols - currentMaxCols} columns.`);
            } else if (currentMaxCols > requiredCols && requiredCols > 0) {
                sheet.deleteColumns(requiredCols + 1, currentMaxCols - requiredCols);
                 logEntry(`DEBUG: Deleted ${currentMaxCols - requiredCols} columns.`);
            } else if (requiredCols == 0 && currentMaxCols > 0) {
                 // If new data is empty, delete all existing columns
                 sheet.deleteColumns(1, currentMaxCols);
                 logEntry(`DEBUG: Deleted all ${currentMaxCols} columns as new data is empty.`);
            }


            // Write new data (only if there's data to write)
            if (requiredRows > 0 && requiredCols > 0) {
                sheet.getRange(1, 1, requiredRows, requiredCols).setValues(newData);
                sheet.setFrozenRows(1); // Re-freeze header
            } else {
                 logEntry("INFO: No data rows/columns to write after clearing/resizing.");
            }

            counters.created = newData.length - 1; // All non-header rows are new in a full write
            const writeAction = (sheetCreated || existingData.length === 0) ? "written to new/empty sheet" : "rewritten sheet";
            logEntry(`INFO: Data ${writeAction} successfully. ${counters.created} records created.`);
            if (recentItems && typeof recentItems.push === 'function') recentItems.push(`${sheetCreated ? "Created" : (existingData.length === 0 ? "Populated empty" : "Rewrote")} sheet '${config.sheetName}' with ${counters.created} records.`);

        } catch (e) {
            const errorContext = performFullWrite && !sheetCreated && existingData.length > 0 ? 'rewriting sheet' : 'writing to new/empty sheet';
            logEntry(`ERROR ${errorContext} '${config.sheetName}': ${e.message}${e.stack ? " | Stack: " + e.stack : ""}`);
            if (recentItems && typeof recentItems.push === 'function') recentItems.push(`Sync Error (${config.type}): Failed ${performFullWrite ? 'rewriting' : 'writing'} sheet.`);
            return { success: false, error: `Failed ${errorContext} data to '${config.sheetName}': ${e.message}`, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
        }
        // If full write was successful, return
        return { success: true, counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }

    // --- Execute Incremental Update (only if not a full write) ---
    logEntry("INFO: Preparing for incremental update...");
    // Indices based on the *existing* sheet header (which we confirmed matches the new header)
    const sheetRecordIdIndex = existingHeader.indexOf("AirtableRecordID"); // Should be 0
    const sheetTimestampIndex = existingHeader.indexOf(config.airtable.timestampField);
    const sheetTitleIndex = existingHeader.indexOf(primaryTitleField);

    if (sheetRecordIdIndex === -1) { // Should not happen if header check passed, but safety first
        logEntry("ERROR: Cannot perform incremental update: 'AirtableRecordID' column not found in existing sheet header.");
        return { success: false, error: "Internal Error: Sheet Record ID index missing.", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }
     if (sheetTimestampIndex === -1) { // Should not happen
        logEntry("ERROR: Cannot perform incremental update: Timestamp field '" + config.airtable.timestampField + "' not found in existing sheet header.");
        return { success: false, error: "Internal Error: Sheet Timestamp index missing.", counters: counters, recentItems: recentItems || [], log: logArray.join("\n") };
    }
     if (sheetTitleIndex === -1) {
         logEntry(`WARN: Cannot find index for primary title field '${primaryTitleField}' in sheet header. Log messages may use Record IDs.`);
     }

    // Build Map of Existing Records from the Sheet
    var existingMap = {}; // key: AirtableRecordID, value: { rowIndex: number, timestamp: string (standardized) }
    for (var i = 1; i < existingData.length; i++) { // Start from row 1 (skip header)
        var existingRow = existingData[i];
        // Basic validation: ensure row has enough columns
        if (existingRow.length <= sheetRecordIdIndex || existingRow.length <= sheetTimestampIndex) {
             logEntry(`WARN: Skipping existing sheet row ${i+1} - row has only ${existingRow.length} columns, needs at least ${Math.max(sheetRecordIdIndex, sheetTimestampIndex) + 1}.`);
             continue;
        }
        var recID = existingRow[sheetRecordIdIndex];
        if (recID && String(recID).trim() !== '') {
            var standardizedExistingTimestamp = standardizeTimestampForComparison_(existingRow[sheetTimestampIndex], recID, `sheet row ${i+1}`);
            existingMap[recID] = {
                rowIndex: i + 1, // 1-based index for sheet ranges
                timestamp: standardizedExistingTimestamp // Use the standardized timestamp
            };
        } else {
            // logEntry(`DEBUG: Skipping existing sheet row ${i+1} due to missing/empty RecordID.`); // Can be noisy
        }
    }
    logEntry("INFO: Built map of " + Object.keys(existingMap).length + " existing records from sheet for comparison.");


    // Compare New Data and Prepare Batches for Update/Append/Delete
    var rowsToUpdate = []; // Array of { range: 'A1Notation', values: [[...]] }
    var rowsToAppend = []; // Array of [[...], [...]] (new rows to add at the end)
    var recordIdsToKeep = new Set(); // Track IDs present in the new Airtable data
    const entityTypeCapitalized = capitalizeFirstLetter(config.type); // For user-friendly logs

    for (var i = 1; i < newData.length; i++) { // Start from row 1 (skip header)
        var newRow = newData[i];
        var newRecordID = newRow[recordIdColIndex_Target]; // Get ID from the *new* data (index 0)

        if (!newRecordID || String(newRecordID).trim() === '') {
             logEntry(`WARN: Skipping new data row ${i+1} due to missing/empty RecordID.`);
             continue; // Skip if ID is missing
        }

        recordIdsToKeep.add(newRecordID); // Mark this ID as present in the latest Airtable data

        // Standardize the timestamp from the *new* data for comparison
        var standardizedNewTimestamp = standardizeTimestampForComparison_(newRow[timestampColIndex_Target], newRecordID, `new data row ${i+1}`);

        // Get a title for logging, fall back to Record ID if title field is missing/empty
        var recordTitleForLog = (titleColIndex_Target !== undefined && newRow.length > titleColIndex_Target && newRow[titleColIndex_Target]) ? newRow[titleColIndex_Target] : newRecordID;

        var existingRecord = existingMap[newRecordID];

        if (existingRecord) {
            // Record exists in the sheet, check if timestamp differs
            if (existingRecord.timestamp !== standardizedNewTimestamp) {
                // Timestamps differ, mark for update
                // Note: newRow is already in the correct targetHeader order
                var rangeNotation = sheet.getRange(existingRecord.rowIndex, 1, 1, targetHeader.length).getA1Notation();
                rowsToUpdate.push({ range: rangeNotation, values: [newRow] });
                counters.updated++;
                if (recentItems && recentItems.length < 150) recentItems.push(`Updated ${entityTypeCapitalized}: '${recordTitleForLog}'`);
                // logEntry(`DEBUG: Marked row ${existingRecord.rowIndex} (${newRecordID} - ${recordTitleForLog}) for update. Sheet TS: '${existingRecord.timestamp}', New TS: '${standardizedNewTimestamp}'`);
            } else {
                // Timestamps match, skip update
                counters.skipped++;
                 // logEntry(`DEBUG: Skipped record ${newRecordID} ('${recordTitleForLog}') - Timestamp matched ('${standardizedNewTimestamp}').`);
            }
        } else {
            // Record does not exist in the sheet, mark for creation (append)
            // Note: newRow is already in the correct targetHeader order
            rowsToAppend.push(newRow);
            counters.created++;
            if (recentItems && recentItems.length < 150) recentItems.push(`Added ${entityTypeCapitalized}: '${recordTitleForLog}'`);
             logEntry(`INFO: Marked record ${newRecordID} ('${recordTitleForLog}') for creation.`);
        }
    } // End comparison loop

    // Determine Rows to Delete (those in existingMap but not in recordIdsToKeep)
    var rowsToDeleteIndices = [];
    for (var recID in existingMap) {
        if (!recordIdsToKeep.has(recID)) {
            let existingInfo = existingMap[recID];
            rowsToDeleteIndices.push(existingInfo.rowIndex);
            counters.deleted++;

            // Try to get the title of the item being deleted for logging
            var deletedTitle = recID; // Fallback to ID
            try {
                 // Ensure row index is valid and title column exists before fetching
                 if (existingInfo.rowIndex > 0 && existingInfo.rowIndex <= sheet.getLastRow() && sheetTitleIndex !== -1 && sheetTitleIndex < sheet.getLastColumn()) {
                    var titleValue = sheet.getRange(existingInfo.rowIndex, sheetTitleIndex + 1).getValue(); // sheetTitleIndex is 0-based, getRange col is 1-based
                    if (titleValue && String(titleValue).trim() !== '') {
                        deletedTitle = String(titleValue).trim();
                    }
                 }
            } catch(fetchErr) {
                logEntry(`WARN: Could not fetch title for deleted row ${existingInfo.rowIndex} (ID: ${recID}): ${fetchErr}`);
            }

            if (recentItems && recentItems.length < 150) recentItems.push(`Removed ${entityTypeCapitalized}: '${deletedTitle}'`);
             logEntry(`INFO: Marked row ${existingInfo.rowIndex} (ID: ${recID}, Title: '${deletedTitle}') for deletion.`);
        }
    }
    rowsToDeleteIndices.sort((a, b) => b - a); // Sort descending for safe deletion from bottom up
    logEntry("INFO: Sync Analysis complete. Update: " + counters.updated + ", Create: " + counters.created + ", Delete: " + counters.deleted + ", Skipped: " + counters.skipped);

    // Perform Batch Operations
    var updateError = null, appendError = null, deleteError = null;
    var operationsPerformed = false;

    // --- Batch Updates ---
    if (rowsToUpdate.length > 0) {
        operationsPerformed = true;
        logEntry("INFO: Applying " + rowsToUpdate.length + " updates...");
        try {
             // Consider using RangeList for very large updates? For moderate numbers, sequential is ok.
             rowsToUpdate.forEach(update => {
                 sheet.getRange(update.range).setValues(update.values);
                 // Utilities.sleep(50); // Optional slight pause for very large updates
             });
             logEntry("INFO: " + rowsToUpdate.length + " updates applied successfully.");
        } catch (e) {
             logEntry("ERROR applying batch updates: " + e.message + (e.stack ? " | Stack: " + e.stack : ""));
             updateError = e;
             if (recentItems) recentItems.push(`Sync Error (${config.type}): Failed during updates.`);
        }
    }

    // --- Batch Appends ---
    if (rowsToAppend.length > 0) {
        operationsPerformed = true;
         logEntry("INFO: Appending " + rowsToAppend.length + " new rows...");
        try {
            var startRow = sheet.getLastRow() + 1;
            // Ensure sheet has enough rows/columns for appended data
            let requiredEndRow = startRow + rowsToAppend.length - 1;
            if(sheet.getMaxRows() < requiredEndRow ) {
                sheet.insertRowsAfter(sheet.getMaxRows(), requiredEndRow - sheet.getMaxRows());
                logEntry(`DEBUG: Added ${requiredEndRow - sheet.getMaxRows()} rows for append.`);
            }
            // Ensure enough columns (targetHeader.length is the required number)
            if (sheet.getMaxColumns() < targetHeader.length) {
                sheet.insertColumnsAfter(sheet.getMaxColumns(), targetHeader.length - sheet.getMaxColumns());
                logEntry(`DEBUG: Added ${targetHeader.length - sheet.getMaxColumns()} columns for append.`);
            }

            sheet.getRange(startRow, 1, rowsToAppend.length, targetHeader.length).setValues(rowsToAppend);
             logEntry("INFO: " + rowsToAppend.length + " appends applied successfully.");
        } catch (e) {
             logEntry("ERROR applying batch appends: " + e.message + (e.stack ? " | Stack: " + e.stack : ""));
             appendError = e;
             if (recentItems) recentItems.push(`Sync Error (${config.type}): Failed during appends.`);
        }
    }

    // --- Batch Deletes ---
    if (rowsToDeleteIndices.length > 0) {
        operationsPerformed = true;
         logEntry("INFO: Deleting " + rowsToDeleteIndices.length + " rows...");
        try {
             rowsToDeleteIndices.forEach(function(rowIndex) {
                 // Double check row index validity before attempting deletion
                 if (rowIndex > 0 && rowIndex <= sheet.getLastRow()) {
                     sheet.deleteRow(rowIndex);
                     // Utilities.sleep(50); // Optional pause for many deletions
                 } else {
                      logEntry("WARN: Skipped deletion of row index " + rowIndex + " as it seems invalid or already deleted.");
                 }
             });
             logEntry("INFO: " + rowsToDeleteIndices.length + " deletes applied successfully.");
        } catch (e) {
             logEntry("ERROR deleting rows: " + e.message + (e.stack ? " | Stack: " + e.stack : ""));
             deleteError = e;
             if (recentItems) recentItems.push(`Sync Error (${config.type}): Failed during deletes.`);
        }
    }

    // --- Final Logging & Return ---
    if (counters.skipped > 0) logEntry("INFO: Skipped " + counters.skipped + " records (timestamp matched).");
    if (!operationsPerformed && counters.skipped > 0) {
        logEntry("INFO: No changes needed for sheet '" + config.sheetName + "'.");
        if (recentItems && recentItems.length < 150) recentItems.push(config.type + ": No changes detected (" + counters.skipped + " checked).");
    } else if (operationsPerformed) {
         logEntry("INFO: Incremental sync operations completed.");
    }

    // Truncate recent items list if it gets too long
    if (recentItems && recentItems.length >= 150 && !recentItems.some(item => item.startsWith("..."))) {
        recentItems.push("... (Action summary list truncated)");
    }

    logEntry("INFO: Sync completed. Final Counts: " + JSON.stringify(counters));

    // Determine overall success based on whether any batch operation failed critically
    var overallSuccess = !updateError && !appendError && !deleteError;
    var errorMessages = [updateError, appendError, deleteError].filter(Boolean).map(e => `${config.type}: ${e.message}`);
    var combinedErrorMessage = overallSuccess ? null : errorMessages.join('; ');

    return {
        success: overallSuccess,
        error: combinedErrorMessage,
        counters: counters,
        recentItems: recentItems, // Return the potentially modified recentItems array
        log: logArray.join("\n")
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
 * getImportStatus - Reads status from CacheService for the UI
 * @param {string} importId - The WP All Import numerical ID.
 * @return {object|null} Parsed status object from cache or null/error object.
 ********************************************************/
function getImportStatus(importId) {
    if (!importId) {
        Logger.log("WARN [getImportStatus]: Called without importId.");
        return { status: 'error', message: 'Import ID missing.', importId: null };
     }
     importId = String(importId);
     var cacheKey = 'import_status_' + importId;
     Logger.log("DEBUG [getImportStatus]: Checking cache key: " + cacheKey);

    try {
        var cachedData = cache.get(cacheKey);
        if (cachedData) {
             Logger.log("DEBUG [getImportStatus]: Cache hit for " + cacheKey);
             var parsedData = JSON.parse(cachedData);
             if (parsedData && parsedData.status) {
                parsedData.cacheCheckTime = new Date().toISOString(); // Add check time
                return parsedData; // Return valid status object
             } else {
                 Logger.log("WARN [getImportStatus]: Cache data invalid (key: " + cacheKey + "): " + cachedData);
                 return null; // Treat as cache miss
             }
        } else {
             Logger.log("DEBUG [getImportStatus]: Cache miss for " + cacheKey);
             return null; // Not found in cache
        }
    } catch (e) {
        Logger.log("ERROR [getImportStatus]: Cache read/parse error (ID " + importId + "): " + e);
        return { status: 'error', message: "Cache read/parse error: " + e.message, importId: importId };
    }
}


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
                        '&action=clear_breeze_cache' + // Matches PHP plugin
                        '&rand=' + Math.random();

    logCache("INFO: Calling WP cache clear endpoint...");
    Logger.log("DEBUG: Cache Clear URL: " + clearCacheUrl);

    // --- Set Fetch Options ---
    var options = {
        method: 'get', muteHttpExceptions: true,
        headers: { 'User-Agent': 'GoogleAppsScript-SessionsSyncDashboard/1.2-CacheClear' },
        deadline: WP_ACTION_TIMEOUT
    };

    // --- Make Request ---
    try {
        var response = UrlFetchApp.fetch(clearCacheUrl, options);
        var responseCode = response.getResponseCode();
        var responseText = response.getContentText() || '(No response body)';
        var responseJson = null;
        logCache("INFO: WP Cache Clear Response Code: " + responseCode);
        Logger.log("DEBUG: WP Cache Clear Response Body: " + responseText);

        try { responseJson = JSON.parse(responseText); } catch (parseErr) {
             logCache("WARN: Could not parse JSON response from cache clear: " + parseErr.message);
        }

        // Check response based on PHP plugin's wpaip_send_json_success/error format
        if (responseCode === 200 && responseJson && responseJson.success === true) {
            success = true; status = "success";
            message = (responseJson.data && responseJson.data.message) ? responseJson.data.message : "Cache cleared successfully.";
            logCache("✅ SUCCESS: WP confirmed cache clear.");
        } else if (responseCode === 200 && responseJson && responseJson.success === false) {
            success = false; status = "wp_error";
            message = (responseJson.data && responseJson.data.message) ? responseJson.data.message : "WP reported error during cache clear.";
            logCache("❌ ERROR: WP reported failure: " + message);
        } else if (responseCode >= 400) {
            success = false; status = "http_error";
            message = "Cache clear endpoint failed (HTTP Status: " + responseCode + ")";
            logCache("❌ ERROR: " + message);
        } else {
             success = false; status = "unexpected_response";
             message = "Unexpected response from WP cache clear. Code: " + responseCode;
             logCache("❌ ERROR: " + message);
        }
    } catch (fetchErr) {
         if (fetchErr.message.includes("Timeout") || fetchErr.message.includes("timed out")) {
            status = "timeout_error";
            message = "Request to clear cache timed out. Status uncertain.";
            logCache("⚠️ WARN: Cache clear request timed out.");
         } else {
            status = "fetch_error";
            message = "Error contacting WP cache clear: " + fetchErr.message;
            logCache("❌ EXCEPTION during cache clear call: " + message);
         }
         success = false;
    }

    // --- Prepare Result ---
    var finalElapsedTime = ((new Date().getTime() - startTime.getTime()) / 1000).toFixed(1);
    logCache("INFO: Cache Clear Finished. Elapsed: " + finalElapsedTime + "s. Status: " + status);

    return { success: success, status: status, message: message, log: logCollector.join("\n") };
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
 * doPost(e) - Web app entry point for receiving data from WordPress
 * Handles the callback after WP All Import finishes.
 ********************************************************/
function doPost(e) {
  var receivedTimestamp = new Date();
  var rawData = '';
  var parsedData = null;
  var parseError = null;
  var sheetUpdateError = null;
  var cacheError = null;
  var importId = null; // Determined after parsing

  // Use specific prefix for callback logs
  const logPrefix = "CALLBACK [doPost]: ";

  try {
    // --- Log Raw Data ---
    if (e && e.postData && e.postData.contents) {
      rawData = e.postData.contents;
      Logger.log(logPrefix + "Received raw data at " + receivedTimestamp.toISOString() + ". Length: " + rawData.length);
      Logger.log(logPrefix + "Raw data snippet: " + rawData.substring(0, 500)); // Log a larger snippet
    } else {
      rawData = "No postData received or e object was undefined.";
      Logger.log(logPrefix + "WARN - No postData received at " + receivedTimestamp.toISOString());
      logToDoPostVerification(receivedTimestamp, rawData, null, "No postData received", null, null);
      return ContentService.createTextOutput("Error: No data received by Google Apps Script.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- Attempt to Parse JSON ---
    try {
      parsedData = JSON.parse(rawData);
      importId = parsedData ? String(parsedData.import_id || '') : ''; // Get importId, ensure string
      Logger.log(logPrefix + "Successfully parsed JSON for Import ID: '" + (importId || 'N/A') + "'");
    } catch (jsonEx) {
      parseError = "Error parsing JSON: " + jsonEx;
      Logger.log(logPrefix + "ERROR - " + parseError + " | Raw Data Snippet: " + rawData.substring(0,200));
      // Log to verification sheet before returning error
      logToDoPostVerification(receivedTimestamp, rawData, null, parseError, null, null);
      return ContentService.createTextOutput("Error: Could not parse received JSON by Google Apps Script.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- Process Parsed Data (Update Sheet & Cache) ---
    // **Crucial Check**: Ensure we have an import ID AND an end_time signifying completion
    if (importId && parsedData.end_time) {
       Logger.log(logPrefix + "Processing completed import data for ID: " + importId);

       // 1. Update the dedicated import log sheet
       var sheetUpdateResult = updateGoogleSheetWithImportData(parsedData); // Use the correct helper
       if (!sheetUpdateResult.success) {
           sheetUpdateError = sheetUpdateResult.error;
           Logger.log(logPrefix + "ERROR - Failed updating Google Sheet (" + SESSIONS_IMPORT_LOG_SHEET_NAME + ") for Import ID " + importId + ": " + sheetUpdateError);
       } else {
           Logger.log(logPrefix + "Successfully updated Google Sheet (" + SESSIONS_IMPORT_LOG_SHEET_NAME + ") for Import ID " + importId);
       }

       // 2. Update the cache with the completion status
       var cacheUpdateResult = updateCacheWithImportData(parsedData); // Use the correct helper
        if (!cacheUpdateResult.success) {
           cacheError = cacheUpdateResult.error;
           Logger.log(logPrefix + "ERROR - Failed updating Cache for Import ID " + importId + ": " + cacheError);
       } else {
           Logger.log(logPrefix + "Successfully updated Cache for Import ID " + importId);
       }

    } else {
       // Log if essential data is missing for completion processing
       var missingDataMsg = "Callback received, but missing essential fields for completion processing.";
       if (!importId) missingDataMsg += " Missing: import_id.";
       if (parsedData && !parsedData.end_time) missingDataMsg += " Missing: end_time.";
       // Treat as warnings rather than errors for response, but log issues
       sheetUpdateError = missingDataMsg;
       cacheError = missingDataMsg;
       Logger.log(logPrefix + "WARN - Cannot finalize processing for Import ID '" + (importId || 'Unknown') + "'. " + missingDataMsg);
    }

    // --- Log to Verification Sheet (Always happens) ---
    logToDoPostVerification(receivedTimestamp, rawData, parsedData, parseError, sheetUpdateError, cacheError);

    // --- Respond to WordPress ---
     // Always send 200 OK if we received and attempted to process the data.
     // WordPress usually just needs acknowledgment. Errors are logged on GAS side.
     var responseMessage = "Callback received by Google Apps Script for Import ID: " + (importId || 'Unknown') + ".";
     var responseStatus = 200;
     var processingErrors = [parseError, sheetUpdateError, cacheError].filter(Boolean); // Filter out null/empty errors

     if (processingErrors.length > 0) {
        responseMessage += " Processed with warnings/errors on GAS side (see logs).";
        Logger.log(logPrefix + "Responding to WP with status 200 but indicating processing issues.");
     } else {
        responseMessage += " Processed successfully on GAS side.";
        Logger.log(logPrefix + "Responding to WP with status 200 and success message.");
     }

     var textOutput = ContentService.createTextOutput(responseMessage).setMimeType(ContentService.MimeType.TEXT);
     return textOutput;

  } catch (fatalError) {
    Logger.log(logPrefix + "FATAL ERROR in doPost for Import ID '" + (importId || 'Unknown') + "': " + fatalError);
    Logger.log(logPrefix + "Stack Trace: " + fatalError.stack);
    // Attempt to log fatal error to verification sheet
    logToDoPostVerification(new Date(), rawData, parsedData, parseError, sheetUpdateError, cacheError, "FATAL doPost Error: " + fatalError.toString());
    // Return a 200 OK but indicate server error in the message
     return ContentService.createTextOutput("Fatal Error during doPost processing on Google Apps Script side. Check GAS logs.").setMimeType(ContentService.MimeType.TEXT);
  }
}


/********************************************************
 * logToDoPostVerification - Helper to log doPost activity to a dedicated sheet
 * Uses SESSIONS_CALLBACK_VERIFICATION_SHEET_NAME constant.
 ********************************************************/
function logToDoPostVerification(timestamp, rawData, parsedData, parseError, sheetError, cacheError, fatalError = null) {
    try {
        var ss = SpreadsheetApp.getActiveSpreadsheet();
        var sheetName = SESSIONS_CALLBACK_VERIFICATION_SHEET_NAME; // Use Constant
        var sheet = ss.getSheetByName(sheetName);
        if (!sheet) {
            sheet = ss.insertSheet(sheetName);
            sheet.appendRow([ // Define headers
                "Timestamp Received", "Raw Data Snippet", "Parsed Import ID", "Parsed End Time (Unix)",
                "Parse Error", "Sheet Update Error", "Cache Update Error", "Fatal Error"
            ]);
            sheet.setFrozenRows(1);
            sheet.getRange("A:A").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Format timestamp column
        }
         sheet.appendRow([ // Append data
            timestamp,
            String(rawData).substring(0, 500), // Log snippet
            parsedData ? String(parsedData.import_id || '') : '', // Ensure string ID
            parsedData ? parsedData.end_time : '', // Log raw Unix timestamp
            parseError || '', sheetError || '', cacheError || '', fatalError || ''
        ]);
    } catch(sheetLogErr) {
        Logger.log("CRITICAL ERROR: Could not write to verification sheet '" + sheetName + "': " + sheetLogErr);
    }
}

/********************************************************
 * updateGoogleSheetWithImportData - Appends WP All Import results to the log sheet.
 * Uses SESSIONS_IMPORT_LOG_SHEET_NAME constant.
 * @param {object} data - Parsed JSON data received from WP doPost.
 * @return {object} { success: boolean, error: string | null }
 ********************************************************/
function updateGoogleSheetWithImportData(data) {
    try {
        var importId = data.import_id ? String(data.import_id) : null; // Ensure string ID
        if (!importId) { throw new Error("Missing import_id in data."); }

        var postsCreated = data.posts_created || 0;
        var postsUpdated = data.posts_updated || 0;
        var postsDeleted = data.posts_deleted || 0;
        var postsSkipped = data.posts_skipped || 0;
        var startTime = data.start_time; // Unix timestamp (seconds)
        var endTime = data.end_time;     // Unix timestamp (seconds)

        var ss = SpreadsheetApp.getActiveSpreadsheet();
        var sheetName = SESSIONS_IMPORT_LOG_SHEET_NAME; // Use Constant
        var sheet = ss.getSheetByName(sheetName);
        if (!sheet) {
            sheet = ss.insertSheet(sheetName);
            sheet.appendRow([ // Define headers
                "Import ID", "Start Time", "End Time", "Duration (Min)",
                "Posts Created", "Posts Updated", "Posts Deleted", "Posts Skipped",
                "Callback Received", "Start Unix", "End Unix" // Simplified header names
            ]);
            sheet.setFrozenRows(1);
            // Apply formatting
            sheet.getRange("B:C").setNumberFormat("yyyy-mm-dd hh:mm:ss");
            sheet.getRange("I:I").setNumberFormat("yyyy-mm-dd hh:mm:ss");
            sheet.getRange("J:K").setNumberFormat("0"); // Unix timestamps as plain numbers
            Logger.log("Created WP Import Log sheet: " + sheetName);
        }

        // Calculate duration
        var durationMinutes = 'N/A';
        if (startTime && endTime && typeof startTime === 'number' && typeof endTime === 'number' && endTime >= startTime) {
             durationMinutes = ((endTime - startTime) / 60).toFixed(2);
        } else if (startTime && endTime) {
            Logger.log("WARN [updateLogSheet]: Invalid start/end times for import " + importId + ". Duration calc skipped.");
        }

        // Format timestamps
        var scriptTimeZone = Session.getScriptTimeZone();
        var formatTs = (ts) => ts && typeof ts === 'number' ? Utilities.formatDate(new Date(ts * 1000), scriptTimeZone, "yyyy-MM-dd HH:mm:ss") : 'N/A';
        var formattedStartTime = formatTs(startTime);
        var formattedEndTime = formatTs(endTime);
        var formattedReceivedTimestamp = Utilities.formatDate(new Date(), scriptTimeZone, "yyyy-MM-dd HH:mm:ss");

        // Append data row
        var nextRow = sheet.getLastRow() + 1;
        sheet.getRange(nextRow, 1, 1, 11).setValues([[
            importId, formattedStartTime, formattedEndTime, durationMinutes,
            postsCreated, postsUpdated, postsDeleted, postsSkipped,
            formattedReceivedTimestamp,
            startTime || '', endTime || '' // Raw Unix timestamps
        ]]);

        Logger.log("Appended WP Import log for ID " + importId + " to sheet " + sheetName);
        return { success: true, error: null };

    } catch (e) {
        Logger.log("ERROR in updateGoogleSheetWithImportData (ID: " + (data.import_id || 'Unknown') + "): " + e + " | Stack: " + e.stack);
        return { success: false, error: e.message };
    }
}

/********************************************************
 * updateCacheWithImportData - Stores import completion results in CacheService.
 * @param {object} data - Parsed JSON data received from WP doPost.
 * @return {object} { success: boolean, error: string | null }
 ********************************************************/
 function updateCacheWithImportData(data) {
     try {
         var importId = data.import_id ? String(data.import_id) : null;
         if (!importId) { throw new Error("Missing import_id for cache update."); }

         // Crucial check: end_time must exist to signify completion
         if (!data.end_time || typeof data.end_time !== 'number') {
             throw new Error("Missing or invalid end_time in data for cache update.");
         }

         var cacheKey = 'import_status_' + importId;
         var resultsData = {
             status: 'complete', // Mark as complete
             importId: importId,
             created: data.posts_created || 0, updated: data.posts_updated || 0,
             deleted: data.posts_deleted || 0, skipped: data.posts_skipped || 0,
             startTime: data.start_time || null, // Unix timestamp (seconds)
             endTime: data.end_time,             // Unix timestamp (seconds)
             receivedTime: Math.floor(new Date().getTime() / 1000), // Unix timestamp (seconds)
             message: "Import completed." // Simple completion message
         };

         Logger.log("DEBUG [updateCache]: Updating cache key: " + cacheKey + " with status: complete");

         try {
           cache.put(cacheKey, JSON.stringify(resultsData), CACHE_EXPIRATION_SECONDS);
           Logger.log("Stored 'complete' status in cache for Import ID " + importId);
           return { success: true, error: null };
         } catch (cachePutError) {
           Logger.log("ERROR [updateCache]: Failed storing data in cache (Key: " + cacheKey + "): " + cachePutError.message);
           // Log data attempted for debugging cache issues
           try { Logger.log("DEBUG [updateCache]: Data for cache: " + JSON.stringify(resultsData)); } catch (stringifyErr) { /* Ignore */ }
           throw cachePutError; // Re-throw
         }

     } catch (e) {
         Logger.log("ERROR in updateCacheWithImportData (ID: " + (data.import_id || 'Unknown') + "): " + e + " | Stack: " + e.stack);
         return { success: false, error: e.message };
     }
 }


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
    var triggerSuccess = false; // Specifically tracks if the trigger was successful *and confirmed by WP*
    var processingAttempted = false;
    var overallStatus = 'error';
    var overallMessage = 'Import initiation failed.';
    var runId = startTime.getTime(); // Unique ID for this specific run attempt

    function logWP(msg) {
        var timeStamped = "[" + startTime.toLocaleTimeString() + "] ";
        logCollector.push(timeStamped + msg);
        Logger.log("WP_IMPORT [" + importId + "]: " + msg); // Log to GAS execution logs with context
    }

    // --- Validate Input ---
    if (!importId) {
        logWP("ERROR: Import ID is missing. Cannot initiate WordPress import.");
        return {
             success: false,
             status: 'error_missing_id',
             message: 'Import ID was not provided.',
             runId: null,
             log: logCollector.join("\n")
        };
    }
    importId = String(importId); // Ensure it's a string for consistency
    var cacheKey = 'import_status_' + importId;
    logWP("INFO: Initiating WordPress Import. Import ID: " + importId + ", Run ID: " + runId);


    // --- Check if already pending/running (Cache Check) ---
    try {
        var existingStatusRaw = cache.get(cacheKey);
        if (existingStatusRaw) {
            var existingStatus = JSON.parse(existingStatusRaw);
            // Prevent starting if explicitly pending
            if (existingStatus && existingStatus.status === 'pending') {
                 logWP("WARN: Import ID " + importId + " already has a 'pending' status in cache. Preventing new initiation.");
                 return {
                     success: false, // Indicate initiation didn't proceed
                     status: 'already_pending',
                     message: 'Import ' + importId + ' appears to be already running or pending. Please wait or Cancel if stuck.',
                     runId: null, // No new run started
                     log: logCollector.join("\n")
                 };
            }
        }
    } catch (cacheReadErr) {
        logWP("WARN: Could not read existing cache status before initiating: " + cacheReadErr.message + ". Proceeding cautiously.");
    }

    // --- Store Pending Status in Cache ---
    try {
      var pendingStatus = {
          status: 'pending',
          runId: runId,
          startTime: startTime.toISOString(),
          importId: importId,
          message: "Import triggered, awaiting completion callback..."
      };
      cache.put(cacheKey, JSON.stringify(pendingStatus), CACHE_EXPIRATION_SECONDS);
      logWP("INFO: Stored 'pending' status in cache for Import ID " + importId + " (Run ID: " + runId + ")");
    } catch (cacheErr) {
        logWP("ERROR: Failed to store pending status in cache: " + cacheErr.message + ". Proceeding with trigger anyway.");
        overallMessage = "Failed to store initial status in cache, but proceeding with trigger.";
        // Don't return here, attempt the trigger
    }

    // --- Construct URLs ---
    var baseUrl = WP_IMPORT_BASE_URL +
                  '?import_key=' + encodeURIComponent(WP_IMPORT_KEY) +
                  '&rand=' + Math.random(); // Base URL + key + randomness

    var triggerUrl = baseUrl +
                     '&import_id=' + encodeURIComponent(importId) +
                     '&action=trigger';
    var processingUrl = baseUrl +
                       '&import_id=' + encodeURIComponent(importId) +
                       '&action=processing';


    // --- Set Fetch Options ---
    var options = {
        method: 'get',
        muteHttpExceptions: true,
        headers: { 'User-Agent': 'GoogleAppsScript-SessionsSyncDashboard/1.2' }, // Identify script
        deadline: WP_ACTION_TIMEOUT // Apply timeout to the fetch itself
    };

    // --- 1. Trigger the Import ---
    logWP("INFO: Attempting to trigger WordPress Import...");
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
            try { triggerResponseJson = JSON.parse(triggerResponseText); } catch (e) { /* ignore parse error, check text below */ }

            // Check for explicit success message OR specific trigger confirmation text
            if (triggerResponseJson.status === 200 || (triggerResponseText.toLowerCase().includes("triggered") && !triggerResponseText.toLowerCase().includes("already processing"))) {
                triggerSuccess = true; // Mark the trigger as successful
                logWP("INFO: Trigger successful (HTTP 200 and/or WP confirmation text found).");

                 // --- 2. Attempt ONE Processing Run (ONLY if Trigger was OK) ---
                processingAttempted = true;
                logWP("INFO: Attempting initial processing run...");
                Logger.log("DEBUG: Processing URL: " + processingUrl);
                Utilities.sleep(1000); // Small delay between trigger and process

                try {
                    var processingResponse = UrlFetchApp.fetch(processingUrl, options); // Reuse options
                    var processingResponseCode = processingResponse.getResponseCode();
                    var processingResponseText = processingResponse.getContentText() || '(No response body)';
                    processingDetails = { code: processingResponseCode, text_snippet: processingResponseText.substring(0, 250) + "..." };

                    logWP("INFO: WP Processing Response Code: " + processingResponseCode);
                    Logger.log("DEBUG: WP Processing Response Body: " + processingResponseText);

                    if (processingResponseCode === 200) {
                        // Successfully called both URLs, now waiting for doPost callback
                        overallStatus = 'initiated';
                        overallMessage = 'Import ' + importId + ' initiated successfully. Waiting for completion callback from WordPress.';
                        logWP("INFO: Initial processing URL call successful (HTTP 200). Waiting for callback.");
                    } else {
                        // Trigger OK, but processing call failed HTTP check
                        overallStatus = 'initiated_processing_http_error';
                        overallMessage = 'Import ' + importId + ' triggered, but initial processing call failed (HTTP Status: ' + processingResponseCode + '). Import might still run.';
                        logWP("ERROR: " + overallMessage);
                        // We still consider the *initiation* successful because the trigger worked.
                    }
                } catch (procErr) {
                    // Error during the fetch call for processing URL
                    processingDetails = { error: procErr.message };
                    if (procErr.message.includes("Timeout") || procErr.message.includes("timed out")) {
                         overallStatus = 'initiated_processing_timeout';
                         overallMessage = 'Import ' + importId + ' triggered, but initial processing call timed out. Import might still be running.';
                         logWP("WARN: Initial processing call timed out. " + procErr.message);
                     } else {
                        overallStatus = 'initiated_processing_fetch_error';
                        overallMessage = 'Import ' + importId + ' triggered, but encountered error calling processing URL: ' + procErr.message;
                        logWP("EXCEPTION during processing call: " + overallMessage);
                     }
                    // Initiation still considered successful as trigger worked.
                }

            } else {
                 // Trigger URL hit (HTTP 200), but WP response indicates an issue (e.g., already running)
                 triggerSuccess = false; // Mark trigger as failed in intent
                 overallStatus = 'trigger_wp_error';
                 var wpErrorMsg = triggerResponseJson.message || (triggerResponseText.length < 200 ? triggerResponseText : "(Could not parse WP response)");
                 overallMessage = 'WordPress reported an issue with triggering Import ID ' + importId + '. Message: ' + wpErrorMsg;
                 logWP("WARN: Trigger call received HTTP 200, but WP reported an issue: " + overallMessage);
                 // Clear the pending status from cache as it didn't actually start a *new* run
                 try { cache.remove(cacheKey); logWP("INFO: Removed pending status from cache due to WP trigger issue."); } catch(rmErr){ logWP("WARN: Failed to remove pending status from cache: "+ rmErr.message);}
            }

        } else { // HTTP error on trigger call itself
            triggerSuccess = false;
            overallStatus = 'trigger_http_error';
            overallMessage = 'WordPress trigger failed for Import ID ' + importId + ' with HTTP Status Code: ' + triggerResponseCode;
            logWP("ERROR: " + overallMessage);
             // Clear pending status if trigger HTTP failed
             try { cache.remove(cacheKey); logWP("INFO: Removed pending status from cache due to trigger HTTP failure."); } catch(rmErr){ logWP("WARN: Failed to remove pending status from cache: "+ rmErr.message);}
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
            overallMessage = 'Error making trigger request to WordPress (Import ID: ' + importId + '): ' + triggerErr.message;
            logWP("EXCEPTION during trigger call: " + overallMessage);
         }
         // Clear pending status if trigger fetch failed
         try { cache.remove(cacheKey); logWP("INFO: Removed pending status from cache due to trigger fetch failure."); } catch(rmErr){ logWP("WARN: Failed to remove pending status from cache: "+ rmErr.message);}
    }

    // --- Prepare and Return Result ---
    var finalElapsedTime = ((new Date().getTime() - startTime.getTime()) / 1000).toFixed(1);
    logWP("INFO: Import Initiation Function Finished in " + finalElapsedTime + "s. Final Status: " + overallStatus);

    // Return success: true only if the trigger call was successful AND WordPress confirmed it.
    return {
        success: triggerSuccess && (overallStatus.startsWith('initiated') || overallStatus === 'success'), // Be more specific about success state
        status: overallStatus,
        message: overallMessage,
        runId: (triggerSuccess && (overallStatus.startsWith('initiated'))) ? runId : null, // Only return runId if successfully initiated
        log: logCollector.join("\n")
    };
}


/********************************************************
 * getImportStatus - Reads status from CacheService for the UI
 * @param {string} importId - The WP All Import numerical ID.
 * @return {object|null} Parsed status object from cache or null if not found/invalid.
 ********************************************************/
function getImportStatus(importId) {
    if (!importId) {
        Logger.log("WARN [getImportStatus]: Called without importId.");
        return null;
     }
     importId = String(importId);
     var cacheKey = 'import_status_' + importId;
     Logger.log("DEBUG [getImportStatus]: Checking cache for key: " + cacheKey);

    try {
        var cachedData = cache.get(cacheKey);
        if (cachedData) {
             Logger.log("DEBUG [getImportStatus]: Cache hit for " + cacheKey + ". Data: " + cachedData.substring(0,100) + "...");
             var parsedData = JSON.parse(cachedData);
              // Basic validation: Check for essential 'status' field
             if (parsedData && parsedData.status) {
                // Add timestamp for freshness check if needed by UI later
                parsedData.cacheCheckTime = new Date().toISOString();
                return parsedData;
             } else {
                 Logger.log("WARN [getImportStatus]: Cache data for " + cacheKey + " seems invalid (missing 'status'): " + cachedData);
                 // Optional: Clear invalid entry
                 // cache.remove(cacheKey);
                 return null; // Treat as cache miss
             }
        } else {
             Logger.log("DEBUG [getImportStatus]: Cache miss for " + cacheKey);
             // No need to check sheet as fallback, cache is the source of truth for status
             return null;
        }
    } catch (e) {
        Logger.log("ERROR [getImportStatus]: Error reading/parsing import status from cache for ID " + importId + ": " + e);
        // Return an error status object recognizable by the UI
        return { status: 'error', message: "Cache read/parse error: " + e.message, importId: importId };
    }
}


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
        logCancel("ERROR: Import ID is missing. Cannot send cancellation request.");
        return { success: false, status: 'error_missing_id', message: 'Import ID was not provided.', log: logCollector.join("\n") };
    }
    importId = String(importId);
    var cacheKey = 'import_status_' + importId; // Define cache key for cleanup

    logCancel("INFO: Received request to cancel WordPress Import ID: " + importId);

    // --- Construct URL ---
    var cancelUrl = WP_IMPORT_BASE_URL +
                '?import_key=' + encodeURIComponent(WP_IMPORT_KEY) +
                '&import_id=' + encodeURIComponent(importId) +
                '&action=cancel' + // Use the 'cancel' action based on WP All Import docs
                '&rand=' + Math.random();

    logCancel("INFO: Calling WordPress cancellation endpoint...");
    Logger.log("DEBUG: Cancel URL: " + cancelUrl);

    // --- Set Fetch Options ---
    var options = {
        method: 'get',
        muteHttpExceptions: true,
        headers: { 'User-Agent': 'GoogleAppsScript-SessionsSyncDashboard/1.2-Cancel' },
        deadline: WP_ACTION_TIMEOUT // Use standard action timeout
    };

    // --- Make Request ---
    try {
        var response = UrlFetchApp.fetch(cancelUrl, options);
        var responseCode = response.getResponseCode();
        var responseText = response.getContentText() || '(No response body)';
        var responseJson = null;

        logCancel("INFO: WP Cancel Response Code: " + responseCode);
        Logger.log("DEBUG: WP Cancel Response Body: " + responseText);

        try { responseJson = JSON.parse(responseText); } catch (parseErr) { /* Ignore, check text */ }

        // Check if WP acknowledged the request (HTTP 200 is key)
        // WP All Import's cancel action might not return JSON, just text confirmation.
        if (responseCode === 200 && (responseText.toLowerCase().includes("cancelled") || responseText.toLowerCase().includes("stopped"))) {
            success = true;
            status = "success";
            message = "Cancellation request sent successfully. Import should stop shortly.";
            logCancel("✅ SUCCESS: WordPress acknowledged cancellation request.");

            // --- Crucial: Remove pending status from cache ---
            try {
                cache.remove(cacheKey);
                logCancel("INFO: Removed status from cache for cancelled Import ID " + importId);
            } catch (cacheErr) {
                logCancel("WARN: Failed to remove status from cache after cancellation: " + cacheErr.message);
                message += " (Warning: Cache cleanup issue)";
            }

        } else if (responseCode === 200) {
            // HTTP 200, but message doesn't confirm cancellation (e.g., "Import not found", "Already complete")
             success = false; // Cancellation *itself* didn't happen as intended
             status = "wp_error";
             message = "WordPress response indicates cancellation was not needed or failed. Message: " + (responseText.length < 200 ? responseText : "(See debug logs)");
             logCancel("WARN: WP indicated cancellation issue: " + message);
              // Optionally remove cache if WP says it's not running anyway? Debatable.
             // try { cache.remove(cacheKey); } catch(e){}
        } else { // HTTP errors
            success = false;
            status = "http_error";
            message = "WordPress cancellation endpoint failed (HTTP Status: " + responseCode + ")";
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
            message = "Error occurred trying to send cancellation request: " + fetchErr.message;
            logCancel("❌ EXCEPTION during cancel call: " + message);
         }
    }

    // --- Prepare and Return Result ---
    var finalElapsedTime = ((new Date().getTime() - startTime.getTime()) / 1000).toFixed(1);
    logCancel("INFO: Cancel Import Function Finished in " + finalElapsedTime + "s. Final Status: " + status);

    return {
        success: success, // True only if WP confirmed cancellation request received ok
        status: status,
        message: message,
        log: logCollector.join("\n")
    };
}


// =======================================================
//                WEB APP & DASHBOARD FUNCTIONS
// =======================================================

/********************************************************
 * doPost(e) - Web app entry point for receiving data from WordPress
 * Handles the callback after WP All Import finishes.
 ********************************************************/
function doPost(e) {
  var receivedTimestamp = new Date();
  var rawData = '';
  var parsedData = null;
  var parseError = null;
  var sheetUpdateError = null;
  var cacheError = null;
  var importId = null; // Define importId earlier

  try {
    // --- Log Raw Data ---
    if (e && e.postData && e.postData.contents) {
      rawData = e.postData.contents;
      Logger.log("CALLBACK [doPost]: Received raw data at " + receivedTimestamp.toISOString() + ": " + rawData);
    } else {
      rawData = "No postData received or e object was undefined.";
      Logger.log("WARN [doPost]: No postData received at " + receivedTimestamp.toISOString());
      logToDoPostVerification(receivedTimestamp, rawData, null, "No postData received", null, null);
      return ContentService.createTextOutput("Error: No data received.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- Attempt to Parse JSON ---
    try {
      parsedData = JSON.parse(rawData);
      importId = parsedData ? parsedData.import_id : null; // Get importId after parsing
      Logger.log("CALLBACK [doPost]: Successfully parsed JSON data for Import ID: " + (importId || 'N/A'));
    } catch (jsonEx) {
      parseError = "Error parsing JSON: " + jsonEx;
      Logger.log("ERROR [doPost]: Error parsing JSON: " + jsonEx + " | Raw Data Snippet: " + rawData.substring(0,200));
      logToDoPostVerification(receivedTimestamp, rawData, null, parseError, null, null);
      return ContentService.createTextOutput("Error: Could not parse received JSON.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- Process Parsed Data (Update Sheet & Cache) ---
    if (importId && parsedData.end_time) { // Check for essential data: ID and end_time
       // Update the dedicated import log sheet
       var sheetUpdateResult = updateGoogleSheetWithImportData(parsedData);
       if (!sheetUpdateResult.success) {
           sheetUpdateError = sheetUpdateResult.error;
           Logger.log("ERROR [doPost]: Error updating Google Sheet (" + SESSIONS_IMPORT_LOG_SHEET_NAME + ") for Import ID " + importId + ": " + sheetUpdateError);
       } else {
           Logger.log("CALLBACK [doPost]: Successfully updated Google Sheet (" + SESSIONS_IMPORT_LOG_SHEET_NAME + ") for Import ID " + importId);
       }

       // Update the cache with the completion status
       var cacheUpdateResult = updateCacheWithImportData(parsedData);
        if (!cacheUpdateResult.success) {
           cacheError = cacheUpdateResult.error;
           Logger.log("ERROR [doPost]: Error updating Cache for Import ID " + importId + ": " + cacheError);
       } else {
           Logger.log("CALLBACK [doPost]: Successfully updated Cache for Import ID " + importId);
       }

    } else {
       var missingDataMsg = "Parsed data missing required fields.";
       if (!importId) missingDataMsg += " Missing: import_id.";
       if (parsedData && !parsedData.end_time) missingDataMsg += " Missing: end_time.";
       sheetUpdateError = missingDataMsg; // Log as errors for consistency
       cacheError = missingDataMsg;
       Logger.log("WARN [doPost]: Cannot process callback data for Import ID " + (importId || 'Unknown') + ". " + missingDataMsg);
    }

    // --- Log to Verification Sheet ---
    logToDoPostVerification(receivedTimestamp, rawData, parsedData, parseError, sheetUpdateError, cacheError);

    // --- Respond to WordPress ---
     var responseMessage = "Callback received for Import ID " + (importId || 'Unknown') + ".";
     var responseStatus = 200; // Default OK
     var errors = [];
     if (parseError) errors.push("Parse Error"); // Keep brief for WP response
     if (sheetUpdateError) errors.push("Sheet Update Error");
     if (cacheError) errors.push("Cache Update Error");

     if (errors.length > 0) {
        responseMessage += " Processed with Errors: " + errors.join(', ');
        // Still return 200 OK to WP, as we *received* the data, even if processing failed.
        // Internal errors are logged on GAS side.
     } else {
        responseMessage += " Processed successfully.";
     }

     Logger.log("CALLBACK [doPost]: Responding to WP for Import ID " + (importId || 'Unknown') + " with Status " + responseStatus + ": " + responseMessage);
     var textOutput = ContentService.createTextOutput(responseMessage).setMimeType(ContentService.MimeType.TEXT);
     return textOutput;

  } catch (fatalError) {
    Logger.log("FATAL ERROR in doPost for Import ID " + (importId || 'Unknown') + ": " + fatalError);
    Logger.log("Stack Trace: " + fatalError.stack);
    // Attempt to log fatal error to verification sheet
    logToDoPostVerification(new Date(), rawData, parsedData, parseError, sheetUpdateError, cacheError, "FATAL doPost Error: " + fatalError.toString());
    // Return a generic 500 error message, but still 200 OK status to acknowledge receipt
     return ContentService.createTextOutput("Fatal Error during doPost processing on Google Apps Script side.").setMimeType(ContentService.MimeType.TEXT);
  }
}


/********************************************************
 * logToDoPostVerification - Helper to log doPost activity to a dedicated sheet
 ********************************************************/
function logToDoPostVerification(timestamp, rawData, parsedData, parseError, sheetError, cacheError, fatalError = null) {
    try {
        var ss = SpreadsheetApp.getActiveSpreadsheet();
        // Use the specific verification sheet name for Sessions
        var sheet = ss.getSheetByName(SESSIONS_CALLBACK_VERIFICATION_SHEET_NAME);
        if (!sheet) {
            sheet = ss.insertSheet(SESSIONS_CALLBACK_VERIFICATION_SHEET_NAME);
            // Define headers for the verification sheet
            sheet.appendRow([
                "Timestamp Received", "Raw Data Snippet", "Parsed Import ID", "Parsed End Time (Unix)",
                "Parse Error", "Sheet Update Error", "Cache Update Error", "Fatal Error"
            ]);
            sheet.setFrozenRows(1);
            sheet.getRange("A:A").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Format timestamp column
        }
         // Append the relevant data
         sheet.appendRow([
            timestamp,
            String(rawData).substring(0, 500), // Log only a snippet of raw data
            parsedData ? parsedData.import_id : '',
            parsedData ? parsedData.end_time : '', // Log raw Unix timestamp
            parseError || '', // Log errors if they occurred
            sheetError || '',
            cacheError || '',
            fatalError || ''
        ]);
    } catch(sheetLogErr) {
        // Log critically if we can't even write to the verification sheet
        Logger.log("CRITICAL ERROR: Could not write to verification sheet '" + SESSIONS_CALLBACK_VERIFICATION_SHEET_NAME + "': " + sheetLogErr);
    }
}

/********************************************************
 * updateGoogleSheetWithImportData - Appends WP All Import results to the log sheet.
 * @param {object} data - Parsed JSON data received from WP doPost.
 * @return {object} { success: boolean, error: string | null }
 ********************************************************/
function updateGoogleSheetWithImportData(data) {
    try {
        var importId = data.import_id;
        if (!importId) { throw new Error("Missing import_id in data."); }

        // Extract stats from the data payload
        var postsCreated = data.posts_created || 0;
        var postsUpdated = data.posts_updated || 0;
        var postsDeleted = data.posts_deleted || 0;
        var postsSkipped = data.posts_skipped || 0;
        var startTime = data.start_time; // Expecting Unix timestamp (seconds)
        var endTime = data.end_time;     // Expecting Unix timestamp (seconds)

        var ss = SpreadsheetApp.getActiveSpreadsheet();
        // Use the specific log sheet name for Sessions
        var sheet = ss.getSheetByName(SESSIONS_IMPORT_LOG_SHEET_NAME);
        if (!sheet) {
            sheet = ss.insertSheet(SESSIONS_IMPORT_LOG_SHEET_NAME);
            // Define headers for the import log sheet
            sheet.appendRow([
                "Import ID", "Start Time", "End Time", "Duration (Min)",
                "Posts Created", "Posts Updated", "Posts Deleted", "Posts Skipped",
                "Callback Received Timestamp", "Start Timestamp (Unix)", "End Timestamp (Unix)"
            ]);
            sheet.setFrozenRows(1);
            sheet.getRange("B:C").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Format date columns
            sheet.getRange("I:I").setNumberFormat("yyyy-mm-dd hh:mm:ss");
            sheet.getRange("J:K").setNumberFormat("0"); // Format timestamp columns as numbers
            Logger.log("Created WP Import Log sheet: " + SESSIONS_IMPORT_LOG_SHEET_NAME);
        }

        // Calculate duration
        var durationMinutes = 'N/A';
        if (startTime && endTime && endTime >= startTime) {
             durationMinutes = ((endTime - startTime) / 60).toFixed(2);
        } else if (startTime && endTime) {
            Logger.log("WARN [updateSheet]: End time appears to be before start time for import " + importId + ". Duration calculation skipped.");
        }

        // Format timestamps for display
        var scriptTimeZone = Session.getScriptTimeZone();
        // Multiply by 1000 to convert Unix timestamp (seconds) to milliseconds for Date object
        var formattedStartTime = startTime ? Utilities.formatDate(new Date(startTime * 1000), scriptTimeZone, "yyyy-MM-dd HH:mm:ss") : 'N/A';
        var formattedEndTime = endTime ? Utilities.formatDate(new Date(endTime * 1000), scriptTimeZone, "yyyy-MM-dd HH:mm:ss") : 'N/A';
        var formattedReceivedTimestamp = Utilities.formatDate(new Date(), scriptTimeZone, "yyyy-MM-dd HH:mm:ss"); // Timestamp when GAS received callback

        // Find the first empty row to append data
        var nextRow = sheet.getLastRow() + 1;

        // Append data row
        sheet.getRange(nextRow, 1, 1, 11).setValues([[
            importId, formattedStartTime, formattedEndTime, durationMinutes,
            postsCreated, postsUpdated, postsDeleted, postsSkipped,
            formattedReceivedTimestamp,
            startTime || '', // Append raw Unix timestamps as well
            endTime || ''
        ]]);

        Logger.log("Appended WP Import data for Import ID " + importId + " to sheet " + SESSIONS_IMPORT_LOG_SHEET_NAME + " at row " + nextRow);
        return { success: true, error: null };

    } catch (e) {
        Logger.log("ERROR in updateGoogleSheetWithImportData: " + e + " | Stack: " + e.stack);
        return { success: false, error: e.message };
    }
}


/********************************************************
 * updateCacheWithImportData - Stores import completion results in CacheService.
 * @param {object} data - Parsed JSON data received from WP doPost.
 * @return {object} { success: boolean, error: string | null }
 ********************************************************/
 function updateCacheWithImportData(data) {
     try {
         var importId = data.import_id;
         if (!importId) { throw new Error("Missing import_id in data for cache update."); }
         importId = String(importId); // Ensure string for cache key consistency

         // Ensure end_time exists before proceeding - this signifies completion
         if (!data.end_time) { throw new Error("Missing end_time in data for cache update."); }

         var cacheKey = 'import_status_' + importId;

         // Construct the data object to store in cache
         var resultsData = {
             status: 'complete', // Mark as complete
             importId: importId,
             created: data.posts_created || 0,
             updated: data.posts_updated || 0,
             deleted: data.posts_deleted || 0,
             skipped: data.posts_skipped || 0,
             startTime: data.start_time || null, // Store Unix timestamp (seconds)
             endTime: data.end_time,             // Store Unix timestamp (seconds)
             receivedTime: Math.floor(new Date().getTime() / 1000), // Unix timestamp (seconds) when GAS received it
             message: "Import completed successfully." // Add a completion message
         };

         Logger.log("DEBUG [updateCache]: Preparing to update cache key: " + cacheKey + " with status: complete");

         // Store in cache with appropriate expiration
         try {
           cache.put(cacheKey, JSON.stringify(resultsData), CACHE_EXPIRATION_SECONDS);
           Logger.log("Stored 'complete' status in cache for Import ID " + importId + ". Cache Key: " + cacheKey);
           return { success: true, error: null };
         } catch (cachePutError) {
           Logger.log("ERROR [updateCache]: Failed putting data into cache for key " + cacheKey + ": " + cachePutError.message);
           // Log the data that failed to be cached for debugging
           try { Logger.log("DEBUG [updateCache]: Data attempted for cache: " + JSON.stringify(resultsData)); } catch (stringifyErr) { Logger.log("DEBUG [updateCache]: Could not stringify data for cache debug log."); }
           throw cachePutError; // Re-throw the error to be caught by the outer try-catch in doPost
         }

     } catch (e) {
         Logger.log("ERROR in updateCacheWithImportData: " + e + " | Stack: " + e.stack);
         return { success: false, error: e.message };
     }
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
      lastSyncResults: aggregatedResults
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