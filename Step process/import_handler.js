/*******************************************************************************
 * WP All Import Callback Handler - v1.0
 * Description: Receives POST requests from the WP All Import Logger & Control
 *              plugin, routes data based on import ID, logs results to
 *              specific Google Sheets, and updates CacheService status.
 * Author:      Your Name/Org
 * Version:     1.0
 *******************************************************************************/

/********************************************************
 * Global Configuration
 ********************************************************/

// --- Cache Service ---
const cache = CacheService.getScriptCache();
const CACHE_EXPIRATION_SECONDS = 21600; // 6 hours (Match your other script)

// --- Spreadsheet Configuration ---
// Define ALL sheet names used by different import types here

// --- Sessions Sheets ---
const SESSIONS_IMPORT_LOG_SHEET = "sessions_wp_import_logs";       // Log of completed Session imports
const SESSIONS_VERIFY_SHEET = "sessions_wp_callback_data";         // Raw callback verification for Sessions

// --- Events Sheets ---
const EVENTS_IMPORT_LOG_SHEET = "Events_Import_Logs";             // Log of completed Event imports
const EVENTS_VERIFY_SHEET = "WP_Plugin_Data_Received";            // Raw callback verification for Events

// --- Add other content types as needed ---
// const PRODUCTS_IMPORT_LOG_SHEET = "products_wp_import_logs";
// const PRODUCTS_VERIFY_SHEET = "products_wp_callback_data";


// --- Import Type Configuration Object ---
// Maps a logical type name to its specific settings
const CONFIG = {
  'sessions': {
     type: 'sessions', // User-friendly type name
     importLogSheet: SESSIONS_IMPORT_LOG_SHEET,
     verifySheet: SESSIONS_VERIFY_SHEET,
     // Unique cache key prefix for this type (used by the *other* dashboard script to read status)
     cachePrefix: 'import_status_' // Prefix for the Sessions Dashboard's getImportStatus
     // Add any other type-specific settings if needed later
  },
  'events': {
     type: 'events',
     importLogSheet: EVENTS_IMPORT_LOG_SHEET,
     verifySheet: EVENTS_VERIFY_SHEET,
     // Unique cache key prefix for the Events dashboard script
     cachePrefix: 'import_status_' // Prefix for the Events Dashboard's getImportStatus (assuming it's the same)
  }
  // --- Add other types ---
  // 'products': {
  //    type: 'products',
  //    importLogSheet: PRODUCTS_IMPORT_LOG_SHEET,
  //    verifySheet: PRODUCTS_VERIFY_SHEET,
  //    cachePrefix: 'import_status_products_'
  // }
};

// --- Import ID to Config Key Mapping ---
// ** ACTION: Fill this map with your ACTUAL WP All Import IDs (as strings) **
// Maps the numerical WP All Import ID to the key used in the CONFIG object above.
const WP_IMPORT_ID_TO_CONFIG_KEY = {
   // -- Sessions --
   '31': 'sessions',  // <-- Replace '31' with your actual Sessions Import ID

   // -- Events --
   '15': 'events',   // <-- Replace '15' with your actual Events Import ID

   // -- Add other mappings --
   // 'YOUR_PRODUCT_IMPORT_ID': 'products',
};


/********************************************************
 * doGet(e) - Simple response for testing deployment URL
 ********************************************************/
function doGet(e) {
  Logger.log("doGet received request.");
  return ContentService.createTextOutput("WP All Import Callback Handler is active.")
         .setMimeType(ContentService.MimeType.TEXT);
}

/********************************************************
 * doPost(e) - Main Entry Point for WordPress Callbacks
 ********************************************************/
function doPost(e) {
  const receivedTimestamp = new Date();
  let rawData = '';
  let parsedData = null;
  let parseError = null;
  let sheetUpdateError = null;
  let cacheError = null;
  let importId = null; // Determined after parsing
  let importConfig = null; // Determined by routing

  const logPrefix = "CALLBACK HANDLER [doPost]: ";

  try {
    Logger.log("CALLBACK HANDLER doPost Started at: " + new Date().toISOString());
    // --- 1. Log Raw Data ---
    if (e && e.postData && e.postData.contents) {
      rawData = e.postData.contents;
      Logger.log(logPrefix + "Received raw data at " + receivedTimestamp.toISOString() + ". Length: " + rawData.length);
      Logger.log(logPrefix + "Raw data snippet: " + rawData.substring(0, 500));
    } else {
      rawData = "No postData received or e object was undefined.";
      Logger.log(logPrefix + "WARN - No postData received.");
      // Attempt to log failure to a default/generic verification sheet if possible? Hard to route without data.
      // logToVerificationSheet(receivedTimestamp, rawData, null, "No postData", null, null, null, "Generic_Callback_Verification"); // Example
      return ContentService.createTextOutput("Error: No data received by Callback Handler.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- 2. Parse JSON ---
    try {
      parsedData = JSON.parse(rawData);
      importId = parsedData ? String(parsedData.import_id || '') : ''; // Get import ID, ensure string
      Logger.log(logPrefix + "Parsed JSON. Import ID: '" + (importId || 'N/A') + "'");
    } catch (jsonEx) {
      parseError = "Error parsing JSON: " + jsonEx;
      Logger.log(logPrefix + "ERROR - " + parseError);
      // Log parse error to a generic verification sheet before returning?
      // logToVerificationSheet(receivedTimestamp, rawData, null, parseError, null, null, null, "Generic_Callback_Verification");
      return ContentService.createTextOutput("Error: Could not parse JSON data.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- 3. Routing based on Import ID ---
    if (!importId) {
      Logger.log(logPrefix + "ERROR - Parsed data missing 'import_id'. Cannot route.");
      // Log missing ID to a generic verification sheet?
      // logToVerificationSheet(receivedTimestamp, rawData, parsedData, "Missing import_id", null, null, null, "Generic_Callback_Verification");
      return ContentService.createTextOutput("Error: Import ID missing in payload.");
    }

    const configKey = WP_IMPORT_ID_TO_CONFIG_KEY[importId];
    if (!configKey || !CONFIG[configKey]) {
      Logger.log(logPrefix + "ERROR - Unknown Import ID received: '" + importId + "'. No configuration found.");
      // Log unknown ID to its own verification sheet maybe? Requires creating it on the fly or pre-defining.
      logToVerificationSheet(receivedTimestamp, rawData, parsedData, "Unknown Import ID", null, null, null, "Unknown_Import_Callbacks");
      return ContentService.createTextOutput("Error: Unknown Import ID '" + importId + "'.");
    }
    importConfig = CONFIG[configKey];
    Logger.log(logPrefix + "Routing Import ID '" + importId + "' to config type: '" + importConfig.type + "'");

    // --- 4. Process Completion Data (if applicable) ---
    // Check for end_time which signifies a completed import from the PHP plugin
    if (parsedData.end_time && typeof parsedData.end_time === 'number') {
      Logger.log(logPrefix + "Processing completed import for ID: " + importId);

      // Call helpers, passing the relevant config details (sheet names, cache prefix)
      const resultsLogSheet = importConfig.importLogSheet;
      const cacheKey = importConfig.cachePrefix + importId; // Construct the specific cache key

      // 4a. Log Results to specific sheet
      const sheetResult = logImportResultsToSheet(parsedData, resultsLogSheet);
      if (!sheetResult.success) sheetUpdateError = sheetResult.error;

      // 4b. Update Cache Status to 'complete'
      const cacheResult = updateCompletionCache(parsedData, cacheKey);
      if (!cacheResult.success) cacheError = cacheResult.error;

    } else {
      Logger.log(logPrefix + "Callback for Import ID '" + importId + "' received, but 'end_time' is missing or invalid. Not processing as complete.");
      // Optionally log this "in-progress" or "non-completion" callback to the verification sheet if needed
    }

    // --- 5. Log to Verification Sheet (Always) ---
    // Use the specific verification sheet name from the routed config
    logToVerificationSheet(receivedTimestamp, rawData, parsedData, parseError, sheetUpdateError, cacheError, null, importConfig.verifySheet);

    // --- 6. Respond to WordPress ---
    // Always send 200 OK if we reached this point, acknowledging receipt.
    let responseMessage = "Callback Handler received data for Import ID: " + importId + ".";
    const processingErrors = [parseError, sheetUpdateError, cacheError].filter(Boolean);
    if (processingErrors.length > 0) {
      responseMessage += " Processed with warnings/errors on GAS side.";
      Logger.log(logPrefix + "Responding 200 OK to WP, but with processing errors noted.");
    } else {
      responseMessage += " Processed successfully on GAS side.";
      Logger.log(logPrefix + "Responding 200 OK to WP with success message.");
    }
    return ContentService.createTextOutput(responseMessage).setMimeType(ContentService.MimeType.TEXT);

  } catch (fatalError) {
    Logger.log(logPrefix + "FATAL ERROR during doPost processing for Import ID '" + (importId || 'Unknown') + "': " + fatalError);
    Logger.log(logPrefix + "Stack Trace: " + fatalError.stack);
    // Attempt to log fatal error to the specific verification sheet if config was determined, otherwise maybe a generic one
    const verifySheet = importConfig ? importConfig.verifySheet : "Fatal_Error_Callbacks";
    logToVerificationSheet(receivedTimestamp, rawData, parsedData, parseError, sheetUpdateError, cacheError, "FATAL doPost Error: " + fatalError.toString(), verifySheet);
    // Return 200 OK but indicate server error in message
    return ContentService.createTextOutput("Fatal Error during doPost processing on Callback Handler side. Check GAS logs.").setMimeType(ContentService.MimeType.TEXT);
  }
}


// =======================================================
//                 HELPER FUNCTIONS
// =======================================================

/**
 * Appends WP All Import results data to the specified log sheet.
 * @param {object} data Parsed JSON data from WP callback.
 * @param {string} logSheetName The name of the Google Sheet to log results to.
 * @return {object} { success: boolean, error: string | null }
 */
function logImportResultsToSheet(data, logSheetName) {
    const logPrefix = "HELPER [logImportResults]: ";
    try {
        const importId = data.import_id ? String(data.import_id) : null;
        if (!importId) { throw new Error("Missing import_id in data."); }

        const postsCreated = data.posts_created || 0;
        const postsUpdated = data.posts_updated || 0;
        const postsDeleted = data.posts_deleted || 0;
        const postsSkipped = data.posts_skipped || 0;
        const startTime = data.start_time; // Unix timestamp (seconds)
        const endTime = data.end_time;     // Unix timestamp (seconds)

        const ss = SpreadsheetApp.getActiveSpreadsheet();
        let sheet = ss.getSheetByName(logSheetName);
        if (!sheet) {
            sheet = ss.insertSheet(logSheetName);
            // Define standard headers for results logs
            sheet.appendRow([
                "Import ID", "Start Time", "End Time", "Duration (Min)",
                "Posts Created", "Posts Updated", "Posts Deleted", "Posts Skipped",
                "Callback Received", "Start Unix", "End Unix"
            ]);
            sheet.setFrozenRows(1);
            // Apply formatting
            sheet.getRange("B:C").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Formatted dates
            sheet.getRange("I:I").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Callback time
            sheet.getRange("J:K").setNumberFormat("0"); // Unix timestamps as plain numbers
            Logger.log(logPrefix + "Created WP Import Results Log sheet: '" + logSheetName + "'");
        }

        // Calculate duration
        let durationMinutes = 'N/A';
        if (startTime && endTime && typeof startTime === 'number' && typeof endTime === 'number' && endTime >= startTime) {
             durationMinutes = ((endTime - startTime) / 60).toFixed(2);
        }

        // Format timestamps
        const scriptTimeZone = Session.getScriptTimeZone();
        const formatTs = (ts) => ts && typeof ts === 'number' ? Utilities.formatDate(new Date(ts * 1000), scriptTimeZone, "yyyy-MM-dd HH:mm:ss") : 'N/A';
        const formattedStartTime = formatTs(startTime);
        const formattedEndTime = formatTs(endTime);
        const formattedReceivedTimestamp = Utilities.formatDate(new Date(), scriptTimeZone, "yyyy-MM-dd HH:mm:ss");

        // Append data row
        sheet.appendRow([
            importId, formattedStartTime, formattedEndTime, durationMinutes,
            postsCreated, postsUpdated, postsDeleted, postsSkipped,
            formattedReceivedTimestamp,
            startTime || '', endTime || '' // Raw Unix timestamps
        ]);

        Logger.log(logPrefix + "Appended results for Import ID " + importId + " to sheet '" + logSheetName + "'");
        return { success: true, error: null };

    } catch (e) {
        Logger.log(logPrefix + "ERROR logging results to sheet '" + logSheetName + "' (ID: " + (data.import_id || 'Unknown') + "): " + e);
        return { success: false, error: e.message };
    }
}


/**
 * Stores import completion results in CacheService using a specific key.
 * @param {object} data Parsed JSON data from WP callback.
 * @param {string} cacheKey The specific cache key to use (e.g., "import_status_sessions_31").
 * @return {object} { success: boolean, error: string | null }
 */
 function updateCompletionCache(data, cacheKey) {
     const logPrefix = "HELPER [updateCompletionCache]: ";
     try {
         const importId = data.import_id ? String(data.import_id) : null;
         if (!importId) { throw new Error("Missing import_id for cache update."); }
         if (!data.end_time || typeof data.end_time !== 'number') { throw new Error("Missing/invalid end_time for cache update."); }
         if (!cacheKey) { throw new Error("Missing cacheKey for cache update."); }

         const resultsData = {
             status: 'complete', // Mark as complete
             importId: importId,
             created: data.posts_created || 0, updated: data.posts_updated || 0,
             deleted: data.posts_deleted || 0, skipped: data.posts_skipped || 0,
             startTime: data.start_time || null, // Unix timestamp (seconds)
             endTime: data.end_time,             // Unix timestamp (seconds)
             receivedTime: Math.floor(new Date().getTime() / 1000), // Unix timestamp (seconds)
             message: "Import completed."
         };

         Logger.log(logPrefix + "Updating cache key: '" + cacheKey + "' with status: 'complete'");

         try {
           cache.put(cacheKey, JSON.stringify(resultsData), CACHE_EXPIRATION_SECONDS);
           Logger.log(logPrefix + "Stored 'complete' status in cache for key '" + cacheKey + "'");
           return { success: true, error: null };
         } catch (cachePutError) {
           Logger.log(logPrefix + "ERROR storing data in cache (Key: " + cacheKey + "): " + cachePutError.message);
           try { Logger.log(logPrefix + "Data for cache: " + JSON.stringify(resultsData)); } catch (stringifyErr) { /* Ignore */ }
           throw cachePutError; // Re-throw
         }

     } catch (e) {
         Logger.log(logPrefix + "ERROR (ID: " + (data.import_id || 'Unknown') + "): " + e);
         return { success: false, error: e.message };
     }
 }


/**
 * Logs callback details (raw data, errors) to a specified verification sheet.
 * Creates the sheet if it doesn't exist.
 * @param {Date} timestamp When the callback was received.
 * @param {string} rawData Raw POST body.
 * @param {object|null} parsedData Parsed JSON object, or null if parsing failed.
 * @param {string|null} parseError Error message during parsing.
 * @param {string|null} sheetError Error message during results sheet logging.
 * @param {string|null} cacheError Error message during cache update.
 * @param {string|null} fatalError Fatal error during doPost.
 * @param {string} verifySheetName The name of the verification sheet to use.
 */
function logToVerificationSheet(timestamp, rawData, parsedData, parseError, sheetError, cacheError, fatalError, verifySheetName) {
    const logPrefix = "HELPER [logToVerification]: ";
     if (!verifySheetName) {
         Logger.log(logPrefix + "ERROR - Verification sheet name not provided. Cannot log verification data.");
         return; // Cannot proceed without a sheet name
     }
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        let sheet = ss.getSheetByName(verifySheetName);
        if (!sheet) {
            sheet = ss.insertSheet(verifySheetName);
            // Define standard headers for verification logs
            sheet.appendRow([
                "Timestamp Received", "Raw Data Snippet", "Parsed Import ID", "Parsed End Time (Unix)",
                "Parse Error", "Results Log Error", "Cache Update Error", "Fatal Error"
            ]);
            sheet.setFrozenRows(1);
            sheet.getRange("A:A").setNumberFormat("yyyy-mm-dd hh:mm:ss");
            Logger.log(logPrefix + "Created Verification Log sheet: '" + verifySheetName + "'");
        }
         sheet.appendRow([ // Append verification data
            timestamp,
            String(rawData).substring(0, 500), // Log snippet
            parsedData ? String(parsedData.import_id || '') : '', // Ensure string ID
            parsedData ? parsedData.end_time : '', // Log raw Unix timestamp
            parseError || '', sheetError || '', cacheError || '', fatalError || ''
        ]);
    } catch(sheetLogErr) {
        // Log critically if we can't write to the verification sheet
        Logger.log(logPrefix + "CRITICAL ERROR - Could not write to verification sheet '" + verifySheetName + "': " + sheetLogErr);
    }
}