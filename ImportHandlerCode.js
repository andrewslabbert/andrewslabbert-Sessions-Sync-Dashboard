/*******************************************************************************
 * WP All Import Central Callback Handler - v1.1
 *
 * Description: Receives secured POST requests from the WPAllImportMonitor plugin,
 *              verifies a shared secret, routes data based on import ID, logs
 *              results/raw data to per-type Google Sheets, updates CacheService
 *              status for completed imports, and exposes library functions for
 *              dashboards to poll.
 *
 * Author:      Four12 Global
 * Version:     1.1
 *


 * 4.  DEPLOY AS LIBRARY (Optional - if Dashboards need status):
 *     - Go to 'Deploy' > 'New deployment'.
 *     - Type: 'Library'.
 *     - Deploy and copy the Script ID to include in your dashboard scripts.
 *******************************************************************************/


/********************************************************
 * Global Configuration & Services
 ********************************************************/

// --- Cache Service ---
// ScriptCache is used to store the status of the *last completed* import for each ID.
// It's accessible only by this script, but other scripts can request data via getImportStatusFromCache().
const cache = CacheService.getScriptCache();
const CACHE_EXPIRATION_SECONDS = 21600; // 6 hours

// --- Import Type Configuration Object ---
// Central configuration mapping a logical type name (e.g., 'sessions')
// to its specific settings like sheet names and cache prefix.
const CONFIG = {
  'sessions': {
     type: 'sessions',                // User-friendly type name
     importLogSheet: 'sessions_wp_import_logs',    // Sheet for structured import results
     verifySheet: 'sessions_wp_callback_data',   // Sheet for raw callback data & errors
     cachePrefix: 'import_status_'    // Prefix for CacheService keys for this type
     // Add any other type-specific settings if needed later
  },
  'events': {
     type: 'events',
     importLogSheet: 'Events_Import_Logs',
     verifySheet: 'WP_Plugin_Data_Received', // Name from original code
     cachePrefix: 'import_status_'     // Can share prefix if IDs are distinct
  },
  // --- Add other content types as needed ---
  /*
  'products': {
     type: 'products',
     importLogSheet: 'products_wp_import_logs',
     verifySheet: 'products_wp_callback_data',
     cachePrefix: 'import_status_product_' // Example of a different prefix
  }
  */
  // --- Fallback/Error Sheets ---
  // These are used if routing fails or critical errors occur before routing.
  'unknown': {
      type: 'unknown',
      verifySheet: 'Unknown_Import_Callbacks' // Log callbacks with unknown IDs here
  },
  'fatal': {
      type: 'fatal',
      verifySheet: 'Fatal_Error_Callbacks' // Log callbacks that cause critical script errors
  }
};

// --- Import ID to Config Key Mapping ---
// ** ACTION REQUIRED: Fill this map with your ACTUAL WP All Import IDs (as strings) **
// Maps the numerical WP All Import ID received from the PHP plugin
// to the key used in the CONFIG object above (e.g., 'sessions', 'events').
const WP_IMPORT_ID_TO_CONFIG_KEY = {
   // -- Sessions --
   '31': 'sessions', 

   // -- Events --
   '30': 'events',   

   // -- Add other mappings --
   // 'YOUR_PRODUCT_IMPORT_ID_STRING': 'products',
};


/********************************************************
 * Web App Entry Points
 ********************************************************/

/**
 * Handles GET requests - useful for simple testing of deployment URL.
 * @param {object} e The event parameter for a GET request.
 * @return {ContentService.TextOutput} Simple text response.
 */
function doGet(e) {
  Logger.log("HANDLER [doGet]: Received GET request.");
  return ContentService.createTextOutput("WP All Import Callback Handler is active.")
         .setMimeType(ContentService.MimeType.TEXT);
}

/**
 * Handles POST requests from the WordPress plugin.
 * Main entry point for processing WP All Import callbacks.
 * @param {object} e The event parameter for a POST request. Contains postData.
 * @return {ContentService.TextOutput} Text response indicating success or failure.
 */


function doPost(e) {
  const receivedTimestamp = new Date();
  const logPrefix = "HANDLER [doPost]: ";
  let rawData = '';
  let parsedData = null;
  let parseError = null;
  let sheetUpdateError = null;
  let cacheError = null;
  let fatalErrorObj = null; // To store fatal error object
  let importId = null; // Determined after parsing
  let importConfig = null; // Determined by routing
  let verifySheetName = CONFIG.fatal.verifySheet; // Default to fatal if things go very wrong early


// ===============================================
  // --- START DEBUGGING BLOCK ---
  // ===============================================
  if (e.postData && e.postData.contents) {
  Logger.log(`postData length: ${e.postData.contents.length}`); // length only
}
  try {
    if (e) {
      // Log all top-level keys available in the event object 'e'
      var eventKeys = Object.keys(e);
      Logger.log(logPrefix + "DEBUG: Top-level keys found in 'e': " + eventKeys.join(', '));

      // Specifically check for 'headers' and log its type if it exists
      if (e.hasOwnProperty('headers')) {
        Logger.log(logPrefix + "DEBUG: Property 'e.headers' EXISTS. Type: " + typeof e.headers);
        // Try logging the headers safely
        try {
          Logger.log(logPrefix + "DEBUG: e.headers content: " + JSON.stringify(e.headers));
        } catch (stringifyErr) {
          Logger.log(logPrefix + "DEBUG: Could not stringify e.headers: " + stringifyErr);
        }
      } else {
        Logger.log(logPrefix + "DEBUG: Property 'e.headers' DOES NOT EXIST on 'e'.");
      }

       // Also log info about postData for context
       if (e.hasOwnProperty('postData')) {
         Logger.log(logPrefix + "DEBUG: Property 'e.postData' EXISTS. Type: " + typeof e.postData);
         if(e.postData) {
             Logger.log(logPrefix + "DEBUG: Keys in 'e.postData': " + Object.keys(e.postData).join(', '));
             Logger.log(logPrefix + "DEBUG: e.postData.type: " + e.postData.type);
             Logger.log(logPrefix + "DEBUG: e.postData.length: " + e.postData.length);
         } else {
             Logger.log(logPrefix + "DEBUG: 'e.postData' is null or undefined.");
         }
       } else {
          Logger.log(logPrefix + "DEBUG: Property 'e.postData' DOES NOT EXIST on 'e'.");
       }

    } else {
      Logger.log(logPrefix + "DEBUG: Event object 'e' is NULL or UNDEFINED.");
    }
  } catch (debugError) {
    // Catch errors *within* the debugging block itself
    Logger.log(logPrefix + "ERROR during DEBUG inspection: " + debugError);
  }
  // ===============================================
  // --- END DEBUGGING BLOCK ---
  // ===============================================

// --- Security Check (query‑string param) ---
const EXPECTED_SECRET = PropertiesService.getScriptProperties()
                       .getProperty('WEBHOOK_SECRET');

const receivedSecret = e.parameter.secret;   // value from ?secret=...

if (!EXPECTED_SECRET || receivedSecret !== EXPECTED_SECRET) {
  return ContentService.createTextOutput('Unauthorized');
}
// --- End Security Check ---

  try {
    Logger.log(logPrefix + "Received POST request at: " + receivedTimestamp.toISOString());

    // --- 2. Log Raw Data ---
    if (e && e.postData && e.postData.contents) {
      rawData = e.postData.contents;
      Logger.log(logPrefix + "Received raw data length: " + rawData.length);
      // Logger.log(logPrefix + "Raw data snippet: " + rawData.substring(0, 500)); // Keep commented unless debugging needed
    } else {
      rawData = "No postData received or e object was undefined.";
      Logger.log(logPrefix + "WARN - No postData received.");
      logToVerificationSheet(receivedTimestamp, rawData, null, "No postData received", null, null, null, CONFIG.unknown.verifySheet); // Log to unknown
      return ContentService.createTextOutput("Error: No data received by Callback Handler.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- 3. Parse JSON ---
    try {
      parsedData = JSON.parse(rawData);
      importId = parsedData ? String(parsedData.import_id || '') : ''; // Get import ID, ensure string
      Logger.log(logPrefix + "Parsed JSON. Import ID: '" + (importId || 'N/A') + "'");
    } catch (jsonEx) {
      parseError = "Error parsing JSON: " + jsonEx.message;
      Logger.log(logPrefix + "ERROR - " + parseError);
      // Try to log to unknown sheet as we don't have a valid ID yet
      logToVerificationSheet(receivedTimestamp, rawData, null, parseError, null, null, null, CONFIG.unknown.verifySheet);
      return ContentService.createTextOutput("Error: Could not parse JSON data.").setMimeType(ContentService.MimeType.TEXT);
    }

    // --- 4. Routing based on Import ID ---
    if (!importId) {
      parseError = "Parsed data missing 'import_id'. Cannot route."; // Log this as a parse-related error
      Logger.log(logPrefix + "ERROR - " + parseError);
      logToVerificationSheet(receivedTimestamp, rawData, parsedData, parseError, null, null, null, CONFIG.unknown.verifySheet);
      return ContentService.createTextOutput("Error: Import ID missing in payload.");
    }

    const configKey = WP_IMPORT_ID_TO_CONFIG_KEY[importId];
    if (!configKey || !CONFIG[configKey]) {
      parseError = "Unknown Import ID received: '" + importId + "'. No configuration found."; // Log as parse error
      Logger.log(logPrefix + "ERROR - " + parseError);
      // Log unknown ID to its own verification sheet
      logToVerificationSheet(receivedTimestamp, rawData, parsedData, parseError, null, null, null, CONFIG.unknown.verifySheet);
      return ContentService.createTextOutput("Error: Unknown Import ID '" + importId + "'.");
    }

    importConfig = CONFIG[configKey];
    verifySheetName = importConfig.verifySheet; // Set correct verification sheet from now on
    Logger.log(logPrefix + "Routing Import ID '" + importId + "' to config type: '" + importConfig.type + "'");

    // --- 5. Process Completion Data (if applicable) ---
    // Check for end_time which signifies a completed import from the PHP plugin
    if (parsedData.end_time && typeof parsedData.end_time === 'number') {
      Logger.log(logPrefix + "Processing completed import for ID: " + importId);

      // Call helpers, passing the relevant config details
      const resultsLogSheetName = importConfig.importLogSheet;
      const cacheKeyPrefix = importConfig.cachePrefix;

      // 5a. Log Results to specific sheet
      const sheetResult = logImportResultsToSheet(parsedData, resultsLogSheetName);
      if (!sheetResult.success) {
           sheetUpdateError = sheetResult.error;
           Logger.log(logPrefix + "Error logging results to sheet: " + sheetUpdateError);
      }

      // 5b. Update Cache Status to 'complete'
      const cacheResult = updateCompletionCache(parsedData, cacheKeyPrefix);
      if (!cacheResult.success) {
          cacheError = cacheResult.error;
          Logger.log(logPrefix + "Error updating cache: " + cacheError);
      }

    } else {
      Logger.log(logPrefix + "Callback for Import ID '" + importId + "' received, but 'end_time' is missing or invalid. Not processing as complete.");
      // Optional: Log non-completion callbacks if needed, currently only completion data triggers logging/cache
    }

    // --- 6. Log to Verification Sheet (Always - using the routed sheet name) ---
    logToVerificationSheet(receivedTimestamp, rawData, parsedData, parseError, sheetUpdateError, cacheError, null, verifySheetName);

    // --- 7. Respond to WordPress ---
    // Always send 200 OK if we reached this point, acknowledging receipt.
    let responseMessage = "Callback Handler received data for Import ID: " + importId + ".";
    const processingErrors = [parseError, sheetUpdateError, cacheError].filter(Boolean); // Filter out null/empty errors
    if (processingErrors.length > 0) {
      responseMessage += " Processed with warnings/errors on GAS side: " + processingErrors.join('; ');
      Logger.log(logPrefix + "Responding 200 OK to WP, but with processing errors noted.");
    } else {
      responseMessage += " Processed successfully on GAS side.";
      Logger.log(logPrefix + "Responding 200 OK to WP with success message.");
    }
    return ContentService.createTextOutput(responseMessage).setMimeType(ContentService.MimeType.TEXT);

  } catch (fatalError) {
    fatalErrorObj = fatalError; // Capture the error object
    Logger.log(logPrefix + "FATAL ERROR during doPost processing for Import ID '" + (importId || 'Unknown') + "': " + fatalErrorObj);
    if (fatalErrorObj.stack) {
        Logger.log(logPrefix + "Stack Trace: " + fatalErrorObj.stack);
    }
    // Attempt to log fatal error details to the verification sheet determined (or fallback)
    logToVerificationSheet(receivedTimestamp, rawData, parsedData, parseError, sheetUpdateError, cacheError, "FATAL doPost Error: " + fatalErrorObj.toString(), verifySheetName);
    // Return 200 OK but indicate server error in message to WP
    return ContentService.createTextOutput("Fatal Error during doPost processing on Callback Handler side. Check GAS logs for Import ID '" + (importId || 'Unknown') + "'.")
           .setMimeType(ContentService.MimeType.TEXT);
  }
}


// =======================================================
//                 HELPER FUNCTIONS
// =======================================================

/**
 * Appends WP All Import results data to the specified log sheet.
 * Creates the sheet and header row if it doesn't exist.
 * @param {object} data Parsed JSON data from WP callback (must contain import_id, end_time).
 * @param {string} logSheetName The name of the Google Sheet to log results to.
 * @return {{ success: boolean, error: string | null }} Operation status.
 */
function logImportResultsToSheet(data, logSheetName) {
    const logPrefix = "HELPER [logImportResults]: ";
    try {
        const importId = data.import_id ? String(data.import_id) : null;
        if (!importId) { throw new Error("Missing import_id in data."); }
        if (!logSheetName) { throw new Error("Log sheet name was not provided."); }

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
            // Apply formatting (adjust columns as needed)
            sheet.getRange("B:C").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Formatted dates
            sheet.getRange("I:I").setNumberFormat("yyyy-mm-dd hh:mm:ss"); // Callback time
            sheet.getRange("J:K").setNumberFormat("0"); // Unix timestamps as plain numbers
            sheet.autoResizeColumns(1, sheet.getLastColumn());
            Logger.log(logPrefix + "Created WP Import Results Log sheet: '" + logSheetName + "'");
        }

        // Calculate duration
        let durationMinutes = 'N/A';
        if (startTime && endTime && typeof startTime === 'number' && typeof endTime === 'number' && endTime >= startTime) {
             durationMinutes = ((endTime - startTime) / 60).toFixed(2);
        }

        // Format timestamps using script's timezone
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
        sheet.setRowHeight(sheet.getLastRow(), 21); // Set default row height

        Logger.log(logPrefix + "Appended results for Import ID " + importId + " to sheet '" + logSheetName + "'");
        return { success: true, error: null };

    } catch (e) {
        Logger.log(logPrefix + "ERROR logging results to sheet '" + logSheetName + "' (ID: " + (data.import_id || 'Unknown') + "): " + e.message);
        Logger.log(e.stack); // Log stack trace for debugging sheet errors
        return { success: false, error: e.message };
    }
}


/**
 * Stores import completion results in CacheService using a key derived from prefix and ID.
 * @param {object} data Parsed JSON data from WP callback (must contain import_id, end_time).
 * @param {string} cacheKeyPrefix The prefix defined in CONFIG for this import type.
 * @return {{ success: boolean, error: string | null }} Operation status.
 */
 function updateCompletionCache(data, cacheKeyPrefix) {
     const logPrefix = "HELPER [updateCompletionCache]: ";
     let importId = null; // Keep ID accessible for logging
     try {
         importId = data.import_id ? String(data.import_id) : null;
         if (!importId) { throw new Error("Missing import_id for cache update."); }
         if (!data.end_time || typeof data.end_time !== 'number') { throw new Error("Missing/invalid end_time for cache update."); }
         if (cacheKeyPrefix === undefined || cacheKeyPrefix === null) { throw new Error("Missing cacheKeyPrefix for cache update."); }

         const cacheKey = cacheKeyPrefix + importId; // Construct the specific cache key

         const resultsData = {
             status: 'complete', // Mark as complete
             importId: importId,
             created: data.posts_created || 0, updated: data.posts_updated || 0,
             deleted: data.posts_deleted || 0, skipped: data.posts_skipped || 0,
             startTime: data.start_time || null, // Unix timestamp (seconds)
             endTime: data.end_time,             // Unix timestamp (seconds)
             receivedTime: Math.floor(new Date().getTime() / 1000), // Unix timestamp (seconds) when GAS processed completion
             message: "Import completed successfully via callback." // Default message
         };

         Logger.log(logPrefix + "Attempting to update cache key: '" + cacheKey + "' with status: 'complete'");
         // Logger.log(logPrefix + "Cache data: " + JSON.stringify(resultsData)); // Uncomment for deep debugging

         // Put data in cache with standard expiration
         cache.put(cacheKey, JSON.stringify(resultsData), CACHE_EXPIRATION_SECONDS);

         Logger.log(logPrefix + "Successfully stored 'complete' status in cache for key '" + cacheKey + "'");
         return { success: true, error: null };

     } catch (e) {
         Logger.log(logPrefix + "ERROR updating cache for Import ID '" + (importId || 'Unknown') + "': " + e.message);
         Logger.log(e.stack); // Log stack trace
         return { success: false, error: e.message };
     }
 }


/**
 * Logs callback details (raw data snippet, errors) to a specified verification sheet.
 * Creates the sheet and header row if it doesn't exist. Robust against errors.
 * @param {Date} timestamp When the callback was received by doPost.
 * @param {string} rawData Raw POST body content.
 * @param {object|null} parsedData Parsed JSON object, or null if parsing failed/not applicable.
 * @param {string|null} parseError Error message during JSON parsing.
 * @param {string|null} sheetError Error message during results sheet logging.
 * @param {string|null} cacheError Error message during cache update.
 * @param {string|null} fatalError General/fatal error message during doPost.
 * @param {string} verifySheetName The name of the verification sheet to use (should be provided even on error).
 */
function logToVerificationSheet(timestamp, rawData, parsedData, parseError, sheetError, cacheError, fatalError, verifySheetName) {
    const logPrefix = "HELPER [logToVerification]: ";
     if (!verifySheetName) {
         // This is a critical fallback failure, log it prominently but don't throw
         Logger.log(logPrefix + "CRITICAL ERROR - Verification sheet name was not provided. Cannot log verification data. Timestamp: " + timestamp.toISOString());
         return;
     }
    try {
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        let sheet = ss.getSheetByName(verifySheetName);
        if (!sheet) {
            // Try to create the sheet if it doesn't exist
            try {
                sheet = ss.insertSheet(verifySheetName);
                // Define standard headers for verification logs
                sheet.appendRow([
                    "Timestamp Received", "Raw Data Snippet", "Parsed Import ID", "Parsed End Time (Unix)",
                    "Parse/Route Error", "Results Log Error", "Cache Update Error", "Fatal doPost Error"
                ]);
                sheet.setFrozenRows(1);
                sheet.getRange("A:A").setNumberFormat("yyyy-mm-dd hh:mm:ss");
                sheet.setColumnWidths(1, 1, 150); // Timestamp
                sheet.setColumnWidths(2, 1, 300); // Raw Data
                sheet.setColumnWidths(3, 2, 120); // ID, EndTime
                sheet.setColumnWidths(5, 4, 200); // Errors
                Logger.log(logPrefix + "Created Verification Log sheet: '" + verifySheetName + "'");
            } catch (createSheetErr) {
                 Logger.log(logPrefix + "CRITICAL ERROR - Failed to CREATE verification sheet '" + verifySheetName + "': " + createSheetErr.message);
                 // If sheet creation fails, we can't log this attempt there. Logging here is the best we can do.
                 return;
            }
        }

        // Prepare data for logging
        const importIdStr = parsedData && parsedData.import_id ? String(parsedData.import_id) : (parseError && parseError.includes("Unknown Import ID") ? parseError.split("'")[1] : ''); // Try to get ID even if unknown
        const endTimeStr = parsedData && parsedData.end_time ? parsedData.end_time : '';
        const rawDataSnippet = String(rawData || '').substring(0, 500) + (String(rawData || '').length > 500 ? '...' : '');

         // Append verification data row
         sheet.appendRow([
            timestamp,
            rawDataSnippet,
            importIdStr,
            endTimeStr,
            parseError || '', // Consolidate Parse/Route/ID errors here
            sheetError || '',
            cacheError || '',
            fatalError || ''
        ]);
        sheet.setRowHeight(sheet.getLastRow(), 21); // Set default row height

    } catch(sheetLogErr) {
        // Log critically if we can't write to the verification sheet AFTER potentially creating it
        Logger.log(logPrefix + "CRITICAL ERROR - Could not write to verification sheet '" + verifySheetName + "': " + sheetLogErr.message);
        Logger.log(sheetLogErr.stack); // Log stack trace for the logging error itself
    }
}


// =======================================================
//       LIBRARY FUNCTION (for Dashboard Scripts)
// =======================================================

/**
 * Retrieves the cached status object for a given import ID.
 * Intended to be called from other scripts via this script published as a Library.
 *
 * @param {string} importId The WP All Import ID (as a string, e.g., '31') for which to retrieve status.
 * @return {object|null} Parsed status object from cache (containing status, importId, created,
 *                       updated, deleted, skipped, startTime, endTime, receivedTime, message),
 *                       or null if the import ID is unknown, status is not found in cache,
 *                       has expired, or cache data is corrupt.
 * @customfunction
 */
function getImportStatusFromCache(importId) {
    const cache = CacheService.getScriptCache(); // Accesses THIS script's (the library's) cache
    const logPrefixLib = "LIBRARY [getImportStatusFromCache]: ";
    let cacheKey = null;

    if (!importId || typeof importId !== 'string') {
         Logger.log(logPrefixLib + "Invalid input: importId must be a non-empty string. Received: " + importId);
         return null;
    }

    // Find the configuration key associated with the provided import ID
    const configKey = WP_IMPORT_ID_TO_CONFIG_KEY[importId];

    if (configKey && CONFIG[configKey] && CONFIG[configKey].cachePrefix !== undefined) {
         // Construct the cache key using the prefix from the config
         cacheKey = CONFIG[configKey].cachePrefix + importId;
    } else {
         // Log if the import ID doesn't map to a known configuration
         Logger.log(logPrefixLib + "Could not find configuration or cachePrefix for import ID: '" + importId + "'. Cannot determine cache key.");
         return null; // Cannot proceed without config/prefix
    }

    Logger.log(logPrefixLib + "Attempting to get status for Import ID '" + importId + "' using cache key: '" + cacheKey + "'");

    // Attempt to retrieve the value from the script cache
    const cachedValue = cache.get(cacheKey);

    if (cachedValue) {
        // If a value is found, attempt to parse it as JSON
        try {
            const statusData = JSON.parse(cachedValue);
            Logger.log(logPrefixLib + "Found and successfully parsed data in cache for key '" + cacheKey + "'. Status: " + statusData.status);
            return statusData; // Return the parsed status object
        } catch (e) {
            // Log an error if the cached data is not valid JSON
            Logger.log(logPrefixLib + "ERROR parsing cached JSON for key '" + cacheKey + "': " + e.message);
            // Optional: Remove the corrupted cache entry to prevent repeated errors
            // cache.remove(cacheKey);
            // Logger.log(logPrefixLib + "Removed potentially corrupt cache entry for key: " + cacheKey);
            return null; // Return null indicating corrupted cache data
        }
    } else {
        // Log if no data is found for the key (either never set, expired, or manually removed)
        Logger.log(logPrefixLib + "No data found in cache for key '" + cacheKey + "' (may be expired or import hasn't completed recently).");
        return null; // Return null indicating status not found/expired
    }
}


// =======================================================
//   LIBRARY FUNCTION (Cache Clearing for Dashboards)
// =======================================================

/**
 * Clears the cached status object for a given import ID from THIS script's cache.
 * Intended to be called from other scripts (like dashboards) via this script
 * published as a Library, typically to manually clear a 'pending' or stuck status.
 *
 * @param {string} importId The WP All Import ID (as a string, e.g., '31') for which to clear the cached status.
 * @return {{ success: boolean, message: string }} An object indicating the result of the clear operation.
 * @customfunction
 */
function clearImportStatusInCache(importId) {
    const cache = CacheService.getScriptCache(); // Accesses THIS script's (the library's) cache
    const logPrefixLibClear = "LIBRARY [clearImportStatusInCache]: ";
    let cacheKey = null;
    let success = false;
    let message = "";

    if (!importId || typeof importId !== 'string') {
         Logger.log(logPrefixLibClear + "Invalid input: importId must be a non-empty string. Received: " + importId);
         return { success: false, message: "Invalid Import ID provided." };
    }

    // Determine the cache key using the same logic as getImportStatusFromCache
    const configKey = WP_IMPORT_ID_TO_CONFIG_KEY[importId];
    if (configKey && CONFIG[configKey] && CONFIG[configKey].cachePrefix !== undefined) {
         cacheKey = CONFIG[configKey].cachePrefix + importId;
         Logger.log(logPrefixLibClear + "Determined cache key for clear operation: '" + cacheKey + "' for Import ID '" + importId + "'");
    } else {
         Logger.log(logPrefixLibClear + "Could not find configuration or cachePrefix for import ID: '" + importId + "'. Cannot determine cache key to clear.");
         return { success: false, message: "Unknown Import ID '" + importId + "'. Cannot clear cache." };
    }

    try {
        // Check if the key exists before removing (optional, but good for logging)
        const currentValue = cache.get(cacheKey);
        if (currentValue) {
            Logger.log(logPrefixLibClear + "Found existing value for key '" + cacheKey + "', attempting removal.");
        } else {
             Logger.log(logPrefixLibClear + "No existing value found for key '" + cacheKey + "'. Cache is already clear.");
             // Return success because the goal is achieved (key doesn't exist)
             return { success: true, message: "Status for Import ID " + importId + " was already clear (not found in cache)." };
        }

        // --- Remove the cache entry ---
        cache.remove(cacheKey);

        // Verify removal
        const valueAfterRemove = cache.get(cacheKey);
        if (valueAfterRemove === null) {
            success = true;
            message = "Successfully cleared cached status for Import ID " + importId + ".";
            Logger.log(logPrefixLibClear + message + " (Key: '" + cacheKey + "')");
        } else {
            // This shouldn't normally happen but check just in case
            success = false;
            message = "Cache removal command sent, but key '" + cacheKey + "' still exists. Check Handler script logs.";
            Logger.log(logPrefixLibClear + "WARN - " + message);
        }
    } catch (e) {
        success = false;
        message = "Error occurred during cache clear operation for Import ID " + importId + ": " + e.message;
        Logger.log(logPrefixLibClear + "ERROR - " + message);
        Logger.log(e.stack);
    }

    return { success: success, message: message };
}