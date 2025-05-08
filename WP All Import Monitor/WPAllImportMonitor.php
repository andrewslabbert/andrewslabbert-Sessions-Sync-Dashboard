<?php
/**
 * Plugin Name: WP All Import - Monitor
 * Description: Tracks WP All Import progress, sends data to GAS dashboard, and provides a cache clearing endpoint. Use Central All Import Hanlder for all sites.
 * Version: 2.1
 * Author: Four12 Global
 */

// Prevent direct access
if (!defined('ABSPATH')) {
    exit;
}

// --- Constants ---
// Define the security key once - Ensure this matches GAS Script Properties
define('WPAIP_SECURITY_KEY', 'DF4J01r');

// --- Action Handler ---
// Hook into 'init' early to handle custom actions before heavy lifting
add_action('init', 'wpaip_handle_direct_actions', 5);

/**
 * Handles direct actions intended for this plugin, like cache clearing.
 * Lets other actions (trigger, processing, cancel) be handled by WP / WP All Import.
 */
function wpaip_handle_direct_actions() {
    // Check if it's a request possibly intended for our plugin
    if (!isset($_GET['import_key']) || !isset($_GET['action'])) {
        return; // Not relevant or malformed request
    }

    // Security Check
    if (sanitize_text_field($_GET['import_key']) !== WPAIP_SECURITY_KEY) {
        // Use helper to send standardized error and exit
        wpaip_send_json_error('Invalid security key provided.', 403);
    }

    // Route ONLY the custom actions this plugin provides
    $action = sanitize_text_field($_GET['action']);

    switch ($action) {
        case 'clear_breeze_cache':
            wpaip_do_clear_breeze_cache(); // This function will handle response and exit
            break;

        case 'run_cli':
            if (isset($_GET['import_id'])) {
                $import_id_int = intval($_GET['import_id']);
                // Get launch result including exit code and output
                $launch_result = wpaip_launch_import_cli($import_id_int);

                // Send JSON response with launch details
                wpaip_send_json_success(
                    "WP-CLI import process for ID " . $import_id_int . " has been initiated.",
                    200,
                    [
                        'import_id' => $import_id_int,
                        'status_message' => 'CLI initiated',
                        'launch_result' => $launch_result
                    ]
                );
            } else {
                wpaip_send_json_error('Import ID not provided for run_cli action.', 400);
            }
            break;

        // IMPORTANT: NO 'cancel' or 'cancel_import' case here.
        // Let WordPress and WP All Import handle their native actions.

        default:
            // Action is not one we specifically handle. Let WordPress/other plugins proceed.
            // Optional: Log unknown actions if debugging needed.
            // error_log("WPAIP: Received unhandled action: " . $action);
            break;
    }
    // If the action wasn't handled by our switch (e.g., 'trigger', 'processing', 'cancel'), execution continues.
}

// --- Custom Action Functions ---

/**
 * Handles the 'clear_breeze_cache' action request.
 * Uses WP-CLI via exec to clear the cache.
 */
function wpaip_do_clear_breeze_cache() {
    $log_prefix = "WP All Import Sync Dashboard (Breeze Clear): ";
    $cli_command = 'wp breeze purge --cache=all'; // Base command
    $output = array();
    $return_var = -1;
    $full_command = '';

    // Check if exec is available - crucial for this function
    if (!function_exists('exec')) {
        error_log($log_prefix . "ERROR - exec() function is disabled. Cannot clear Breeze cache via WP-CLI.");
        wpaip_send_json_error('Server configuration prevents cache clearing (exec disabled).', 501); // 501 Not Implemented
    }

    // Try to find the WP CLI path automatically, otherwise assume it's in PATH
    $wp_cli_path = 'wp'; // Default assumption

    // Construct the command with the absolute path to WordPress if possible for reliability
    if (defined('ABSPATH')) {
        // Use escapeshellarg for security on the path
        $full_command = $wp_cli_path . ' breeze purge --cache=all --path=' . escapeshellarg(ABSPATH);
    } else {
        // Fallback if ABSPATH isn't defined (shouldn't happen in 'init', but safety first)
        error_log($log_prefix . "ABSPATH not defined. Cannot determine WP path for WP-CLI.");
        wpaip_send_json_error('WordPress path could not be determined for cache clearing.', 500);
    }

    error_log($log_prefix . "Attempting command: " . $full_command);

    // Execute the command, capturing both stdout and stderr (2>&1)
    exec($full_command . ' 2>&1', $output, $return_var);

    $output_string = implode("\n", $output);
    error_log($log_prefix . "Command output: " . $output_string);
    error_log($log_prefix . "Command return code: " . $return_var);

    // Check return code AND output for success confirmation
    if ($return_var === 0 && strpos(strtolower($output_string), 'success') !== false) {
        wpaip_send_json_success(
            'Breeze cache cleared successfully.',
             200,
             ['output' => $output_string]
        );
    } else {
        wpaip_send_json_error(
            'Failed to clear Breeze cache. Check WP error logs for details.',
            500, // Internal Server Error likely
            ['output' => $output_string, 'return_code' => $return_var]
        );
    }
    // Note: wpaip_send_json_success/error include exit()
}

/**
 * Launches WP‑CLI for a specific import in the background.
 */
/**
 * Launches WP-CLI for a specific import in the background.
 * Logs output to a file within this plugin's directory.
 */
function wpaip_launch_import_cli( $import_id ) {
    $wp   = '/usr/local/bin/wp'; // Your verified WP-CLI path
    $root = ABSPATH;             // WordPress root for this instance

    // --- Define Log Directory and File ---
    $plugin_base_dir = plugin_dir_path(__FILE__); // Gets /path/to/wp-content/plugins/wp-all-import-monitor/
    $log_dir_name = 'import-logs'; // Just the name of the subdirectory
    $log_dir_path = $plugin_base_dir . $log_dir_name . '/'; // Full path to log directory
    $log_file_name = 'wpai_' . (int) $import_id . '.log'; // e.g., wpai_31.log
    $log_file_full_path = $log_dir_path . $log_file_name;

    // --- Ensure Log Directory Exists ---
    if (!file_exists($log_dir_path)) {
        // Try to create it. Set permissions carefully.
        if (!mkdir($log_dir_path, 0755, true) && !is_dir($log_dir_path)) {
            error_log("WPAIP: ERROR - Could not create log directory: " . $log_dir_path);
            $log_file_full_path = '/tmp/wpai_fallback_' . (int) $import_id . '.log'; // Fallback if dir creation fails
            error_log("WPAIP: Falling back to temporary log file: " . $log_file_full_path);
        } else {
            error_log("WPAIP: Log directory created or already exists: " . $log_dir_path);
            // Optional: Add an .htaccess to deny direct web access
            if (!file_exists($log_dir_path . '.htaccess')) {
                @file_put_contents($log_dir_path . '.htaccess', "Require all denied\n");
            }
            // Optional: Add an empty index.html to prevent directory listing
            if (!file_exists($log_dir_path . 'index.html')) {
                @file_put_contents($log_dir_path . 'index.html', "<!-- Silence is golden -->\n");
            }
        }
    } else {
         error_log("WPAIP: Log directory already exists: " . $log_dir_path);
    }

    // Build a FULL command string
    $cmd = sprintf(
        'setsid %s --path=%s all-import run %d --quiet --force-run '
      . '> %s 2>&1 < /dev/null & echo $!',
        escapeshellarg( $wp ),
        escapeshellarg( $root ),
        (int) $import_id,
        escapeshellarg( $log_file_full_path )
    );

    $output = [];
    $code   = 0;
    error_log( "WPAIP: WP-CLI for import {$import_id} will attempt to log to: " . $log_file_full_path );

    $pid    = exec( $cmd, $output, $code );

    error_log( "WPAIP: Spawn cmd for import {$import_id} resulted in exit_code=$code, pid=$pid. Log actual path: " . $log_file_full_path );

    return [
        'success' => ($code === 0 && !empty($pid)),
        'exit_code' => $code,
        'pid' => $pid,
        'log_file' => $log_file_full_path,
        'raw_output' => $output
    ];
}

// --- JSON Response Helpers ---

/**
 * Sends a standardized JSON response and exits.
 *
 * @param bool   $success    True for success, false for error.
 * @param string $message    The primary message.
 * @param int    $http_code  HTTP status code.
 * @param array  $extra_data Optional additional data under the 'data' key.
 */
function wpaip_send_json_response($success, $message, $http_code = 200, $extra_data = array()) {
    $response_data = array(
        'success' => (bool)$success,
        // Ensure 'data' key exists as per WP standard
        'data' => array_merge(
            array('message' => $message), // Core message
            (array)$extra_data // Merge any additional context
         )
    );
    // Log errors server-side before sending response
    if (!$success) {
         error_log("WPAIP Error Response: Code=$http_code, Msg=$message, Data=" . print_r($extra_data, true));
    }
    wp_send_json($response_data, $http_code);
    // wp_send_json includes exit()
}

/**
 * Helper to send a success JSON response.
 */
function wpaip_send_json_success($message, $http_code = 200, $extra_data = array()) {
    wpaip_send_json_response(true, $message, $http_code, $extra_data);
}

/**
 * Helper to send an error JSON response.
 */
function wpaip_send_json_error($message, $http_code = 400, $extra_data = array()) {
    wpaip_send_json_response(false, $message, $http_code, $extra_data);
}


// --- WP All Import Data Tracking Utilities ---

/**
 * Gets temporary import data stored in options.
 *
 * @param int $import_id The import ID.
 * @return array|false The import data array or false on failure.
 */
function wpaip_get_import_data($import_id) {
    if (!$import_id) return false;
    $option_name = 'wpaip_import_data_' . intval($import_id);
    // Provide default structure if option doesn't exist
    $defaults = array(
        'start_time' => null,
        'posts_created' => 0,
        'posts_updated' => 0,
        'posts_deleted' => 0,
        'posts_skipped' => 0,
        'end_time' => null,
        'error' => null,
        'import_id' => intval($import_id)
        // Removed string time versions - GAS can format timestamps
    );
    return get_option($option_name, $defaults);
}

/**
 * Updates temporary import data in options.
 *
 * @param int   $import_id The import ID.
 * @param array $data      The data array to save.
 * @return bool True if updated/added, false on failure.
 */
function wpaip_update_import_data($import_id, $data) {
    if (!$import_id || !is_array($data)) return false;
    $option_name = 'wpaip_import_data_' . intval($import_id);
    // Ensure import_id is always part of the data being saved
    $data['import_id'] = intval($import_id);
    // Use autoload 'no' to prevent loading this data on every page load
    return update_option($option_name, $data, 'no');
}

/**
 * Deletes temporary import data from options.
 *
 * @param int $import_id The import ID.
 * @return bool True if deleted, false on failure or if not found.
 */
function wpaip_clear_import_data($import_id) {
    if (!$import_id) return false;
    $option_name = 'wpaip_import_data_' . intval($import_id);
    return delete_option($option_name);
}

/**
 * Attempts to determine the current WP All Import ID.
 * Tries multiple methods for robustness.
 *
 * @return int|null The import ID or null if not found.
 */
function wpaip_get_current_import_id() {
    // Method 1: WP All Import's function (Best if available in hook context)
    if (function_exists('wp_all_import_get_import_id')) {
        $import_id = wp_all_import_get_import_id();
        if ($import_id) {
            return intval($import_id);
        }
    }

    // Method 2: Instantiate the record class (More general fallback)
    if (class_exists('PMXI_Import_Record')) {
        try {
            $import_record = new PMXI_Import_Record();
            if (method_exists($import_record, 'getImportId')) {
                $import_id = $import_record->getImportId();
                if ($import_id) {
                    return intval($import_id);
                }
            }
        } catch (Throwable $e) { // Catch modern errors/exceptions
             error_log("WPAIP (wpaip_get_current_import_id): Exception getting ID via PMXI_Import_Record: " . $e->getMessage());
        }
    }

    // Method 3: Check GET/POST (Less reliable for hooks, but useful for direct calls)
     if (!empty($_GET['import_id'])) return intval($_GET['import_id']);
     if (!empty($_POST['import_id'])) return intval($_POST['import_id']);

     // If we reach here, ID couldn't be determined
     error_log("WPAIP (wpaip_get_current_import_id): Could not determine current Import ID.");
    return null;
}


// --- WP All Import Action Hooks for Stat Tracking ---

/**
 * Hook: pmxi_before_xml_import
 * Initializes the tracking data when an import starts.
 */
add_action('pmxi_before_xml_import', 'wpaip_before_xml_import', 10, 1);
function wpaip_before_xml_import($import_id) {
    if (!$import_id) {
         error_log("WPAIP (pmxi_before_xml_import): Hook fired with invalid Import ID.");
         return;
    }
    $import_id = intval($import_id);
    $current_timestamp = current_time('timestamp', true); // GMT timestamp

    $import_data = array(
        'start_time' => $current_timestamp,
        'posts_created' => 0,
        'posts_updated' => 0,
        'posts_deleted' => 0,
        'posts_skipped' => 0,
        'end_time' => null,
        'error' => null,
        'import_id' => $import_id
    );
    wpaip_update_import_data($import_id, $import_data);
    error_log("WPAIP: Initialized tracking for Import ID " . $import_id);
}

/**
 * Hook: pmxi_saved_post
 * Increments created/updated counters.
 */
add_action('pmxi_saved_post', 'wpaip_saved_post', 10, 3);
function wpaip_saved_post($post_id, $xml_node, $is_update) {
    $current_import_id = wpaip_get_current_import_id();
    if (!$current_import_id) {
        error_log("WPAIP (pmxi_saved_post): Could not get current Import ID. Skipping count.");
        return;
    }

    $import_data = wpaip_get_import_data($current_import_id);
    if ($import_data === false) {
         error_log("WPAIP (pmxi_saved_post): Failed to get import data for ID " . $current_import_id);
         return;
    }

    if ($is_update) {
        $import_data['posts_updated'] = isset($import_data['posts_updated']) ? intval($import_data['posts_updated']) + 1 : 1;
    } else {
        $import_data['posts_created'] = isset($import_data['posts_created']) ? intval($import_data['posts_created']) + 1 : 1;
    }
    wpaip_update_import_data($current_import_id, $import_data);
}

/**
 * Hook: pmxi_delete_post
 * Increments deleted counter.
 */
add_action('pmxi_delete_post', 'wpaip_delete_post', 10, 2);
function wpaip_delete_post($ids_to_delete, $import) {
    // Validate input
    if (empty($ids_to_delete) || !is_array($ids_to_delete) || !is_object($import) || !isset($import->id)) {
         error_log("WPAIP (pmxi_delete_post): Invalid arguments or no IDs to delete.");
        return;
    }
    $import_id = intval($import->id);
    $import_data = wpaip_get_import_data($import_id);
     if ($import_data === false) {
         error_log("WPAIP (pmxi_delete_post): Failed to get import data for ID " . $import_id);
         return;
    }

    $delete_count = count($ids_to_delete);
    $import_data['posts_deleted'] = isset($import_data['posts_deleted']) ? intval($import_data['posts_deleted']) + $delete_count : $delete_count;
    wpaip_update_import_data($import_id, $import_data);
    error_log("WPAIP (pmxi_delete_post): Recorded " . $delete_count . " deletions for Import ID " . $import_id);
}

/**
 * Hook: wp_all_import_post_skipped
 * Increments skipped counter.
 */
add_action('wp_all_import_post_skipped', 'wpaip_post_skipped', 10, 3);
function wpaip_post_skipped($post_id_or_term_id, $import_id, $import_record_data) {
     if (!$import_id) {
        error_log("WPAIP (wp_all_import_post_skipped): Hook fired with invalid Import ID.");
        return;
    }
    $import_id = intval($import_id);
    $import_data = wpaip_get_import_data($import_id);
     if ($import_data === false) {
         error_log("WPAIP (wp_all_import_post_skipped): Failed to get import data for ID " . $import_id);
         return;
    }
    $import_data['posts_skipped'] = isset($import_data['posts_skipped']) ? intval($import_data['posts_skipped']) + 1 : 1;
    wpaip_update_import_data($import_id, $import_data);
}

/**
 * Hook: pmxi_after_xml_import
 * Finalizes data, sends it to GAS, and cleans up.
 */
add_action('pmxi_after_xml_import', 'wpaip_after_xml_import', 10, 2);
function wpaip_after_xml_import($import_id, $import) {
     // $import_id from hook argument is usually reliable here
     if (!$import_id) {
        error_log("WPAIP (pmxi_after_xml_import): Hook fired with invalid Import ID. Trying object.");
        $import_id = (is_object($import) && isset($import->id)) ? intval($import->id) : null;
        if (!$import_id) {
             error_log("WPAIP (pmxi_after_xml_import): Still no valid Import ID found. Aborting.");
             return;
        }
    }
    $import_id = intval($import_id);

    $import_data = wpaip_get_import_data($import_id);
     if ($import_data === false) {
         error_log("WPAIP (pmxi_after_xml_import): Failed to get import data for ID " . $import_id . ". Cannot finalize or send.");
         return;
    }

    // Record end time
    $import_data['end_time'] = current_time('timestamp', true); // GMT timestamp

    // Attempt to get more accurate final stats from the $import object if possible
     if (is_object($import) && isset($import->options['last_run']) && is_array($import->options['last_run'])) {
        $last_run_stats = $import->options['last_run'];
        // // Overwrite manually counted stats with potentially more accurate final counts
        // if (isset($last_run_stats['created'])) $import_data['posts_created'] = intval($last_run_stats['created']);
        // if (isset($last_run_stats['updated'])) $import_data['posts_updated'] = intval($last_run_stats['updated']);
        // if (isset($last_run_stats['skipped'])) $import_data['posts_skipped'] = intval($last_run_stats['skipped']);
        // Note: 'deleted' count from pmxi_delete_post hook is likely the only reliable source.
        error_log("WPAIP: Using final stats from import object's last_run for Import ID " . $import_id);
    } else {
         error_log("WPAIP: Using manually counted stats (or last_run unavailable) for Import ID " . $import_id);
    }

    // Ensure import ID is correct before sending
    $import_data['import_id'] = $import_id;

    // --- Send Data to Google Apps Script ---
    wpaip_send_data_to_script($import_id, $import_data);

    // --- Cleanup Temporary Data ---
    wpaip_clear_import_data($import_id);
    error_log("WPAIP: Cleared temporary tracking data for Import ID " . $import_id);
}


// --- Data Sending Function ---

/**
 * Sends the collected import data to the Google Apps Script Web App.
 *
 * @param int   $import_id   The import ID.
 * @param array $import_data The collected data array.
 */
function wpaip_send_data_to_script($import_id, $import_data) {
    $log_prefix = "WPAIP (Send Data ID: " . $import_id . "): ";
    if (!$import_id || !is_array($import_data)) {
        error_log($log_prefix . "ERROR - Invalid arguments provided.");
        return;
    }

    // =================== ACTION REQUIRED ===================
    // vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    // **IMPORTANT**: Replace the URL below with the deployed URL
    //               of your DEDICATED "WP Callback Handler" Apps Script.
    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    $script_url = add_query_arg(
        'secret',
        WPAIP_SECURITY_KEY,
        'https://script.google.com/macros/s/AKfycbyjMiz0VBM6BSRe-fcjxt0S-f8m-QAr5TLKJb7Muoph7kwUapTGKi_LP6u8tbz1hgdG/exec'
    );

    if (empty($script_url) || $script_url === 'PASTE_YOUR_NEW_CALLBACK_HANDLER_APPS_SCRIPT_URL_HERE/exec') {
        error_log($log_prefix . "ERROR - Google Apps Script URL is not configured correctly in the plugin!");
        return;
    }

     // Prepare final payload
     $import_data['import_id'] = $import_id; // Ensure correct ID
     $import_data['import_complete'] = true; // Flag for GAS
     unset($import_data['start_time_string']); // Remove if GAS handles formatting
     unset($import_data['end_time_string']);   // Remove if GAS handles formatting

    $payload = wp_json_encode($import_data);

    // Check for JSON encoding errors
    if (json_last_error() !== JSON_ERROR_NONE) {
        error_log($log_prefix . "ERROR - Failed to encode data as JSON. Error: " . json_last_error_msg());
        error_log($log_prefix . "Data structure was: " + print_r($import_data, true));
        return;
    }

    // Log before sending
    error_log( $log_prefix . "=== BEFORE wp_remote_post ===" );

    // Use cURL directly to ensure payload is sent untouched
    $curl = curl_init( $script_url );
    curl_setopt_array( $curl, [
        CURLOPT_POST            => true,
        CURLOPT_POSTFIELDS      => $payload,
        CURLOPT_HTTPHEADER      => [
            'Content-Type: application/json',
            'Content-Length: ' . strlen( $payload ),
            'Expect:',
        ],
        CURLOPT_RETURNTRANSFER  => true,
        CURLOPT_TIMEOUT         => 60,
    ] );

    $response_body  = curl_exec( $curl );
    $response_code  = curl_getinfo( $curl, CURLINFO_HTTP_CODE );
    $curl_error     = curl_error( $curl );
    curl_close( $curl );

    error_log( $log_prefix . "Received RESPONSE. Code: {$response_code}" );
    if ( $curl_error ) {
        error_log( $log_prefix . "cURL error: {$curl_error}" );
    }

    // Process the response
    if ( in_array( $response_code, [200, 302], true ) ) {
        error_log( $log_prefix . "Callback accepted (HTTP {$response_code})." );
    } else {
        error_log( $log_prefix . "WARNING – Non-200/302 Response: {$response_body}" );
    }
}
?>