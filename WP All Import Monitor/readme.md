# WP All Import Monitor

**Version:** 2.1
**Author:** Four12 Global

A WordPress plugin that tracks WP All Import progress, exposes import statistics, and provides a cache-clearing endpoint. It works in conjunction with the Central Import Handler Google Apps Script for a unified import dashboard experience.

---

## Features

* **Import Monitoring:** Hooks into WP All Import lifecycle to count created, updated, deleted, and skipped items.
* **Logging:** Stores import data in WordPress options (no autoload) for temporary tracking.
* **Cache Clearing Endpoint:** Secure `?import_key=<key>&action=clear_breeze_cache` URL to purge Breeze cache via WP-CLI.
* **Callback Posting:** Sends a JSON payload of final import stats back to your GAS callback handler.

---

## Installation

1. Copy the `WPAllImportMonitor.php` file (and supporting classes) into a folder named `wpallimport-monitor` under `wp-content/plugins/`.
2. In your WordPress admin, go to **Plugins** and activate **WP All Import Monitor**.

---

## Configuration

1. **Security Key:** In `WPAllImportMonitor.php`, set `WPAIP_SECURITY_KEY` to match the `WEBHOOK_SECRET` in your GAS handler script properties.

2. **Callback URL:** Locate the `\$script_url` in `WPAllImportMonitor.php` and update it to your deployed GAS callback URL, e.g.:

   ```php
   $script_url = add_query_arg(
     'secret', WPAIP_SECURITY_KEY,
     'https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec'
   );
   ```

3. **Breeze Cache:** Ensure `exec()` and WP-CLI are available on your server for the cache-clearing action.

---

## Usage

* After activation, imports run as usual via WP All Import.
* Import stats are tracked automatically.
* Upon completion, final stats are POSTed to your GAS callback handler and then cleared from options.
* To manually clear Breeze cache, visit:
  `<your-site>/wp-admin/admin-ajax.php?import_key=<your_key>&action=clear_breeze_cache`

---

## Developer Notes

* **Classes:** Consider splitting functionality into `class-import-logger.php` and `class-import-controller.php` for maintainability.
* **Naming:** Ensure plugin folder and main file share the `wpallimport-monitor` slug.
* **Extensibility:** Future actions (start/stop imports) can be added to `wpaip_handle_direct_actions()`.

---

Built for Four12 Global – keeping your imports transparent and under control!
