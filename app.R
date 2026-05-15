# ─────────────────────────────────────────────────────────────────────────────
# Crop Cut Survey – Missing Images QC Dashboard
# ─────────────────────────────────────────────────────────────────────────────
#
# CREDENTIALS: never stored in UI.  Set these environment variables before
# running locally (e.g. in ~/.Renviron or a project-level .Renviron) or in
# your Posit Connect deployment's "Environment Variables" panel:
#
#   DB_HOST     = your-db-host
#   DB_PORT     = 5432          (optional, defaults to 5432)
#   DB_NAME     = your-db-name
#   DB_USER     = your-db-user
#   DB_PASSWORD = your-db-password
#
# Required packages (run once):
#   install.packages(c("shiny","bslib","bsicons","DT","dplyr","DBI","RPostgres",
#                      "jsonlite","tidyr","stringr","httr","purrr","writexl",
#                      "plotly","rlang"))
# ─────────────────────────────────────────────────────────────────────────────

pacman::p_load(shiny, bslib, bsicons, DT, dplyr, DBI, RPostgres, jsonlite,
               tidyr, stringr, httr, purrr, writexl, plotly, rlang)

# ══════════════════════════════════════════════════════════════════════════════
# 1.  DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

make_conn <- function() {
  tryCatch(
    dbConnect(
      RPostgres::Postgres(),
      host     = Sys.getenv("DB_HOST"),
      port     = as.integer(Sys.getenv("DB_PORT", "5432")),
      dbname   = Sys.getenv("DB_NAME"),
      user     = Sys.getenv("DB_USER"),
      password = Sys.getenv("DB_PASSWORD")
    ),
    error = function(e) { message("DB connection failed: ", conditionMessage(e)); NULL }
  )
}

conn_ok <- function(conn) !is.null(conn) && tryCatch(dbIsValid(conn), error = function(e) FALSE)

get_projects <- function(conn) {
  tryCatch(
    dbGetQuery(conn,
               "SELECT DISTINCT project_id AS id, project_name AS name
       FROM   common_cropcut
       WHERE  project_name IS NOT NULL
       ORDER  BY project_name"),
    error = function(e) data.frame(id = integer(), name = character())
  )
}

fetch_image_data <- function(conn, project_ids) {
  ids_str <- paste(as.integer(project_ids), collapse = ",")
  q <- sprintf("
    WITH user_supervisor_data AS (
      SELECT e.user_id,
             e.name            AS enumerator_name,
             e.phone_number    AS enumerator_phone,
             s.name            AS supervisor_name
      FROM   common_fieldstaff e
      LEFT JOIN common_fieldstaff s
             ON e.supervisor_id = s.id AND s.role = 'SUPERVISOR'
      WHERE  e.role = 'ENUMERATOR'
    )
    SELECT
        cc.id              AS cropcut_id,
        cc.boxes_pula_id,
        cc.project_id,
        cc.project_name,
        cc.boxes_farmer_name,
        res.position,
        res.response_id,
        res.responses,
        res.farmer_responses,
        res.submitted_by_id,
        res.start_time,
        res.end_time,
        i.cce_id,
        i.question_id,
        i.s3_url,
        u.enumerator_name,
        u.enumerator_phone,
        u.supervisor_name
    FROM common_questionnaireresponse res
    LEFT JOIN common_cropcut       cc  ON res.cropcut_id  = cc.id
    LEFT JOIN common_surveyimage   i   ON res.response_id = i.response_id
    LEFT JOIN user_supervisor_data u   ON res.submitted_by_id = u.user_id
    WHERE cc.project_id IN (%s)
      AND cc.status != 'REJECTED';
  ", ids_str)
  
  tryCatch(
    dbGetQuery(conn, q),
    error = function(e) { message("Query failed: ", conditionMessage(e)); NULL }
  )
}

# ══════════════════════════════════════════════════════════════════════════════
# 2.  DATA PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

COMMON_COLS <- c(
  "cropcut_id", "boxes_pula_id", "project_name", "boxes_farmer_name",
  "enumerator_name", "enumerator_phone", "supervisor_name",
  "start_time", "end_time"
)

process_data <- function(raw) {
  if (is.null(raw) || nrow(raw) == 0) return(NULL)
  
  colnames(raw) <- make.unique(colnames(raw))
  photocols     <- na.omit(unique(raw$question_id))
  
  # Build wide image-link lookup
  image_links <- raw %>%
    select(cce_id, boxes_pula_id, position, question_id, s3_url) %>%
    distinct() %>%
    pivot_wider(
      names_from  = question_id,
      values_from = s3_url,
      values_fn   = ~ paste(unique(na.omit(.x)), collapse = ", ")
    )
  
  # Parse JSON response columns
  parsed <- raw %>%
    mutate(
      responses        = map(responses,        ~ tryCatch(fromJSON(.x, flatten = TRUE), error = function(e) list())),
      farmer_responses = map(farmer_responses, ~ tryCatch(fromJSON(.x, flatten = TRUE), error = function(e) list()))
    ) %>%
    unnest_wider(responses,        names_repair = "unique") %>%
    unnest_wider(farmer_responses, names_repair = "unique")
  
  Data <- parsed %>%
    select(-any_of(c(photocols, "question_id", "s3_url"))) %>%
    distinct() %>%
    left_join(image_links, by = c("boxes_pula_id", "position")) %>%
    select(any_of(c(COMMON_COLS, "position")), starts_with("q_"))
  
  list(
    full        = Data,
    css         = Data %>% filter(position == 1),
    wet         = Data %>% filter(position == 2),
    dry         = Data %>% filter(position == 3),
    photocols   = photocols,
    image_links = image_links
  )
}

# ══════════════════════════════════════════════════════════════════════════════
# 3.  MISSING IMAGE CHECKS
#     check_missing() is safe: it returns NULL if the photo column doesn't
#     exist in `data`, and skips conditional filtering if the condition column
#     doesn't exist either.
# ══════════════════════════════════════════════════════════════════════════════

#' @param data        A data frame (css / wet / dry)
#' @param photo_col   Name of the expected photo column
#' @param cond_col    (optional) Name of the column that gates this photo
#' @param cond_type   "equal" | "contains" | "in"
#' @param cond_val    Value(s) to match in cond_col
#' @param section     Label for the 'section' column
check_missing <- function(data, photo_col,
                          cond_col  = NULL,
                          cond_type = c("equal", "contains", "in"),
                          cond_val  = NULL,
                          section   = "Unknown") {
  cond_type <- match.arg(cond_type)
  
  # Guard 1: photo column must exist
  if (!photo_col %in% names(data)) return(NULL)
  
  d <- data
  
  # Guard 2: apply condition only when condition column exists
  if (!is.null(cond_col) && cond_col %in% names(d) && !is.null(cond_val)) {
    d <- switch(cond_type,
                "equal"    = d %>% filter(.data[[cond_col]] == cond_val),
                "contains" = d %>% filter(str_detect(.data[[cond_col]], cond_val)),
                "in"       = d %>% filter(.data[[cond_col]] %in% cond_val)
    )
  }
  
  result <- d %>%
    filter(is.na(.data[[photo_col]])) %>%
    select(any_of(c(COMMON_COLS, "position"))) %>%
    mutate(question_id = photo_col, section = section)
  
  if (nrow(result) == 0) NULL else result
}

build_missing <- function(css, wet, dry) {
  loss_conditions <- c("average", "below_average", "no_crop_survived")
  
  all_checks <- list(
    # ── CSS (position = 1) ───────────────────────────────────────────────────
    check_missing(css, "q_farmer_sign",              section = "CSS"),
    check_missing(css, "q_photo_both_boxes",         section = "CSS"),
    check_missing(css, "q_field_photo",              section = "CSS"),
    check_missing(css, "q_crop_closeup_photo",       section = "CSS"),
    check_missing(css, "q_farmer_photo",             section = "CSS"),
    
    check_missing(css, "q_irrigation_type_photo",
                  cond_col = "q_field_irrigated", cond_type = "equal", cond_val = "yes",
                  section = "CSS"),
    
    check_missing(css, "q_weeds_photo",
                  cond_col = "q_problem", cond_type = "contains", cond_val = "weeds",
                  section = "CSS"),
    
    check_missing(css, "q_flood_evidence_photo",
                  cond_col = "q_problem", cond_type = "contains", cond_val = "flood",
                  section = "CSS"),
    
    check_missing(css, "q_pests_or_diseases_evidence_photo",
                  cond_col = "q_problem", cond_type = "contains", cond_val = "pest",
                  section = "CSS"),
    
    check_missing(css, "q_secondary_issue_photo",
                  cond_col = "q_secondary_issue_present", cond_type = "equal", cond_val = "yes",
                  section = "CSS"),
    
    check_missing(css, "q_corner1_total_loss_photo",
                  cond_col = "q_crop_condition", cond_type = "in", cond_val = loss_conditions,
                  section = "CSS"),
    check_missing(css, "q_corner2_total_loss_photo",
                  cond_col = "q_crop_condition", cond_type = "in", cond_val = loss_conditions,
                  section = "CSS"),
    check_missing(css, "q_corner3_total_loss_photo",
                  cond_col = "q_crop_condition", cond_type = "in", cond_val = loss_conditions,
                  section = "CSS"),
    check_missing(css, "q_corner4_total_loss_photo",
                  cond_col = "q_crop_condition", cond_type = "in", cond_val = loss_conditions,
                  section = "CSS"),
    
    # ── Wet Harvest (position = 2) ───────────────────────────────────────────
    check_missing(wet, "q_farmer_sign",       section = "Wet Harvest"),
    check_missing(wet, "q_wet_harvest_photo",
                  cond_col = "q_box2_harvest_possible", cond_type = "equal", cond_val = "yes",
                  section = "Wet Harvest"),
    check_missing(wet, "q_harvest_bags_photo",
                  cond_col = "q_box2_harvest_possible", cond_type = "equal", cond_val = "yes",
                  section = "Wet Harvest"),
    check_missing(wet, "q_attestation_form",
                  cond_col = "q_box2_harvest_possible", cond_type = "equal", cond_val = "yes",
                  section = "Wet Harvest"),
    check_missing(wet, "q_box1_wet_weight_photo",
                  cond_col = "q_box1_harvest_possible", cond_type = "equal", cond_val = "yes",
                  section = "Wet Harvest"),
    check_missing(wet, "q_box2_wet_weight_photo",
                  cond_col = "q_box2_harvest_possible", cond_type = "equal", cond_val = "yes",
                  section = "Wet Harvest"),
    
    check_missing(wet, "q_box1_corner1_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box1_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    check_missing(wet, "q_box1_corner2_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box1_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    check_missing(wet, "q_box1_corner3_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box1_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    check_missing(wet, "q_box1_corner4_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box1_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    
    check_missing(wet, "q_box2_corner1_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box2_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    check_missing(wet, "q_box2_corner2_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box2_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    check_missing(wet, "q_box2_corner3_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box2_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    check_missing(wet, "q_box2_corner4_total_loss_photo",
                  cond_col = "q_why_unable_to_capture_box2_weight",
                  cond_type = "in", cond_val = loss_conditions, section = "Wet Harvest"),
    
    # ── Dry Harvest (position = 3) ───────────────────────────────────────────
    check_missing(dry, "q_farmer_sign",       section = "Dry Harvest"),
    check_missing(dry, "q_attestation_form",
                  cond_col = "q_capture_wet_weight_1", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box1_unthreshed_photo",
                  cond_col = "q_capture_wet_weight_1", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box1_dry_weight_photo_unthreshed",
                  cond_col = "q_capture_wet_weight_1", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box1_threshed_photo",
                  cond_col = "q_capture_wet_weight_1", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box1_dry_weight_photo_threshed",
                  cond_col = "q_capture_wet_weight_1", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box2_unthreshed_photo",
                  cond_col = "q_capture_wet_weight_2", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box2_dry_weight_photo_unthreshed",
                  cond_col = "q_capture_wet_weight_2", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box2_threshed_photo",
                  cond_col = "q_capture_wet_weight_2", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest"),
    check_missing(dry, "q_box2_dry_weight_photo_threshed",
                  cond_col = "q_capture_wet_weight_2", cond_type = "equal", cond_val = "yes",
                  section = "Dry Harvest")
  )
  
  result <- bind_rows(Filter(Negate(is.null), all_checks)) %>%
    mutate(`Days Since Submission` = as.numeric(Sys.Date() - as.Date(end_time))) %>%
    filter(`Days Since Submission` >= 0) %>%
    distinct()
  
  result
}

# ══════════════════════════════════════════════════════════════════════════════
# 4.  URL VALIDATION  –  Authenticated AWS S3
#
# AWS credentials are read from environment variables (never from the UI):
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   AWS_DEFAULT_REGION      (optional – overridden by region parsed from URL)
#   AWS_SESSION_TOKEN       (optional – for temporary/assumed-role credentials)
#
# Supported URL format (path-style and virtual-hosted):
#   https://<bucket>.s3[-.]<region>.amazonaws.com/<key>
#   https://s3[.-]<region>.amazonaws.com/<bucket>/<key>
#
# Returned status values:
#   "ok"                  – object exists and is accessible
#   "http_403"            – credentials rejected / missing bucket permission
#   "http_404"            – object key does not exist in the bucket
#   "http_<N>"            – any other HTTP error from the S3 API
#   "invalid_s3_format"   – URL does not match any recognised S3 pattern
#   "unreachable"         – network / DNS error before an HTTP response arrived
# ══════════════════════════════════════════════════════════════════════════════

S3_BUCKET <- "mavuno-files"   # canonical bucket name for validation

#' Parse an S3 URL into a list(bucket, key, region) or NULL
parse_s3_url <- function(url) {
  url <- trimws(url)
  if (is.na(url) || !nzchar(url)) return(NULL)
  
  # Pattern 1 – virtual-hosted style:
  #   https://<bucket>.s3[-.]<region>.amazonaws.com/<key>
  m1 <- regmatches(url,
                   regexec(
                     "^https?://([^.]+)\\.s3[.-]([^.]+)\\.amazonaws\\.com/(.+)$",
                     url, perl = TRUE
                   ))[[1]]
  
  if (length(m1) == 4) {
    return(list(bucket = m1[2], region = m1[3], key = m1[4]))
  }
  
  # Pattern 2 – path style:
  #   https://s3[.-]<region>.amazonaws.com/<bucket>/<key>
  m2 <- regmatches(url,
                   regexec(
                     "^https?://s3[.-]([^.]+)\\.amazonaws\\.com/([^/]+)/(.+)$",
                     url, perl = TRUE
                   ))[[1]]
  
  if (length(m2) == 4) {
    return(list(bucket = m2[3], region = m2[2], key = m2[4]))
  }
  
  # Pattern 3 – bucket-only virtual-hosted (no explicit region):
  #   https://<bucket>.s3.amazonaws.com/<key>
  m3 <- regmatches(url,
                   regexec(
                     "^https?://([^.]+)\\.s3\\.amazonaws\\.com/(.+)$",
                     url, perl = TRUE
                   ))[[1]]
  
  if (length(m3) == 3) {
    return(list(
      bucket = m3[2],
      region = Sys.getenv("AWS_DEFAULT_REGION", "eu-west-1"),
      key    = m3[3]
    ))
  }
  
  NULL   # no pattern matched
}

#' Check one S3 URL using the AWS SDK (authenticated HEAD request)
check_s3_url <- function(url) {
  if (is.na(url) || !nzchar(trimws(url))) return("no_url")
  
  parsed <- parse_s3_url(url)
  if (is.null(parsed)) return("invalid_s3_format")
  
  tryCatch({
    # aws.s3::head_object() sends an authenticated HEAD request.
    # It picks up AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
    # AWS_SESSION_TOKEN automatically from the environment.
    result <- aws.s3::head_object(
      object = parsed$key,
      bucket = parsed$bucket,
      region = parsed$region
    )
    # head_object returns TRUE (with attributes) on success
    if (isTRUE(result) || is.list(result)) "ok" else "http_404"
    
  }, error = function(e) {
    msg <- conditionMessage(e)
    
    # aws.s3 embeds the HTTP status in the error message
    if      (grepl("404", msg, fixed = TRUE)) "http_404"
    else if (grepl("403", msg, fixed = TRUE)) "http_403"
    else if (grepl("401", msg, fixed = TRUE)) "http_403"
    else {
      # Extract any other HTTP status code from the message
      code <- regmatches(msg, regexpr("[45][0-9]{2}", msg))
      if (length(code) == 1 && nzchar(code)) paste0("http_", code)
      else "unreachable"
    }
  })
}

#' Flatten image_links into one row per URL (handles comma-separated multi-URLs)
run_url_validation <- function(image_links) {
  meta_cols <- c("cce_id", "boxes_pula_id", "position")
  url_cols  <- setdiff(names(image_links), meta_cols)
  
  image_links %>%
    pivot_longer(cols = all_of(url_cols), names_to = "question_id", values_to = "url") %>%
    filter(!is.na(url), nzchar(url)) %>%
    mutate(url = strsplit(url, ",\\s*")) %>%
    unnest(url) %>%
    mutate(url = trimws(url)) %>%
    filter(!is.na(url), nzchar(url)) %>%
    # Validate bucket name matches expected bucket
    mutate(
      parsed_bucket = map_chr(url, ~ {
        p <- parse_s3_url(.x)
        if (is.null(p)) NA_character_ else p$bucket
      }),
      bucket_mismatch = !is.na(parsed_bucket) & parsed_bucket != S3_BUCKET
    ) %>%
    distinct()
}

# ══════════════════════════════════════════════════════════════════════════════
# 5.  UI
# ══════════════════════════════════════════════════════════════════════════════

ui <- page_navbar(
  title = tags$span(
    tags$img(src = NULL, height = 28),
    "🌾 Crop Cut – Image QC Dashboard"
  ),
  theme = bs_theme(
    bootswatch = "flatly",
    primary    = "#2E7D32",
    "navbar-bg" = "#1B5E20"
  ),
  bg = "#1B5E20",
  
  sidebar = sidebar(
    width = 290,
    open  = "always",
    
    tags$small(class = "text-muted",
               "Credentials are loaded from environment variables — never entered here.",
               tags$br(),
               tags$code("DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
    ),
    
    hr(),
    
    actionButton("connect_btn", "🔌  Connect / Refresh Projects",
                 class = "btn-success btn-sm w-100"),
    br(),
    uiOutput("conn_status_ui"),
    
    hr(),
    
    uiOutput("project_ui"),
    numericInput("days_threshold", "Highlight if outstanding ≥ (days)",
                 value = 3, min = 1, step = 1, width = "100%"),
    actionButton("load_btn", "📥  Load Data",
                 class = "btn-primary w-100"),
    
    hr(),
    uiOutput("last_updated_ui")
  ),
  
  # ── Overview ────────────────────────────────────────────────────────────────
  nav_panel(
    title = "📊 Overview",
    uiOutput("kpi_row"),
    br(),
    layout_columns(
      col_widths = c(6, 6),
      card(
        card_header("Missing Images by Survey Section"),
        plotlyOutput("chart_section", height = "260px")
      ),
      card(
        card_header("Top 10 Questions with Most Missing Images"),
        plotlyOutput("chart_question", height = "260px")
      )
    ),
    layout_columns(
      col_widths = c(6, 6),
      card(
        card_header("Top 10 Enumerators with Missing Images"),
        plotlyOutput("chart_enum", height = "260px")
      ),
      card(
        card_header("Missing Images by Submission Date"),
        plotlyOutput("chart_timeline", height = "260px")
      )
    )
  ),
  
  # ── Missing Images ──────────────────────────────────────────────────────────
  nav_panel(
    title = "🖼️ Missing Images",
    card(
      card_header(
        div(class = "d-flex justify-content-between align-items-center",
            span("Missing Images Detail"),
            downloadButton("dl_missing", "⬇️ Excel", class = "btn-sm btn-outline-success")
        )
      ),
      layout_columns(
        col_widths = c(4, 4, 4),
        uiOutput("filter_section_ui"),
        uiOutput("filter_supervisor_ui"),
        uiOutput("filter_enumerator_ui")
      ),
      DTOutput("tbl_missing")
    )
  ),
  
  # ── URL Validation ──────────────────────────────────────────────────────────
  nav_panel(
    title = "🔗 URL Check",
    card(
      card_header("Image URL Accessibility Check"),
      p("Tests every submitted image URL with an HTTP HEAD request.",
        "Large datasets may take several minutes."),
      actionButton("validate_btn", "🚀 Start Validation", class = "btn-warning"),
      hr(),
      uiOutput("url_kpi_row"),
      br(),
      DTOutput("tbl_urls"),
      br(),
      downloadButton("dl_urls", "⬇️ Download Report", class = "btn-outline-secondary btn-sm")
    )
  ),
  
  # ── By Enumerator ───────────────────────────────────────────────────────────
  nav_panel(
    title = "👥 Enumerator Summary",
    card(
      card_header(
        div(class = "d-flex justify-content-between align-items-center",
            span("Per-Enumerator / Supervisor Breakdown"),
            downloadButton("dl_enum", "⬇️ Excel", class = "btn-sm btn-outline-success")
        )
      ),
      DTOutput("tbl_enum")
    )
  ),
  
  # ── By Project ──────────────────────────────────────────────────────────────
  nav_panel(
    title = "📁 By Project",
    card(
      card_header("Missing Images per Project"),
      DTOutput("tbl_project")
    )
  )
)

# ══════════════════════════════════════════════════════════════════════════════
# 6.  SERVER
# ══════════════════════════════════════════════════════════════════════════════

server <- function(input, output, session) {
  
  rv <- reactiveValues(
    conn         = NULL,
    projects     = NULL,
    processed    = NULL,
    missing      = NULL,
    url_df       = NULL,    # URL rows awaiting validation
    url_results  = NULL,    # validated URL results
    last_updated = NULL
  )
  
  # ── Connect ──────────────────────────────────────────────────────────────────
  observeEvent(input$connect_btn, {
    if (conn_ok(rv$conn)) try(dbDisconnect(rv$conn), silent = TRUE)
    rv$conn <- make_conn()
    if (conn_ok(rv$conn)) {
      rv$projects <- get_projects(rv$conn)
      showNotification("✅ Connected to database", type = "message")
    } else {
      showNotification("❌ Connection failed – check DB_* environment variables",
                       type = "error", duration = 10)
    }
  })
  
  output$conn_status_ui <- renderUI({
    if (conn_ok(rv$conn)) {
      tags$div(class = "alert alert-success p-1 small mb-0 mt-1", "✅ Connected")
    } else {
      tags$div(class = "alert alert-danger p-1 small mb-0 mt-1", "❌ Not connected")
    }
  })
  
  output$project_ui <- renderUI({
    req(rv$projects)
    choices <- setNames(rv$projects$id, paste0(rv$projects$id, '-', rv$projects$name))
    selectizeInput("project_ids", "Project(s)",
                   choices  = choices,
                   multiple = TRUE,
                   selected = choices[[1]],
                   options  = list(placeholder = "Select one or more projects…"))
  })
  
  output$last_updated_ui <- renderUI({
    req(rv$last_updated)
    tags$small(class = "text-muted",
               "Last loaded: ", format(rv$last_updated, "%Y-%m-%d %H:%M"))
  })
  
  # ── Load data ─────────────────────────────────────────────────────────────
  observeEvent(input$load_btn, {
    req(conn_ok(rv$conn), input$project_ids)
    
    id <- showNotification("⏳ Fetching data from database…", duration = NULL)
    on.exit(removeNotification(id))
    
    raw <- fetch_image_data(rv$conn, input$project_ids)
    
    if (is.null(raw) || nrow(raw) == 0) {
      showNotification("⚠️ No data found for the selected project(s).", type = "warning")
      return()
    }
    
    updateNotification <- function(...) {}   # suppress noise
    rv$processed    <- process_data(raw)
    rv$missing      <- build_missing(rv$processed$css, rv$processed$wet, rv$processed$dry)
    rv$url_df       <- run_url_validation(rv$processed$image_links)
    rv$url_results  <- NULL
    rv$last_updated <- Sys.time()
    
    showNotification(
      sprintf("✅ Loaded %d rows across %d project(s).",
              nrow(raw), length(input$project_ids)),
      type = "message"
    )
  })
  
  # ── KPI cards ─────────────────────────────────────────────────────────────
  output$kpi_row <- renderUI({
    req(rv$processed, rv$missing)
    p <- rv$processed
    m <- rv$missing
    
    total_cuts    <- n_distinct(p$full$cropcut_id, na.rm = TRUE)
    total_missing <- nrow(m)
    affected      <- n_distinct(m$boxes_pula_id,   na.rm = TRUE)
    n_enum_issues <- n_distinct(m$enumerator_name, na.rm = TRUE)
    avg_days      <- round(mean(m$`Days Since Submission`, na.rm = TRUE), 1)
    css_miss      <- nrow(m %>% filter(section == "CSS"))
    wet_miss      <- nrow(m %>% filter(section == "Wet Harvest"))
    dry_miss      <- nrow(m %>% filter(section == "Dry Harvest"))
    
    layout_columns(
      col_widths = c(3, 3, 2, 2, 2),
      value_box("Total Cropcuts",     total_cuts,    showcase = bs_icon("clipboard-data"),  theme = "success"),
      value_box("Missing Images",     total_missing, showcase = bs_icon("images"),         theme = "danger"),
      value_box("Affected Farms",     affected,      showcase = bs_icon("house-exclamation"),theme = "warning"),
      value_box("Enumerators Affected", n_enum_issues, showcase = bs_icon("person-x"),     theme = "secondary"),
      value_box("Avg Days Outstanding", avg_days,    showcase = bs_icon("clock-history"),   theme = "info")
    )
  })
  
  # ── Charts ────────────────────────────────────────────────────────────────
  plt_colors <- c(CSS = "#2E7D32", `Wet Harvest` = "#1565C0", `Dry Harvest` = "#E65100")
  
  output$chart_section <- renderPlotly({
    req(rv$missing)
    d <- rv$missing %>% count(section)
    plot_ly(d, x = ~section, y = ~n, color = ~section,
            colors = unname(plt_colors[d$section]),
            type = "bar") %>%
      layout(showlegend = FALSE,
             xaxis = list(title = ""),
             yaxis = list(title = "Count"),
             margin = list(t = 10))
  })
  
  output$chart_question <- renderPlotly({
    req(rv$missing)
    d <- rv$missing %>% count(question_id, sort = TRUE) %>% head(10)
    plot_ly(d, x = ~n, y = ~reorder(question_id, n),
            type = "bar", orientation = "h",
            marker = list(color = "#1565C0")) %>%
      layout(xaxis = list(title = "Count"),
             yaxis = list(title = ""),
             margin = list(l = 220, t = 10))
  })
  
  output$chart_enum <- renderPlotly({
    req(rv$missing)
    d <- rv$missing %>%
      filter(!is.na(enumerator_name)) %>%
      count(enumerator_name, sort = TRUE) %>%
      head(10)
    plot_ly(d, x = ~n, y = ~reorder(enumerator_name, n),
            type = "bar", orientation = "h",
            marker = list(color = "#E65100")) %>%
      layout(xaxis = list(title = "Missing Images"),
             yaxis = list(title = ""),
             margin = list(l = 160, t = 10))
  })
  
  output$chart_timeline <- renderPlotly({
    req(rv$missing)
    d <- rv$missing %>%
      mutate(date = as.Date(end_time)) %>%
      filter(!is.na(date)) %>%
      count(date)
    plot_ly(d, x = ~date, y = ~n, type = "scatter", mode = "lines+markers",
            line = list(color = "#2E7D32"),
            marker = list(color = "#2E7D32")) %>%
      layout(xaxis = list(title = "Submission Date"),
             yaxis = list(title = "Count"),
             margin = list(t = 10))
  })
  
  # ── Missing images table – filters ────────────────────────────────────────
  output$filter_section_ui <- renderUI({
    req(rv$missing)
    selectInput("filter_section", "Section",
                choices = c("All", sort(unique(rv$missing$section))),
                selected = "All")
  })
  
  output$filter_supervisor_ui <- renderUI({
    req(rv$missing)
    sups <- c("All", sort(na.omit(unique(rv$missing$supervisor_name))))
    selectInput("filter_supervisor", "Supervisor", choices = sups, selected = "All")
  })
  
  output$filter_enumerator_ui <- renderUI({
    req(rv$missing)
    enums <- c("All", sort(na.omit(unique(rv$missing$enumerator_name))))
    selectInput("filter_enumerator", "Enumerator", choices = enums, selected = "All")
  })
  
  missing_filtered <- reactive({
    req(rv$missing)
    d <- rv$missing
    if (!is.null(input$filter_section)    && input$filter_section    != "All")
      d <- d %>% filter(section          == input$filter_section)
    if (!is.null(input$filter_supervisor) && input$filter_supervisor != "All")
      d <- d %>% filter(supervisor_name  == input$filter_supervisor)
    if (!is.null(input$filter_enumerator) && input$filter_enumerator != "All")
      d <- d %>% filter(enumerator_name  == input$filter_enumerator)
    d
  })
  
  output$tbl_missing <- renderDT({
    req(missing_filtered())
    thresh <- as.numeric(input$days_threshold %||% 3)
    display <- missing_filtered() %>%
      select(-any_of("position")) %>%
      arrange(desc(`Days Since Submission`))
    
    datatable(
      display,
      rownames   = FALSE,
      extensions = "Buttons",
      options    = list(
        pageLength = 25,
        scrollX    = TRUE,
        dom        = "Bfrtip",
        buttons    = c("copy", "csv")
      ),
      class = "table-striped table-hover table-sm"
    ) %>%
      formatStyle(
        "Days Since Submission",
        backgroundColor = styleInterval(
          c(thresh - 0.01, thresh * 2),
          c("#c8e6c9", "#fff9c4", "#ffcdd2")
        )
      )
  })
  
  output$dl_missing <- downloadHandler(
    filename = function() paste0("missing_images_", Sys.Date(), ".xlsx"),
    content  = function(file) write_xlsx(missing_filtered(), file)
  )
  
  # ── URL validation (S3 authenticated) ────────────────────────────────────
  observeEvent(input$validate_btn, {
    req(rv$url_df)
    
    # Sanity-check AWS credentials are present before starting
    if (!nzchar(Sys.getenv("AWS_ACCESS_KEY_ID")) ||
        !nzchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))) {
      showModal(modalDialog(
        title = "⚠️ AWS Credentials Missing",
        tags$p("The following environment variables must be set:"),
        tags$ul(
          tags$li(tags$code("AWS_ACCESS_KEY_ID")),
          tags$li(tags$code("AWS_SECRET_ACCESS_KEY")),
          tags$li(tags$code("AWS_DEFAULT_REGION"), " (optional, defaults to eu-west-1)"),
          tags$li(tags$code("AWS_SESSION_TOKEN"),  " (optional, for temporary credentials)")
        ),
        tags$p("Set them in your ", tags$code(".Renviron"), " locally, or in the",
               " Posit Connect deployment's Environment Variables panel."),
        easyClose = TRUE,
        footer    = modalButton("Close")
      ))
      return()
    }
    
    df <- rv$url_df
    n  <- nrow(df)
    
    if (n == 0) {
      showNotification("No URLs found to validate.", type = "warning")
      return()
    }
    
    statuses <- character(n)
    
    withProgress(message = "Checking S3 objects…", value = 0, {
      for (i in seq_len(n)) {
        incProgress(
          1 / n,
          detail = sprintf("[%d/%d] %s – %s",
                           i, n, df$question_id[i],
                           basename(df$url[i]))
        )
        statuses[i] <- check_s3_url(df$url[i])
      }
    })
    
    # Build status-to-meaning lookup for user-friendly display
    status_labels <- c(
      ok                  = "✅ Accessible",
      http_403            = "🔒 http_403 – Access Denied",
      http_404            = "❌ http_404 – Object Not Found",
      invalid_s3_format   = "⚠️ invalid_s3_format – URL Not Recognised",
      unreachable         = "🌐 unreachable – Network Error",
      no_url              = "— no_url"
    )
    
    rv$url_results <- df %>%
      mutate(
        status      = statuses,
        status_label = dplyr::recode(status, !!!status_labels, .default = paste0("❓ ", status)),
        accessible  = status == "ok"
      ) %>%
      # Flag records where URL bucket doesn't match expected bucket
      mutate(
        bucket_ok = !bucket_mismatch | is.na(bucket_mismatch)
      ) %>%
      select(boxes_pula_id, position, question_id,
             url, parsed_bucket, bucket_ok,
             status, status_label, accessible)
    
    n_ok     <- sum(rv$url_results$status == "ok")
    n_403    <- sum(rv$url_results$status == "http_403")
    n_404    <- sum(rv$url_results$status == "http_404")
    n_other  <- n - n_ok - n_403 - n_404
    
    showNotification(
      sprintf("✅ Done: %d ok  |  %d not found (404)  |  %d access denied (403)  |  %d other",
              n_ok, n_404, n_403, n_other),
      type     = "message",
      duration = 10
    )
  })
  
  output$url_kpi_row <- renderUI({
    req(rv$url_results)
    d        <- rv$url_results
    total    <- nrow(d)
    n_ok     <- sum(d$status == "ok")
    n_403    <- sum(d$status == "http_403")
    n_404    <- sum(d$status == "http_404")
    n_format <- sum(d$status == "invalid_s3_format")
    n_reach  <- sum(d$status == "unreachable")
    pct_ok   <- round(100 * n_ok / max(total, 1), 1)
    
    layout_columns(
      col_widths = c(2, 2, 2, 2, 2, 2),
      value_box("Checked",           total,                   showcase = bs_icon("link-45deg"),      theme = "secondary"),
      value_box("Accessible",        paste0(n_ok, " (", pct_ok, "%)"), showcase = bs_icon("check-circle-fill"), theme = "success"),
      value_box("Not Found (404)",   n_404,                   showcase = bs_icon("file-x"),          theme = "danger"),
      value_box("Access Denied (403)", n_403,                 showcase = bs_icon("lock-fill"),       theme = "warning"),
      value_box("Bad URL Format",    n_format,                showcase = bs_icon("exclamation-triangle"), theme = "info"),
      value_box("Unreachable",       n_reach,                 showcase = bs_icon("wifi-off"),        theme = "dark")
    )
  })
  
  output$tbl_urls <- renderDT({
    req(rv$url_results)
    
    # Colour palette covering all expected status values
    status_colors <- c(
      "ok"                = "#c8e6c9",
      "http_403"          = "#ffe0b2",
      "http_404"          = "#ffcdd2",
      "invalid_s3_format" = "#e1bee7",
      "unreachable"       = "#cfd8dc",
      "no_url"            = "#f5f5f5"
    )
    known_statuses <- names(status_colors)
    known_bg       <- unname(status_colors)
    
    display <- rv$url_results %>%
      select(boxes_pula_id, position, question_id,
             bucket_ok, status, status_label, accessible, url)
    
    datatable(
      display,
      rownames  = FALSE,
      colnames  = c("Farm ID", "Position", "Question", "Bucket OK",
                    "Status Code", "Status Detail", "Accessible", "URL"),
      extensions = "Buttons",
      options   = list(
        pageLength = 25,
        scrollX   = TRUE,
        dom       = "Bfrtip",
        buttons   = c("copy", "csv"),
        columnDefs = list(
          list(targets = 7, render = JS(
            "function(data){ return data ?
               '<a href=\"' + data + '\" target=\"_blank\">🔗 view</a>' : '—'; }"
          ))
        )
      ),
      escape = FALSE,
      class  = "table-sm table-striped table-hover"
    ) %>%
      formatStyle(
        "status",
        backgroundColor = styleEqual(known_statuses, known_bg)
      ) %>%
      formatStyle(
        "accessible",
        color      = styleEqual(c(TRUE, FALSE), c("#1b5e20", "#b71c1c")),
        fontWeight = "bold"
      ) %>%
      formatStyle(
        "bucket_ok",
        backgroundColor = styleEqual(c(TRUE, FALSE), c("transparent", "#fff9c4"))
      )
  })
  
  output$dl_urls <- downloadHandler(
    filename = function() paste0("s3_url_validation_", Sys.Date(), ".xlsx"),
    content  = function(file) {
      req(rv$url_results)
      write_xlsx(rv$url_results %>% select(-status_label), file)
    }
  )
  
  # ── Enumerator summary ────────────────────────────────────────────────────
  enum_summary <- reactive({
    req(rv$missing)
    rv$missing %>%
      group_by(project_name, supervisor_name, enumerator_name, enumerator_phone) %>%
      summarise(
        `Total Missing`         = n(),
        `Distinct Farms`        = n_distinct(boxes_pula_id, na.rm = TRUE),
        `CSS Missing`           = sum(section == "CSS"),
        `Wet Harvest Missing`   = sum(section == "Wet Harvest"),
        `Dry Harvest Missing`   = sum(section == "Dry Harvest"),
        `Avg Days Outstanding`  = round(mean(`Days Since Submission`, na.rm = TRUE), 1),
        `Max Days Outstanding`  = max(`Days Since Submission`, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(desc(`Total Missing`))
  })
  
  output$tbl_enum <- renderDT({
    req(enum_summary())
    datatable(
      enum_summary(),
      rownames  = FALSE,
      options   = list(pageLength = 25, scrollX = TRUE),
      class     = "table-sm table-striped table-hover"
    ) %>%
      formatStyle(
        "Total Missing",
        background        = styleColorBar(range(enum_summary()$`Total Missing`), "#ffcdd2"),
        backgroundSize    = "100% 90%",
        backgroundRepeat  = "no-repeat",
        backgroundPosition = "center"
      ) %>%
      formatStyle(
        "Max Days Outstanding",
        color = styleInterval(c(7, 14), c("black", "#e65100", "#b71c1c"))
      )
  })
  
  output$dl_enum <- downloadHandler(
    filename = function() paste0("enumerator_summary_", Sys.Date(), ".xlsx"),
    content  = function(file) { req(enum_summary()); write_xlsx(enum_summary(), file) }
  )
  
  # ── By Project summary ────────────────────────────────────────────────────
  output$tbl_project <- renderDT({
    req(rv$missing, rv$processed)
    total_per_proj <- rv$processed$full %>%
      group_by(project_name) %>%
      summarise(`Total Cropcuts` = n_distinct(cropcut_id, na.rm = TRUE), .groups = "drop")
    
    proj_summary <- rv$missing %>%
      group_by(project_name) %>%
      summarise(
        `Total Missing`        = n(),
        `Affected Farms`       = n_distinct(boxes_pula_id, na.rm = TRUE),
        `CSS Missing`          = sum(section == "CSS"),
        `Wet Harvest Missing`  = sum(section == "Wet Harvest"),
        `Dry Harvest Missing`  = sum(section == "Dry Harvest"),
        `Avg Days Outstanding` = round(mean(`Days Since Submission`, na.rm = TRUE), 1),
        .groups = "drop"
      ) %>%
      left_join(total_per_proj, by = "project_name") %>%
      mutate(`Missing per Cropcut` = round(`Total Missing` / `Total Cropcuts`, 2)) %>%
      arrange(desc(`Total Missing`))
    
    datatable(
      proj_summary,
      rownames = FALSE,
      options  = list(pageLength = 25, scrollX = TRUE),
      class    = "table-sm table-striped table-hover"
    )
  })
  
  # ── Cleanup on session end ────────────────────────────────────────────────
  onSessionEnded(function() {
    if (conn_ok(isolate(rv$conn))) try(dbDisconnect(isolate(rv$conn)), silent = TRUE)
  })
}

shinyApp(ui, server)