# Load required libraries
library(shiny)
library(shinydashboard)
library(data.table)
library(DT)
library(readxl)
library(tools)
library(esquisse)
library(dplyr)
library(DBI)
library(odbc)
library(RMySQL)
library(RPostgres)
library(RSQLite)
library(future)
library(promises)
library(memoise)
library(pryr)
library(bit64)
library(arrow)
library(dtplyr)
library(prophet)  # Add prophet library
library(plotly)   # Add plotly library
library(rpivotTable) # Add rpivotTable library
library(shinyjs)  # Add shinyjs library for runjs function

# Configure parallel processing
future::plan(multicore)
options(future.globals.maxSize = 8000 * 1024^2)  # 8GB limit for future
options(datatable.print.class = TRUE)
options(datatable.optimize = TRUE)

# Helper functions for large data handling
chunk_read_csv <- function(file_path, chunk_size = 100000) {
  con <- file(file_path, "r")
  header <- read.csv(con, nrows = 1, header = TRUE)
  
  chunks <- list()
  while (TRUE) {
    chunk <- tryCatch(
      read.csv(con, nrows = chunk_size, header = FALSE, col.names = names(header)),
      error = function(e) NULL
    )
    if (is.null(chunk) || nrow(chunk) == 0) break
    chunks[[length(chunks) + 1]] <- as.data.table(chunk)
    gc()  # Force garbage collection after each chunk
  }
  close(con)
  rbindlist(chunks)
}

sample_large_dataset <- function(dt, n = 1000) {
  if (nrow(dt) <= n) return(dt)
  dt[sample(nrow(dt), n)]
}

get_memory_usage <- function() {
  mem <- pryr::mem_used()
  paste0("Memory Usage: ", round(mem/1024/1024, 2), " MB")
}

# Cache function for database queries
cached_query <- memoise(function(conn, query) {
  dbGetQuery(conn, query)
})

# Add safe Excel reading function
safe_read_excel <- function(file_path) {
  tryCatch({
    # Read with minimal options
    data <- read_excel(
      file_path,
      sheet = 1,
      col_names = TRUE,
      na = ""
    )
    
    # Convert to data.table if successful
    if(!is.null(data) && nrow(data) > 0 && ncol(data) > 0) {
      return(as.data.table(data))
    }
    return(NULL)
  }, error = function(e) {
    return(NULL)
  })
}

# Compress large datasets
compress_dataset <- function(dt) {
  for (col in names(dt)) {
    if (is.character(dt[[col]])) {
      dt[, (col) := as.factor(get(col))]
    } else if (is.numeric(dt[[col]])) {
      if (all(floor(dt[[col]]) == dt[[col]], na.rm = TRUE)) {
        if (max(dt[[col]], na.rm = TRUE) <= .Machine$integer.max) {
          dt[, (col) := as.integer(get(col))]
        }
      }
    }
  }
  gc()
  return(dt)
}

# Batch processing for data operations
batch_process <- function(dt, fn, batch_size = 50000) {
  total_rows <- nrow(dt)
  batches <- ceiling(total_rows / batch_size)
  result <- vector("list", batches)
  
  withProgress(message = 'Processing data', value = 0, {
    for(i in 1:batches) {
      start_idx <- (i-1) * batch_size + 1
      end_idx <- min(i * batch_size, total_rows)
      batch <- dt[start_idx:end_idx]
      result[[i]] <- fn(batch)
      incProgress(i/batches)
      gc()
    }
  })
  
  rbindlist(result, fill = TRUE)
}

# Optimized data loading function
optimized_read_file <- function(file_path) {
  ext <- tolower(file_ext(file_path))
  withProgress(message = 'Reading file', value = 0, {
    tryCatch({
      if (ext == "csv") {
        # Use Arrow for CSV files
        incProgress(0.3, detail = "Loading data with Arrow...")
        data <- arrow::read_csv_arrow(file_path) %>%
          as.data.table()
        incProgress(0.7)
        return(data)
      } else if (ext %in% c("xls", "xlsx")) {
        # Use readxl for Excel files
        incProgress(0.3, detail = "Loading data with readxl...")
        data <- read_excel(file_path) %>%
          as.data.table()
        incProgress(0.7)
        return(data)
      } else {
        showNotification(paste("Unsupported file type:", ext), type = "error")
        return(NULL)
      }
    }, error = function(e) {
      showNotification(paste("Error reading file:", e$message), type = "error")
      return(NULL)
    })
  })
}

# Memory-efficient data manipulation
safe_merge <- function(dt1, dt2, by.x, by.y, type = "inner") {
  gc()  # Force garbage collection before merge
  result <- tryCatch({
    merge(dt1, dt2, by.x = by.x, by.y = by.y, 
          all.x = type %in% c("left", "full"),
          all.y = type %in% c("right", "full"))
  }, error = function(e) {
    showNotification("Memory limit reached during merge. Try reducing data size.", type = "error")
    return(NULL)
  })
  gc()  # Force garbage collection after merge
  result
}

# UI Definition
ui <- dashboardPage(
  dashboardHeader(
    title = "Advanced Data Dashboard",
    tags$li(class = "dropdown",
            tags$a(id = "memory_usage",
                   style = "padding: 15px; color: white;"))
  ),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Data Sources", tabName = "sources", icon = icon("database"),
               menuSubItem("File Upload", tabName = "file", icon = icon("file")),
               menuSubItem("Database", tabName = "db", icon = icon("server"))
      ),
      menuItem("Data Management", tabName = "data", icon = icon("table")),
      menuItem("Data Manipulation", tabName = "manipulation", icon = icon("tools")),
      menuItem("Visualization", tabName = "viz", icon = icon("chart-bar")),
      menuItem("Pivot Table", tabName = "pivot", icon = icon("table")), # Add Pivot Table menu item
      menuItem("Forecasting", tabName = "forecasting", icon = icon("chart-line"))  # Add Forecasting menu item
    )
  ),
  dashboardBody(
    shinyjs::useShinyjs(),  # Enable shinyjs functionality
    tabItems(
      # Add this new Database Tab
      tabItem(tabName = "db",
              fluidRow(
                box(width = 12,
                    title = "Database Connection",
                    status = "primary",
                    solidHeader = TRUE,
                    selectInput("db_type", "Database Type",
                                choices = c("MySQL", "PostgreSQL", "SQLite", 
                                            "SQL Server", "Azure SQL", "Other (ODBC)")),
                    conditionalPanel(
                      condition = "input.db_type !== 'SQLite'",
                      textInput("db_host", "Host", value = "localhost"),
                      numericInput("db_port", "Port", value = 3306),
                      textInput("db_user", "Username"),
                      passwordInput("db_pass", "Password")
                    ),
                    conditionalPanel(
                      condition = "input.db_type === 'SQLite'",
                      fileInput("sqlite_file", "Select SQLite Database File",
                                accept = c(".sqlite", ".db", ".sqlite3"))
                    ),
                    conditionalPanel(
                      condition = "input.db_type !== 'SQLite'",
                      textInput("db_name", "Database Name")
                    ),
                    conditionalPanel(
                      condition = "input.db_type === 'SQL Server' || input.db_type === 'Azure SQL'",
                      textInput("db_driver", "ODBC Driver", 
                                value = "ODBC Driver 17 for SQL Server")
                    ),
                    conditionalPanel(
                      condition = "input.db_type === 'Other (ODBC)'",
                      textInput("db_dsn", "Data Source Name (DSN)"),
                      textInput("db_driver", "Custom Driver Name")
                    ),
                    actionButton("db_connect", "Connect", 
                                 icon = icon("plug"), 
                                 class = "btn-primary"),
                    tags$hr(),
                    uiOutput("db_table_ui")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Database Tables",
                    status = "info",
                    solidHeader = TRUE,
                    DTOutput("db_tables_preview")
                )
              )
      ),
      # File Upload Tab
      tabItem(tabName = "file",
              fluidRow(
                box(width = 12,
                    fileInput("files", "Upload Data Files", 
                              multiple = TRUE,
                              accept = c(".csv", ".xlsx", ".xls")),
                    fileInput("folder", "Select Folder Containing Data Files", 
                              multiple = TRUE,
                              accept = c(".csv", ".xlsx", ".xls")),
                    numericInput("page_size", "Rows per page:", 
                                 value = 500, min = 100, max = 1000)
                )
              )
      ),
      
      # Data Management Tab
      tabItem(tabName = "data",
              fluidRow(
                box(width = 12,
                    uiOutput("table_tabs")
                )
              )
      ),
      
      # Data Manipulation Tab
      tabItem(tabName = "manipulation",
              fluidRow(
                box(width = 6,
                    selectInput("manipulation_type", "Select Operation",
                                choices = c("Merge Tables" = "merge",
                                            "Append Tables" = "append",
                                            "Summarize Data" = "summarize",
                                            "Inspect Data Types" = "inspect",
                                            "Remove Columns" = "remove",
                                            "Remove Duplicates" = "remove_duplicates",
                                            "Keep Only Duplicates" = "keep_duplicates",
                                            "Replace Value" = "replace_value",
                                            "Add Conditional Column" = "conditional")),  # Add this line
                    # Merge Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'merge'",
                      selectInput("table1", "Select First Table", choices = NULL),
                      selectInput("table2", "Select Second Table", choices = NULL),
                      selectInput("merge_type", "Merge Type",
                                  choices = c("Inner Join", 
                                              "Left Join",
                                              "Right Join",
                                              "Full Join",
                                              "Left Anti Join",
                                              "Right Anti Join")),
                      # Add column selection for both tables
                      selectizeInput("merge_by_table1", "Select Columns from First Table", 
                                     choices = NULL, multiple = TRUE),
                      selectizeInput("merge_by_table2", "Select Columns from Second Table", 
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_merge", "Merge Tables", 
                                   class = "btn-primary")
                    ),
                    # Append Panel (unchanged)
                    conditionalPanel(
                      condition = "input.manipulation_type == 'append'",
                      selectizeInput("tables_to_append", "Select Tables to Append",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_append", "Append Tables")
                    ),
                    # Enhanced Summarize Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'summarize'",
                      selectInput("sum_table", "Select Table", choices = NULL),
                      selectizeInput("sum_group", "Group By Columns", 
                                     choices = NULL, multiple = TRUE),
                      selectizeInput("sum_cols", "Select Columns to Summarize",
                                     choices = NULL, multiple = TRUE),
                      selectizeInput("sum_functions", "Select Summary Functions",
                                     choices = c("Mean" = "mean", 
                                                 "Sum" = "sum", 
                                                 "Count" = "length",
                                                 "Min" = "min",
                                                 "Max" = "max",
                                                 "Median" = "median",
                                                 "Standard Deviation" = "sd"),
                                     multiple = TRUE,
                                     selected = c("mean", "sum", "length")),
                      actionButton("do_summarize", "Create Summary")
                    ),
                    # Add new Inspect Data Types Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'inspect'",
                      selectInput("inspect_table", "Select Table", choices = NULL),
                      DTOutput("column_types_table")
                    ),
                    # Add the Remove Columns Panel after the other panels
                    conditionalPanel(
                      condition = "input.manipulation_type == 'remove'",
                      selectInput("remove_table", "Select Table", choices = NULL),
                      selectizeInput("columns_to_remove", "Select Columns to Remove",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_remove", "Remove Columns",
                                   class = "btn-warning")
                    ),
                    # Add Remove Duplicates Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'remove_duplicates'",
                      selectInput("remove_duplicates_table", "Select Table", choices = NULL),
                      selectizeInput("remove_duplicates_columns", "Select Columns to Check for Duplicates",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_remove_duplicates", "Remove Duplicates", class = "btn-warning")
                    ),
                    # Add Keep Only Duplicates Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'keep_duplicates'",
                      selectInput("keep_duplicates_table", "Select Table", choices = NULL),
                      selectizeInput("keep_duplicates_columns", "Select Columns to Check for Duplicates",
                                     choices = NULL, multiple = TRUE),
                      actionButton("do_keep_duplicates", "Keep Only Duplicates", class = "btn-warning")
                    ),
                    # Add Replace Value Panel
                    conditionalPanel(
                      condition = "input.manipulation_type == 'replace_value'",
                      selectInput("replace_value_table", "Select Table", choices = NULL),
                      selectizeInput("replace_value_columns", "Select Columns to Replace Values",
                                     choices = NULL, multiple = TRUE),
                      textInput("old_value", "Old Value"),
                      textInput("new_value", "New Value"),
                      actionButton("do_replace_value", "Replace Value", class = "btn-warning")
                    ),
                    # Update Conditional Column Panel with multiple conditions
                    conditionalPanel(
                      condition = "input.manipulation_type == 'conditional'",
                      selectInput("conditional_table", "Select Table", choices = NULL),
                      textInput("new_column_name", "New Column Name"),
                      
                      # First condition
                      tags$div(
                        style = "border: 1px solid #ddd; padding: 10px; margin: 5px;",
                        selectInput("condition_column_1", "Column to Check", choices = NULL),
                        selectInput("condition_operator_1", "Operator", 
                                    choices = c("equals" = "==",
                                                "not equals" = "!=",
                                                "greater than" = ">",
                                                "less than" = "<",
                                                "greater or equal" = ">=",
                                                "less or equal" = "<=",
                                                "contains" = "contains",
                                                "starts with" = "startswith",
                                                "ends with" = "endswith")),
                        textInput("condition_value_1", "Value to Compare")
                      ),
                      
                      # Logic selector for second condition
                      selectInput("condition_logic", "Add Second Condition?", 
                                  choices = c("None", "AND", "OR")),
                      
                      # Second condition (shown conditionally)
                      conditionalPanel(
                        condition = "input.condition_logic !== 'None'",
                        tags$div(
                          style = "border: 1px solid #ddd; padding: 10px; margin: 5px;",
                          selectInput("condition_column_2", "Second Column to Check", choices = NULL),
                          selectInput("condition_operator_2", "Second Operator", 
                                      choices = c("equals" = "==",
                                                  "not equals" = "!=",
                                                  "greater than" = ">",
                                                  "less than" = "<",
                                                  "greater or equal" = ">=",
                                                  "less or equal" = "<=",
                                                  "contains" = "contains",
                                                  "starts with" = "startswith",
                                                  "ends with" = "endswith")),
                          textInput("condition_value_2", "Second Value to Compare")
                        )
                      ),
                      
                      textInput("true_value", "Value if True"),
                      textInput("false_value", "Value if False"),
                      actionButton("do_conditional", "Add Conditional Column", class = "btn-primary")
                    )
                ),
                box(width = 6,
                    title = "Operation Preview",
                    status = "info",
                    solidHeader = TRUE,
                    DTOutput("manipulation_preview")
                )
              )
      ),
      
      # Visualization Tab
      tabItem(tabName = "viz",
              fluidRow(
                box(width = 12,
                    selectInput("viz_table", "Select Table for Visualization", 
                                choices = NULL),
                    esquisse::esquisse_ui(
                      id = "esquisse"
                    )
                )
              )
      ),
      tabItem(tabName = "pivot",
              fluidRow(
                box(width = 12,
                    title = "Pivot Table Analysis",
                    status = "primary",
                    solidHeader = TRUE,
                    selectInput("pivot_table", "Select Table for Pivot Analysis", 
                                choices = NULL),
                    actionButton("create_pivot", "Create Pivot Table", 
                                 class = "btn-primary")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Interactive Pivot Table",
                    status = "info",
                    solidHeader = TRUE,
                    height = "auto",
                    style = "overflow: visible; min-height: 600px;",
                    div(
                      style = "overflow: visible; width: 100%;",
                      rpivotTableOutput("pivot_output", height = "auto")
                    )
                )
              )
      ),
      tabItem(tabName = "forecasting",
              fluidRow(
                box(width = 12,
                    title = "Forecasting with Prophet",
                    status = "primary",
                    solidHeader = TRUE,
                    selectInput("forecast_table", "Select Table", choices = NULL),
                    selectInput("date_column", "Select Date Column", choices = NULL),
                    selectInput("value_column", "Select Value Column", choices = NULL),
                    numericInput("forecast_period", "Forecast Period (days)", value = 30, min = 1),
                    actionButton("run_forecast", "Run Forecast", class = "btn-primary")
                )
              ),
              fluidRow(
                box(width = 12,
                    title = "Forecast Results",
                    status = "info",
                    solidHeader = TRUE,
                    div(style = "position: relative;",
                        plotlyOutput("forecast_plot")
                    ),
                    div(style = "margin-top: 20px;",
                        DTOutput("forecast_table")
                    )
                )
              )
      )
    )
  )
)

# Server Logic
server <- function(input, output, session) {
  # Initialize reactive values
  datasets <- reactiveVal(list())  # Change from reactiveValues to reactiveVal
  db_conn <- reactiveVal(NULL)
  
  # Add a reactive for current data preview
  selected_data <- reactive({
    req(input$manipulation_type)
    
    if (input$manipulation_type == "merge") {
      req(input$table1, input$table2)
      list(
        table1 = datasets()[[input$table1]],
        table2 = datasets()[[input$table2]]
      )
    } else if (input$manipulation_type %in% c("summarize", "inspect", "remove")) {
      req(input$sum_table %||% input$inspect_table %||% input$remove_table)
      datasets()[[input$sum_table %||% input$inspect_table %||% input$remove_table]]
    }
  })
  
  # Add preview output
  output$manipulation_preview <- renderDT({
    req(selected_data())
    if (input$manipulation_type == "merge") {
      head(selected_data()$table1, 5)
    } else {
      head(selected_data(), 5)
    }
  })
  
  # Function to read files based on extension with optimizations
  read_file <- optimized_read_file
  
  
  # Handle file uploads with compression
  observeEvent(input$files, {
    req(input$files)
    withProgress(message = 'Processing files', value = 0, {
      current_data <- datasets()
      
      for(i in seq_along(input$files$name)) {
        incProgress(i/length(input$files$name), 
                    detail = paste("Processing", input$files$name[i]))
        
        base_name <- tools::file_path_sans_ext(input$files$name[i])
        ext <- tolower(file_ext(input$files$name[i]))
        
        if (ext %in% c("xls", "xlsx")) {
          # Get all available sheets from the Excel file
          sheets <- excel_sheets(input$files$datapath[i])
          showModal(modalDialog(
            title = paste("Select Sheet for", input$files$name[i]),
            selectInput(paste0("sheet_select_", i), "Select Sheet Name:", choices = sheets),
            footer = tagList(
              modalButton("Cancel"),
              actionButton(paste0("ok_sheet_", i), "OK")
            )
          ))
          
          observeEvent(input[[paste0("ok_sheet_", i)]], {
            removeModal()
            sheet_name <- input[[paste0("sheet_select_", i)]]
            new_data <- tryCatch({
              as.data.table(read_excel(input$files$datapath[i], sheet = sheet_name))
            }, error = function(e) {
              showNotification(paste("Error reading sheet from", input$files$name[i]), type = "error")
              NULL
            })
            
            if (!is.null(new_data)) {
              # Compress dataset
              new_data <- compress_dataset(new_data)
              
              # Handle duplicate names
              if (base_name %in% names(current_data)) {
                counter <- 1
                while(paste0(base_name, "_", counter) %in% names(current_data)) {
                  counter <- counter + 1
                }
                base_name <- paste0(base_name, "_", counter)
              }
              current_data[[base_name]] <- new_data
              datasets(current_data)
              updateAllSelectInputs(session)
            }
          })
        } else {
          new_data <- optimized_read_file(input$files$datapath[i])
          
          if (!is.null(new_data)) {
            # Compress dataset
            new_data <- compress_dataset(new_data)
            
            # Handle duplicate names
            if (base_name %in% names(current_data)) {
              counter <- 1
              while(paste0(base_name, "_", counter) %in% names(current_data)) {
                counter <- counter + 1
              }
              base_name <- paste0(base_name, "_", counter)
            }
            current_data[[base_name]] <- new_data
          } else {
            showNotification(paste("Failed to load data from", input$files$name[i]), type = "error")
          }
        }
      }
      
      datasets(current_data)
      updateAllSelectInputs(session)
    })
  })
  
  # Function to update all select inputs
  updateAllSelectInputs <- function(session) {
    choices <- names(datasets())
    updateSelectInput(session, "table1", choices = choices)
    updateSelectInput(session, "table2", choices = choices)
    updateSelectizeInput(session, "tables_to_append", choices = choices)
    updateSelectInput(session, "sum_table", choices = choices)
    updateSelectInput(session, "viz_table", choices = choices)
    updateSelectInput(session, "inspect_table", choices = choices)
    updateSelectInput(session, "remove_table", choices = choices)
    updateSelectInput(session, "remove_duplicates_table", choices = choices)
    updateSelectInput(session, "keep_duplicates_table", choices = choices)
    updateSelectInput(session, "replace_value_table", choices = choices)
    updateSelectInput(session, "forecast_table", choices = choices)
    updateSelectInput(session, "conditional_table", choices = choices)
    updateSelectInput(session, "pivot_table", choices = choices) # Add pivot table selection
  }
  
  # Update merge columns when tables are selected
  observeEvent(c(input$table1, input$table2), {
    req(input$table1, input$table2)
    df1 <- datasets()[[input$table1]]
    df2 <- datasets()[[input$table2]]
    
    if (!is.null(df1) && !is.null(df2)) {
      updateSelectizeInput(session, "merge_by_table1", 
                           choices = names(df1))
      updateSelectizeInput(session, "merge_by_table2", 
                           choices = names(df2))
    }
  })
  
  # Enhanced merge operation
  observeEvent(input$do_merge, {
    req(input$table1, input$table2, input$merge_type, 
        input$merge_by_table1, input$merge_by_table2)
    
    # Validate same number of columns selected
    if (length(input$merge_by_table1) != length(input$merge_by_table2)) {
      showNotification("Please select the same number of columns from both tables", 
                       type = "error")
      return()
    }
    
    df1 <- datasets()[[input$table1]]
    df2 <- datasets()[[input$table2]]
    
    # Create named vector for merge columns
    by_cols <- setNames(input$merge_by_table1, input$merge_by_table2)
    
    tryCatch({
      merged_data <- switch(input$merge_type,
                            "Inner Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                 by.y = input$merge_by_table2),
                            "Left Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                by.y = input$merge_by_table2, all.x = TRUE),
                            "Right Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                 by.y = input$merge_by_table2, all.y = TRUE),
                            "Full Join" = merge(df1, df2, by.x = input$merge_by_table1, 
                                                by.y = input$merge_by_table2, all = TRUE),
                            "Left Anti Join" = df1[!do.call(paste0, df1[input$merge_by_table1]) %in% 
                                                     do.call(paste0, df2[input$merge_by_table2]), ],
                            "Right Anti Join" = df2[!do.call(paste0, df2[input$merge_by_table2]) %in% 
                                                      do.call(paste0, df1[input$merge_by_table1]), ]
      )
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$table1]] <- merged_data
      datasets(current_data)
      updateAllSelectInputs(session)
      showNotification("Merged data successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Merge failed:", e$message), type = "error")
    })
  })
  
  # Update summarize columns when table is selected
  observeEvent(input$sum_table, {
    req(input$sum_table)
    data <- datasets()[[input$sum_table]]
    if (!is.null(data)) {
      # Get numeric columns for summarization
      numeric_cols <- names(data)[sapply(data, is.numeric)]
      updateSelectizeInput(session, "sum_cols", 
                           choices = numeric_cols)
      # All columns can be used for grouping
      updateSelectizeInput(session, "sum_group", 
                           choices = names(data))
    }
  })
  
  # Add reactive value to store folder path
  selected_folder <- reactiveVal(NULL)
  
  # Handle folder loading
  observeEvent(input$folder, {
    req(input$folder)
    withProgress(message = 'Loading folder contents', value = 0, {
      files <- input$folder$datapath
      
      # Check if we have only Excel files
      if(length(files) > 0 && all(tolower(tools::file_ext(files)) %in% c("xls", "xlsx"))) {
        # Get all available sheets from first file
        sheets <- excel_sheets(files[1])
        showModal(modalDialog(
          title = "Select Sheet to Combine",
          selectInput("combine_sheet", "Select Sheet Name:", choices = sheets),
          footer = tagList(
            modalButton("Cancel"),
            actionButton("ok_combine_sheet", "OK")
          )
        ))
      } else {
        # Original folder loading logic for mixed file types
        processFiles(files)
      }
    })
  })
  
  # Add new function to process files
  processFiles <- function(files, sheet_name = NULL) {
    all_data <- list()
    for(i in seq_along(files)) {
      incProgress(i/length(files), 
                  detail = paste("Processing", basename(files[i])))
      
      ext <- tolower(tools::file_ext(files[i]))
      
      if(ext == "csv") {
        data <- optimized_read_file(files[i])
      } else if(ext %in% c("xls", "xlsx")) {
        if(!is.null(sheet_name)) {
          # Use specified sheet name for all Excel files
          data <- tryCatch({
            as.data.table(read_excel(files[i], sheet = sheet_name))
          }, error = function(e) {
            showNotification(
              paste("Error reading sheet from", basename(files[i])), 
              type = "warning"
            )
            NULL
          })
        } else {
          data <- safe_read_excel(files[i])
        }
      }
      
      if(!is.null(data) && nrow(data) > 0) {
        all_data[[length(all_data) + 1]] <- data
      }
      gc()
    }
    
    if(length(all_data) > 0) {
      combined_data <- rbindlist(all_data, fill = TRUE, use.names = TRUE)
      combined_data <- compress_dataset(combined_data)
      
      current_data <- datasets()
      new_name <- "combined_data"
      if(new_name %in% names(current_data)) {
        counter <- 1
        while(paste0(new_name, "_", counter) %in% names(current_data)) {
          counter <- 1
        }
        new_name <- paste0(new_name, "_", counter)
      }
      current_data[[new_name]] <- combined_data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      showNotification(paste("Successfully loaded", length(files), "files"), type = "message")
    } else {
      showNotification("No valid data found in files", type = "warning")
    }
  }
  
  # Add handler for sheet selection
  observeEvent(input$ok_combine_sheet, {
    req(input$combine_sheet, input$folder)
    removeModal()
    
    withProgress(message = 'Loading folder contents', value = 0, {
      files <- input$folder$datapath
      processFiles(files, input$combine_sheet)
    })
  })
  
  
  
  
  
  
  # Generate tabs for each dataset
  output$table_tabs <- renderUI({
    req(datasets())
    do.call(tabBox, c(
      width = 12,
      lapply(names(datasets()), function(name) {
        tabPanel(
          title = span(name, 
                       tags$button(class = "close", type = "button", 
                                   "×", onclick = sprintf("Shiny.setInputValue('remove_dataset', '%s')", name))),
          DTOutput(paste0("table_", gsub("[^[:alnum:]]", "", name)))
        )
      })
    ))
  })
  
  # Handle dataset removal
  observeEvent(input$remove_dataset, {
    current_data <- datasets()
    current_data[[input$remove_dataset]] <- NULL
    datasets(current_data)
  })
  
  # Render individual tables with optimizations
  observe({
    req(datasets())
    for(name in names(datasets())) {
      local({
        local_name <- name
        output[[paste0("table_", gsub("[^[:alnum:]]", "", local_name))]] <- renderDT({
          data <- datasets()[[local_name]]
          # Sample data for preview
          preview_data <- sample_large_dataset(data)
          
          datatable(
            preview_data,
            options = list(
              pageLength = input$page_size,
              processing = TRUE,
              serverSide = TRUE,
              scrollX = TRUE,
              scrollY = "400px",
              dom = 'Bfrtip',
              buttons = c('copy', 'csv', 'excel', 'pdf', 'print'),
              deferRender = TRUE,
              scroller = TRUE
            ),
            extensions = c('Buttons', 'Scroller'),
            filter = 'top',
            style = 'bootstrap',
            class = 'cell-border stripe'
          )
        })
      })
    }
  })
  
  # Handle append operation
  observeEvent(input$do_append, {
    req(input$tables_to_append)
    selected_data <- datasets()[input$tables_to_append]
    appended_data <- rbindlist(selected_data, fill = TRUE)
    
    current_data <- datasets()
    current_data[["appended_result"]] <- appended_data
    datasets(current_data)
  })
  
  # Enhanced summarize operation
  observeEvent(input$do_summarize, {
    req(input$sum_table, input$sum_group, input$sum_cols, input$sum_functions)
    data <- datasets()[[input$sum_table]]
    
    tryCatch({
      # Create summary functions list
      summary_functions <- lapply(input$sum_functions, function(fn) {
        switch(fn,
               "mean" = function(x) if(is.numeric(x)) mean(x, na.rm = TRUE) else NA_real_,
               "sum" = function(x) if(is.numeric(x)) sum(x, na.rm = TRUE) else NA_real_,
               "length" = length,
               "min" = function(x) if(is.numeric(x)) min(x, na.rm = TRUE) else NA_real_,
               "max" = function(x) if(is.numeric(x)) max(x, na.rm = TRUE) else NA_real_,
               "median" = function(x) if(is.numeric(x)) median(x, na.rm = TRUE) else NA_real_,
               "sd" = function(x) if(is.numeric(x)) sd(x, na.rm = TRUE) else NA_real_
        )
      })
      names(summary_functions) <- input$sum_functions
      
      # Perform summarization
      summary_data <- data %>%
        group_by(across(all_of(input$sum_group))) %>%
        summarise(across(all_of(input$sum_cols), 
                         summary_functions,
                         .names = "{.col}_{.fn}"),
                  .groups = "drop")
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$sum_table]] <- as.data.table(summary_data)
      datasets(current_data)
      updateAllSelectInputs(session)
      showNotification("Summary data created successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Summarize failed:", e$message), 
                       type = "error")
    })
  })
  
  # Add memory monitoring
  observe({
    invalidateLater(5000)  # Update every 5 seconds
    shinyjs::html("memory_usage", get_memory_usage())
  })
  
  # Optimize visualization data handling
  observeEvent(input$viz_table, {
    req(input$viz_table)
    data <- datasets()[[input$viz_table]]
    
    # Sample data for visualization if too large
    if(nrow(data) > 10000) {
      data <- sample_large_dataset(data, n = 10000)
      showNotification(
        "Dataset sampled to 10,000 rows for visualization", 
        type = "warning"
      )
    }
    
    esquisse::esquisse_server(
      id = "esquisse",
      data = data
    )
  })
  
  # Add Excel sheet handling to server
  observeEvent(input$ok_sheet, {
    req(input$files, input$sheet_select)
    file_path <- input$files$datapath[1]  # Get the current Excel file path
    
    tryCatch({
      data <- as.data.table(read_excel(file_path, 
                                       sheet = input$sheet_select))
      
      # Handle file naming
      base_name <- tools::file_path_sans_ext(input$files$name[1])
      if (input$sheet_select != "Sheet1") {
        base_name <- paste0(base_name, "_", input$sheet_select)
      }
      
      current_data <- datasets()
      if (base_name %in% names(current_data)) {
        counter <- 1
        while(paste0(base_name, "_", counter) %in% names(current_data)) {
          counter <- counter + 1
        }
        base_name <- paste0(base_name, "_", counter)
      }
      
      current_data[[base_name]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      removeModal()
      
    }, error = function(e) {
      showNotification(paste("Error loading Excel sheet:", e$message), 
                       type = "error")
    })
  })
  
  # Update the column type inspection output
  output$column_types_table <- renderDT({
    req(input$manipulation_type == "inspect", input$inspect_table)
    
    tryCatch({
      data <- datasets()[[input$inspect_table]]
      
      if (!is.null(data)) {
        # Create a simplified data frame with just column names and types
        col_info <- data.frame(
          Column = names(data),
          Type = sapply(data, function(x) {
            # Get basic type information
            if (is.factor(x)) {
              "Factor"
            } else if (is.numeric(x)) {
              if (all(floor(x[!is.na(x)]) == x[!is.na(x)])) {
                "Integer"
              } else {
                "Numeric"
              }
            } else if (is.character(x)) {
              "Character"
            } else if (inherits(x, "Date")) {
              "Date"
            } else if (inherits(x, "POSIXct")) {
              "DateTime"
            } else if (is.logical(x)) {
              "Logical"
            } else {
              class(x)[1]
            }
          })
        )
        
        # Render the table with minimal options
        datatable(
          col_info,
          options = list(
            pageLength = 25,
            scrollY = "400px",
            dom = 't',  # Show only the table
            ordering = TRUE,
            searching = FALSE
          ),
          style = 'bootstrap',
          rownames = FALSE
        )
      }
    }, error = function(e) {
      showNotification(paste("Error inspecting data types:", e$message), 
                       type = "error")
      return(NULL)
    })
  })
  
  # Database connection logic
  observeEvent(input$db_connect, {
    req(input$db_type)
    
    tryCatch({
      conn <- switch(input$db_type,
                     "MySQL" = dbConnect(
                       RMySQL::MySQL(),
                       host = input$db_host,
                       port = input$db_port,
                       user = input$db_user,
                       password = input$db_pass,
                       dbname = input$db_name
                     ),
                     "PostgreSQL" = dbConnect(
                       RPostgres::Postgres(),
                       host = input$db_host,
                       port = input$db_port,
                       user = input$db_user,
                       password = input$db_pass,
                       dbname = input$db_name
                     ),
                     "SQLite" = {
                       req(input$sqlite_file)
                       dbConnect(RSQLite::SQLite(), 
                                 dbname = input$sqlite_file$datapath)
                     },
                     "SQL Server" = dbConnect(
                       odbc::odbc(),
                       Driver = input$db_driver,
                       Server = input$db_host,
                       Database = input$db_name,
                       UID = input$db_user,
                       PWD = input$db_pass,
                       Port = input$db_port
                     ),
                     "Azure SQL" = dbConnect(
                       odbc::odbc(),
                       Driver = input$db_driver,
                       Server = paste0(input$db_host, ",", input$db_port),
                       Database = input$db_name,
                       UID = input$db_user,
                       PWD = input$db_pass,
                       Encrypt = "yes",
                       TrustServerCertificate = "no"
                     ),
                     "Other (ODBC)" = dbConnect(
                       odbc::odbc(),
                       DSN = input$db_dsn,
                       Driver = input$db_driver,
                       UID = input$db_user,
                       PWD = input$db_pass
                     )
      )
      
      db_conn(conn)
      showNotification("Connected successfully!", type = "message")
      
      # After successful connection, update the table list
      tables <- dbListTables(conn)
      updateSelectInput(session, "db_table", choices = tables)
      
    }, error = function(e) {
      showNotification(paste("Connection failed:", e$message), 
                       type = "error", duration = 10)
    })
  })
  
  # Add a preview of the selected database table
  output$db_tables_preview <- renderDT({
    req(db_conn(), input$db_table)
    tryCatch({
      data <- dbGetQuery(db_conn(), 
                         paste("SELECT * FROM", input$db_table, "LIMIT 1000"))
      datatable(
        data,
        options = list(
          pageLength = 10,
          scrollX = TRUE,
          scrollY = "300px",
          scroller = TRUE
        ),
        style = 'bootstrap'
      )
    }, error = function(e) {
      showNotification(paste("Error loading table preview:", e$message), 
                       type = "error")
      return(NULL)
    })
  })
  
  # Database table selection UI
  output$db_table_ui <- renderUI({
    req(db_conn())
    tables <- dbListTables(db_conn())
    selectInput("db_table", "Select Table", choices = tables)
  })
  
  # Optimize database loading with chunking
  observeEvent(input$db_table, {
    req(db_conn(), input$db_table)
    withProgress(message = 'Loading database table', value = 0, {
      tryCatch({
        # Get total count
        count_query <- sprintf("SELECT COUNT(*) as count FROM %s", input$db_table)
        total_rows <- dbGetQuery(db_conn(), count_query)$count
        
        chunk_size <- 50000
        chunks <- ceiling(total_rows / chunk_size)
        
        all_data <- data.table()
        
        for(i in 1:chunks) {
          incProgress(i/chunks, 
                      detail = sprintf("Loading chunk %d of %d", i, chunks))
          
          offset <- (i-1) * chunk_size
          query <- sprintf(
            "SELECT * FROM %s LIMIT %d OFFSET %d", 
            input$db_table, chunk_size, offset
          )
          chunk_data <- as.data.table(dbGetQuery(db_conn(), query))
          all_data <- rbindlist(list(all_data, chunk_data), fill=TRUE)
          
          # Force garbage collection
          gc()
        }
        
        current_data <- datasets()
        current_data[[input$db_table]] <- all_data
        datasets(current_data)
        updateAllSelectInputs(session)
        
      }, error = function(e) {
        showNotification(
          paste("Error loading table:", e$message), 
          type = "error"
        )
      })
    })
  })
  
  # Close database connection on exit
  session$onSessionEnded(function() {
    if (!is.null(db_conn())) {
      dbDisconnect(db_conn())
    }
  })
  
  # Update columns for remove duplicates and keep duplicates when table is selected
  observeEvent(input$remove_duplicates_table, {
    req(input$remove_duplicates_table)
    data <- datasets()[[input$remove_duplicates_table]]
    if (!is.null(data)) {
      updateSelectizeInput(session, "remove_duplicates_columns", choices = names(data))
    }
  })
  
  observeEvent(input$keep_duplicates_table, {
    req(input$keep_duplicates_table)
    data <- datasets()[[input$keep_duplicates_table]]
    if (!is.null(data)) {
      updateSelectizeInput(session, "keep_duplicates_columns", choices = names(data))
    }
  })
  
  # Handle remove duplicates operation
  observeEvent(input$do_remove_duplicates, {
    req(input$remove_duplicates_table, input$remove_duplicates_columns)
    
    tryCatch({
      data <- datasets()[[input$remove_duplicates_table]]
      data <- data[!duplicated(data, by = input$remove_duplicates_columns)]
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$remove_duplicates_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show success message
      showNotification("Duplicates removed successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error removing duplicates:", e$message), type = "error")
    })
  })
  
  # Handle keep only duplicates operation
  observeEvent(input$do_keep_duplicates, {
    req(input$keep_duplicates_table, input$keep_duplicates_columns)
    
    tryCatch({
      data <- datasets()[[input$keep_duplicates_table]]
      data <- data[duplicated(data, by = input$keep_duplicates_columns) | 
                     duplicated(data, by = input$keep_duplicates_columns, fromLast = TRUE)]
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$keep_duplicates_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show success message
      showNotification("Only duplicates kept successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error keeping only duplicates:", e$message), type = "error")
    })
  })
  
  # Update columns for replace value when table is selected
  observeEvent(input$replace_value_table, {
    req(input$replace_value_table)
    data <- datasets()[[input$replace_value_table]]
    if (!is.null(data)) {
      updateSelectizeInput(session, "replace_value_columns", choices = names(data))
    }
  })
  
  # Handle replace value operation
  observeEvent(input$do_replace_value, {
    req(input$replace_value_table, input$replace_value_columns, input$old_value, input$new_value)
    
    tryCatch({
      data <- datasets()[[input$replace_value_table]]
      for (col in input$replace_value_columns) {
        data[[col]][data[[col]] == input$old_value] <- input$new_value
      }
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$replace_value_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Show success message
      showNotification("Values replaced successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error replacing values:", e$message), type = "error")
    })
  })
  
  # Update columns for forecasting when table is selected
  observeEvent(input$forecast_table, {
    req(input$forecast_table)
    data <- datasets()[[input$forecast_table]]
    if (!is.null(data)) {
      updateSelectInput(session, "date_column", choices = names(data))
      updateSelectInput(session, "value_column", choices = names(data))
    }
  })
  
  # Handle forecasting operation
  observeEvent(input$run_forecast, {
    req(input$forecast_table, input$date_column, input$value_column, input$forecast_period)
    
    tryCatch({
      data <- datasets()[[input$forecast_table]]
      df <- data[, .(ds = as.Date(get(input$date_column)), y = as.numeric(get(input$value_column)))]
      
      # Fit the model
      m <- prophet(df)
      
      # Make future dataframe
      future <- make_future_dataframe(m, periods = input$forecast_period)
      
      # Predict
      forecast <- predict(m, future)
      
      # Update datasets with forecast
      forecast_data <- data.table(ds = forecast$ds, yhat = round(forecast$yhat, 2), yhat_lower = round(forecast$yhat_lower, 2), yhat_upper = round(forecast$yhat_upper, 2))
      current_data <- datasets()
      current_data[["forecast_results"]] <- forecast_data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      # Plot forecast using plotly
      output$forecast_plot <- renderPlotly({
        plot_ly() %>%
          add_lines(x = ~forecast_data$ds, y = ~forecast_data$yhat, name = 'Forecast') %>%
          add_ribbons(x = ~forecast_data$ds, ymin = ~forecast_data$yhat_lower, ymax = ~forecast_data$yhat_upper, name = 'Uncertainty', fillcolor = 'rgba(7, 164, 181, 0.2)', line = list(color = 'transparent')) %>%
          layout(title = 'Forecast', xaxis = list(title = 'Date'), yaxis = list(title = 'Value', tickformat = ".2f"))
      })
      
      # Show forecast table
      output$forecast_table <- renderDT({
        datatable(forecast_data, 
                  options = list(
                    pageLength = 10, 
                    scrollX = TRUE,
                    dom = 'Bfrtip',
                    buttons = list(
                      'copy',
                      list(
                        extend = 'collection',
                        buttons = list(
                          list(
                            extend = 'csv',
                            filename = paste0("forecast_results_", format(Sys.Date(), "%Y%m%d"))
                          ),
                          list(
                            extend = 'excel',
                            filename = paste0("forecast_results_", format(Sys.Date(), "%Y%m%d"))
                          ),
                          list(
                            extend = 'pdf',
                            filename = paste0("forecast_results_", format(Sys.Date(), "%Y%m%d"))
                          )
                        ),
                        text = 'Download'
                      ),
                      'print',
                      list(
                        extend = 'collection',
                        text = 'View',
                        action = JS("function ( e, dt, node, config ) {
                         Shiny.setInputValue('fullscreen_plot', true);
                       }")
                      )
                    )
                  ),
                  extensions = c('Buttons'),
                  callback = JS("
                   table.buttons().container().css('margin-bottom', '10px');
                   table.buttons().container().css('margin-right', '10px');
                 ")
        )
      })
      
      showNotification("Forecast completed successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error running forecast:", e$message), type = "error")
    })
  })
  
  # Handle full-screen plot
  observeEvent(input$fullscreen_plot, {
    showModal(modalDialog(
      title = "Forecast Plot",
      size = "l",
      easyClose = TRUE,
      plotlyOutput("fullscreen_forecast_plot", height = "600px")
    ))
  })
  
  # Render full-screen plot
  output$fullscreen_forecast_plot <- renderPlotly({
    req(datasets()[["forecast_results"]])
    forecast_data <- datasets()[["forecast_results"]]
    
    plot_ly() %>%
      add_lines(x = ~forecast_data$ds, y = ~forecast_data$yhat, name = 'Forecast') %>%
      add_ribbons(x = ~forecast_data$ds, ymin = ~forecast_data$yhat_lower, ymax = ~forecast_data$yhat_upper, 
                  name = 'Uncertainty', fillcolor = 'rgba(7, 164, 181, 0.2)', line = list(color = 'transparent')) %>%
      layout(title = 'Forecast',
             xaxis = list(title = 'Date'),
             yaxis = list(title = 'Value', tickformat = ".2f"),
             showlegend = TRUE,
             margin = list(l = 50, r = 50, b = 50, t = 50, pad = 4))
  })
  
  # Update condition column choices when table is selected
  observeEvent(input$conditional_table, {
    req(input$conditional_table)
    data <- datasets()[[input$conditional_table]]
    if (!is.null(data)) {
      updateSelectInput(session, "condition_column", 
                        choices = names(data))
    }
  })
  
  # Handle conditional column operation
  observeEvent(input$do_conditional, {
    req(input$conditional_table, input$condition_column, 
        input$condition_operator, input$condition_value,
        input$true_value, input$false_value, input$new_column_name)
    
    tryCatch({
      data <- datasets()[[input$conditional_table]]
      
      # Create the conditional column based on operator
      result <- switch(input$condition_operator,
                       "==" = data[[input$condition_column]] == input$condition_value,
                       "!=" = data[[input$condition_column]] != input$condition_value,
                       ">" = as.numeric(data[[input$condition_column]]) > as.numeric(input$condition_value),
                       "<" = as.numeric(data[[input$condition_column]]) < as.numeric(input$condition_value),
                       ">=" = as.numeric(data[[input$condition_column]]) >= as.numeric(input$condition_value),
                       "<=" = as.numeric(data[[input$condition_column]]) <= as.numeric(input$condition_value),
                       "contains" = grepl(input$condition_value, data[[input$condition_column]], fixed = TRUE),
                       "startswith" = startsWith(as.character(data[[input$condition_column]]), input$condition_value),
                       "endswith" = endsWith(as.character(data[[input$condition_column]]), input$condition_value)
      )
      
      # Add the new column with conditional values
      data[, (input$new_column_name) := ifelse(result, input$true_value, input$false_value)]
      
      # Update datasets
      current_data <- datasets()
      current_data[[input$conditional_table]] <- data
      datasets(current_data)
      updateAllSelectInputs(session)
      
      showNotification("Conditional column added successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error adding conditional column:", e$message), 
                       type = "error")
    })
  })
  
  # Update both condition column choices when table is selected
  observeEvent(input$conditional_table, {
    req(input$conditional_table)
    data <- datasets()[[input$conditional_table]]
    if (!is.null(data)) {
      updateSelectInput(session, "condition_column_1", choices = names(data))
      updateSelectInput(session, "condition_column_2", choices = names(data))
    }
  })
  
  # Updated handle conditional column operation with multiple conditions
  observeEvent(input$do_conditional, {
    req(input$conditional_table, input$condition_column_1, 
        input$condition_operator_1, input$condition_value_1,
        input$true_value, input$false_value, input$new_column_name)
    
    tryCatch({
      data <- copy(datasets()[[input$conditional_table]])  # Create a copy of the data
      
      # Evaluate first condition
      result1 <- switch(input$condition_operator_1,
                        "==" = data[[input$condition_column_1]] == input$condition_value_1,
                        "!=" = data[[input$condition_column_1]] != input$condition_value_1,
                        ">" = as.numeric(data[[input$condition_column_1]]) > as.numeric(input$condition_value_1),
                        "<" = as.numeric(data[[input$condition_column_1]]) < as.numeric(input$condition_value_1),
                        ">=" = as.numeric(data[[input$condition_column_1]]) >= as.numeric(input$condition_value_1),
                        "<=" = as.numeric(data[[input$condition_column_1]]) <= as.numeric(input$condition_value_1),
                        "contains" = grepl(input$condition_value_1, data[[input$condition_column_1]], fixed = TRUE),
                        "startswith" = startsWith(as.character(data[[input$condition_column_1]]), input$condition_value_1),
                        "endswith" = EndsWith(as.character(data[[input$condition_column_1]]), input$condition_value_1))
      
      final_result <- result1
      
      # If second condition is active, evaluate it and combine with first condition
      if (input$condition_logic != "None") {
        req(input$condition_column_2, input$condition_operator_2, input$condition_value_2)
        
        result2 <- switch(input$condition_operator_2,
                          "==" = data[[input$condition_column_2]] == input$condition_value_2,
                          "!=" = data[[input$condition_column_2]] != input$condition_value_2,
                          ">" = as.numeric(data[[input$condition_column_2]]) > as.numeric(input$condition_value_2),
                          "<" = as.numeric(data[[input$condition_column_2]]) < as.numeric(input$condition_value_2),
                          ">=" = as.numeric(data[[input$condition_column_2]]) >= as.numeric(input$condition_value_2),
                          "<=" = as.numeric(data[[input$condition_column_2]]) <= as.numeric(input$condition_value_2),
                          "contains" = grepl(input$condition_value_2, data[[input$condition_column_2]], fixed = TRUE),
                          "startswith" = startsWith(as.character(data[[input$condition_column_2]]), input$condition_value_2),
                          "endswith" = EndsWith(as.character(data[[input$condition_column_2]]), input$condition_value_2))
        
        # Combine conditions based on logic
        final_result <- if (input$condition_logic == "AND") {
          result1 & result2
        } else {  # OR
          result1 | result2
        }
      }
      
      # Add the new column with conditional values
      data[, (input$new_column_name) := ifelse(final_result, input$true_value, input$false_value)]
      
      # Update datasets with the modified data
      current_data <- datasets()
      current_data[[input$conditional_table]] <- data
      datasets(current_data)
      
      # Show success message
      showNotification("Conditional column added successfully.", type = "message")
      
    }, error = function(e) {
      showNotification(paste("Error adding conditional column:", e$message), 
                       type = "error")
    })
  })
  
  # Add Pivot Table functionality
  observeEvent(input$create_pivot, {
    req(input$pivot_table)
    data <- datasets()[[input$pivot_table]]
    
    # Sample data for pivot table if too large
    if(nrow(data) > 20000) {
      data <- sample_large_dataset(data, n = 20000)
      showNotification(
        "Dataset sampled to 20,000 rows for pivot table analysis", 
        type = "warning"
      )
    }
    
    # Render the pivot table
    output$pivot_output <- renderRpivotTable({
      rpivotTable(data,
                  rows = names(data)[1], 
                  cols = if(length(names(data)) > 1) names(data)[2] else NULL,
                  aggregatorName = "Count",
                  rendererName = "Table",
                  width = "100%",
                  height = "auto",
                  onRefresh = htmlwidgets::JS("function(config) { setTimeout(function() { window.dispatchEvent(new Event('resize')); }, 500); }"))
    })
    
    # Force resize of the container after pivot table is rendered
    runjs("
      $(document).ready(function() {
        setTimeout(function() {
          window.dispatchEvent(new Event('resize'));
        }, 1000);
      });
    ")
  })
}

# Run the application
shinyApp(ui = ui, server = server)
