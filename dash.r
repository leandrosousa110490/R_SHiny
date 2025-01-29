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

# UI Definition
ui <- dashboardPage(
    dashboardHeader(title = "Advanced Data Dashboard"),
    dashboardSidebar(
        sidebarMenu(
            menuItem("Data Sources", tabName = "sources", icon = icon("database"),
                menuSubItem("File Upload", tabName = "file", icon = icon("file")),
                menuSubItem("Database", tabName = "db", icon = icon("server"))
            ),
            menuItem("Data Management", tabName = "data", icon = icon("table")),
            menuItem("Data Manipulation", tabName = "manipulation", icon = icon("tools")),
            menuItem("Visualization", tabName = "viz", icon = icon("chart-bar"))
        )
    ),
    dashboardBody(
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
                        actionButton("load_folder", "Load Folder"),
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
                                            "Remove Columns" = "remove")),
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
    
    # Function to read files based on extension
    read_file <- function(file_path) {
        ext <- tolower(file_ext(file_path))
        tryCatch({
            if (ext == "csv") {
                fread(file_path)
            } else if (ext %in% c("xls", "xlsx")) {
                # Get sheet names
                sheets <- excel_sheets(file_path)
                # Create modal dialog for sheet selection
                showModal(modalDialog(
                    title = "Select Excel Sheet",
                    selectInput("sheet_select", "Available Sheets:", 
                              choices = sheets),
                    footer = tagList(
                        modalButton("Cancel"),
                        actionButton("ok_sheet", "OK")
                    )
                ))
                return(NULL)  # Return NULL initially
            }
        }, error = function(e) {
            showNotification(paste("Error reading file:", e$message), 
                            type = "error")
            return(NULL)
        })
    }
    
    # Handle file uploads with duplicate name handling
    observeEvent(input$files, {
        req(input$files)
        current_data <- datasets()
        
        for(i in seq_along(input$files$name)) {
            base_name <- tools::file_path_sans_ext(input$files$name[i])
            new_data <- read_file(input$files$datapath[i])
            
            if (!is.null(new_data)) {
                # Handle duplicate names
                if (base_name %in% names(current_data)) {
                    counter <- 1
                    while(paste0(base_name, "_", counter) %in% names(current_data)) {
                        counter <- counter + 1
                    }
                    base_name <- paste0(base_name, "_", counter)
                }
                current_data[[base_name]] <- new_data
            }
        }
        
        datasets(current_data)
        updateAllSelectInputs(session)
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
            
            # Handle duplicate names
            new_name <- paste0(input$table1, "_merged_", input$table2)
            current_data <- datasets()
            if (new_name %in% names(current_data)) {
                counter <- 1
                while(paste0(new_name, "_", counter) %in% names(current_data)) {
                    counter <- counter + 1
                }
                new_name <- paste0(new_name, "_", counter)
            }
            
            current_data[[new_name]] <- merged_data
            datasets(current_data)
            updateAllSelectInputs(session)
            showNotification(paste("Merged data saved as:", new_name), 
                           type = "message")
            
        }, error = function(e) {
            showNotification(paste("Merge failed:", e$message), 
                           type = "error")
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
    
    # Handle folder loading
    observeEvent(input$load_folder, {
        folder <- choose.dir(caption = "Select folder containing data files")
        if (!is.null(folder)) {
            files <- list.files(folder, pattern = "\\.csv$|\\.xlsx$|\\.xls$", 
                              full.names = TRUE)
            all_data <- rbindlist(
                lapply(files, read_file),
                fill = TRUE
            )
            datasets(all_data)
        }
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
    
    # Render individual tables
    observe({
        req(datasets())
        for(name in names(datasets())) {
            local({
                local_name <- name
                output[[paste0("table_", gsub("[^[:alnum:]]", "", local_name))]] <- renderDT({
                    datatable(
                        datasets()[[local_name]],
                        options = list(
                            pageLength = input$page_size,
                            processing = TRUE,
                            scrollX = TRUE,
                            scrollY = "400px",
                            dom = 'Bfrtip',
                            buttons = c('copy', 'csv', 'excel', 'pdf', 'print')
                        ),
                        extensions = c('Buttons'),
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
            
            # Handle duplicate names
            new_name <- paste0(input$sum_table, "_summary")
            current_data <- datasets()
            if (new_name %in% names(current_data)) {
                counter <- 1
                while(paste0(new_name, "_", counter) %in% names(current_data)) {
                    counter <- counter + 1
                }
                new_name <- paste0(new_name, "_", counter)
            }
            
            # Update datasets
            current_data[[new_name]] <- as.data.table(summary_data)
            datasets(current_data)
            updateAllSelectInputs(session)
            showNotification(paste("Summary data saved as:", new_name), 
                           type = "message")
            
        }, error = function(e) {
            showNotification(paste("Summarize failed:", e$message), 
                           type = "error")
        })
    })
    
    # Initialize esquisse
    observeEvent(input$viz_table, {
        req(input$viz_table)
        data <- datasets()[[input$viz_table]]
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
    
    # Load data from selected database table
    observeEvent(input$db_table, {
        req(db_conn(), input$db_table)
        tryCatch({
            data <- dbGetQuery(db_conn(), 
                             paste("SELECT * FROM", input$db_table, "LIMIT 100000"))
            
            current_data <- datasets()
            current_data[[input$db_table]] <- as.data.table(data)
            datasets(current_data)
            
            # Update choices for all select inputs
            updateSelectInput(session, "table1", choices = names(datasets()))
            updateSelectInput(session, "table2", choices = names(datasets()))
            updateSelectizeInput(session, "tables_to_append", choices = names(datasets()))
            updateSelectInput(session, "sum_table", choices = names(datasets()))
            updateSelectInput(session, "viz_table", choices = names(datasets()))
            
        }, error = function(e) {
            showNotification(paste("Error loading table:", e$message), type = "error")
        })
    })
    
    # Close database connection on exit
    session$onSessionEnded(function() {
        if (!is.null(db_conn())) {
            dbDisconnect(db_conn())
        }
    })

    # Update column choices when remove table is selected
    observeEvent(input$remove_table, {
        req(input$remove_table)
        data <- datasets()[[input$remove_table]]
        if (!is.null(data)) {
            updateSelectizeInput(session, "columns_to_remove", 
                               choices = names(data))
        }
    })

    # Handle remove columns operation
    observeEvent(input$do_remove, {
        req(input$remove_table, input$columns_to_remove)
        
        tryCatch({
            data <- datasets()[[input$remove_table]]
            
            # Ensure we're not removing all columns
            if (length(input$columns_to_remove) >= ncol(data)) {
                showNotification("Cannot remove all columns from the table", 
                               type = "error")
                return()
            }
            
            # Remove selected columns
            data_subset <- data[, !(names(data) %in% input$columns_to_remove), with = FALSE]
            
            # Create new name for modified dataset
            new_name <- paste0(input$remove_table, "_modified")
            current_data <- datasets()
            if (new_name %in% names(current_data)) {
                counter <- 1
                while(paste0(new_name, "_", counter) %in% names(current_data)) {
                    counter <- counter + 1
                }
                new_name <- paste0(new_name, "_", counter)
            }
            
            # Update datasets
            current_data[[new_name]] <- data_subset
            datasets(current_data)
            updateAllSelectInputs(session)
            
            # Show success message
            showNotification(paste("Columns removed. New dataset saved as:", new_name), 
                           type = "message")
            
        }, error = function(e) {
            showNotification(paste("Error removing columns:", e$message), 
                           type = "error")
        })
    })
}

# Run the application
shinyApp(ui = ui, server = server)
