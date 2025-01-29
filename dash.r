# Load required libraries
library(shiny)
library(shinydashboard)
library(data.table)
library(DT)
library(readxl)
library(tools)
library(DBI)
library(odbc)
library(RMySQL)
library(RPostgres)
library(RSQLite)

# UI Definition
ui <- dashboardPage(
  dashboardHeader(title = "Multi-Table Data Dashboard"),
  dashboardSidebar(
    sidebarMenu(
      id = "menu",
      menuItem("File Upload", tabName = "file", icon = icon("file")),
      menuItem("Database", tabName = "db", icon = icon("database"))
    ),
    conditionalPanel(
      'input.menu === "file"',
      fileInput("files", "Upload Data Files", 
                multiple = TRUE,
                accept = c(".csv", ".xlsx", ".xls")),
      actionButton("load_folder", "Load Folder"),
      numericInput("page_size", "Rows per page:", 
                   value = 500, min = 100, max = 1000)
    ),
    conditionalPanel(
      'input.menu === "db"',
      selectInput("db_type", "Database Type",
                  choices = c("MySQL", "PostgreSQL", "SQLite", 
                              "SQL Server", "Azure SQL", "Other (ODBC)")),
      conditionalPanel(
        'input.db_type !== "SQLite"',
        textInput("db_host", "Host", value = "localhost"),
        numericInput("db_port", "Port", value = 3306),
        textInput("db_user", "Username"),
        passwordInput("db_pass", "Password")
      ),
      conditionalPanel(
        'input.db_type === "SQLite"',
        textInput("db_name", "Database File", 
                  placeholder = "path/to/database.sqlite")
      ),
      conditionalPanel(
        'input.db_type !== "SQLite"',
        textInput("db_name", "Database Name")
      ),
      conditionalPanel(
        'input.db_type === "SQL Server" || input.db_type === "Azure SQL"',
        textInput("db_driver", "ODBC Driver", 
                  value = "ODBC Driver 17 for SQL Server")
      ),
      conditionalPanel(
        'input.db_type === "Other (ODBC)"',
        textInput("db_dsn", "Data Source Name (DSN)"),
        textInput("db_driver", "Custom Driver Name")
      ),
      actionButton("db_connect", "Connect", icon = icon("plug")),
      uiOutput("db_table_ui")
    )
  ),
  dashboardBody(
    tabsetPanel(
      id = "dataset_tabs",
      type = "tabs"
    )
  )
)

# Server Logic
server <- function(input, output, session) {
  # Reactive values
  datasets <- reactiveValues()
  db_conn <- reactiveVal(NULL)
  
  # File handling logic - fixed
  observeEvent(input$files, {
    req(input$files)
    for(i in seq_along(input$files$name)){
      file <- input$files$datapath[i]
      name <- tools::file_path_sans_ext(input$files$name[i])
      ext <- tools::file_ext(input$files$name[i])
      
      tryCatch({
        data <- if(ext == "csv"){
          fread(file)
        } else if(ext %in% c("xls", "xlsx")){
          as.data.table(read_excel(file))
        }
        
        datasets[[name]] <- data
        appendTab("dataset_tabs", tabPanel(
          title = name,
          DTOutput(paste0("table_", name))
        ))
        output[[paste0("table_", name)]] <- renderDT({
          datatable(
            datasets[[name]],
            options = list(
              pageLength = input$page_size,
              processing = TRUE,
              serverSide = TRUE,
              deferRender = TRUE,
              scrollX = TRUE,
              scrollY = "600px",
              scroller = TRUE
            )
          )
        })
      }, error = function(e) {
        showNotification(paste("Error loading", input$files$name[i], ":", e$message), 
                         type = "error")
      })
    }
  })
  
  # Database connection logic
  observeEvent(input$db_connect, {
    req(input$db_type, input$db_name)
    
    tryCatch({
      conn <- switch(input$db_type,
                     "MySQL" = dbConnect(
                       MySQL(),
                       host = input$db_host,
                       port = input$db_port,
                       user = input$db_user,
                       password = input$db_pass,
                       dbname = input$db_name
                     ),
                     "PostgreSQL" = dbConnect(
                       Postgres(),
                       host = input$db_host,
                       port = input$db_port,
                       user = input$db_user,
                       password = input$db_pass,
                       dbname = input$db_name
                     ),
                     "SQLite" = dbConnect(
                       SQLite(),
                       dbname = input$db_name
                     ),
                     "SQL Server" = dbConnect(
                       odbc::odbc(),
                       driver = input$db_driver,
                       server = input$db_host,
                       database = input$db_name,
                       uid = input$db_user,
                       pwd = input$db_pass,
                       port = input$db_port
                     ),
                     "Azure SQL" = dbConnect(
                       odbc::odbc(),
                       driver = input$db_driver,
                       server = input$db_host,
                       database = input$db_name,
                       uid = input$db_user,
                       pwd = input$db_pass,
                       port = input$db_port,
                       Encrypt = "Yes",
                       TrustServerCertificate = "No"
                     ),
                     "Other (ODBC)" = dbConnect(
                       odbc::odbc(),
                       dsn = input$db_dsn,
                       driver = input$db_driver,
                       uid = input$db_user,
                       pwd = input$db_pass
                     )
      )
      
      db_conn(conn)
      showNotification("Connected successfully!", type = "message")
    }, error = function(e) {
      showNotification(paste("Connection failed:", e$message), type = "error")
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
      
      datasets[[input$db_table]] <- as.data.table(data)
      appendTab("dataset_tabs", tabPanel(
        title = input$db_table,
        DTOutput(paste0("table_", input$db_table))
      ))
      output[[paste0("table_", input$db_table)]] <- renderDT({
        datatable(
          datasets[[input$db_table]],
          options = list(
            pageLength = input$page_size,
            processing = TRUE,
            serverSide = TRUE,
            deferRender = TRUE,
            scrollX = TRUE,
            scrollY = "600px",
            scroller = TRUE
          )
        )
      })
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
}

# Run the application
shinyApp(ui = ui, server = server)
