

# require(tidyverse)
require(readxl)
require(openxlsx)
require(lubridate)

print('
buildcolnames
excel_tablerange
excel_cols
excel_meta
readtable_excel
schemaed_readtable_excel
'
)

buildcolnames <- function(dataframe) {
  colnames(dataframe) <- colnames(dataframe) %>% make.names() %>% tolower %>% str_replace_all('\\.', '_')
  return(dataframe)
}

excel_tablerange <- function(excelfile, sheet, tableName) {
  wb <- loadWorkbook(excelfile)
  tables <- getTables(wb = wb, sheet=sheet)
  target_readrange <- names(tables[tables == tableName])
  return(target_readrange)
}



excel_meta <- function(excelfile) {
  
  listsheets <- excel_sheets(excelfile)
  open_wb <- loadWorkbook(excelfile)
  
  metalog <- matrix(nrow = 0, ncol = 2)
  
  for (sheet in listsheets) {
    tables_list <- getTables(open_wb, sheet)
    if (length(tables_list) > 0) {
      sheetmeta <- cbind(sheet, tables_list)
      metalog <- rbind(metalog, sheetmeta)
    }
  }
  print(metalog)
}


excel_cols <- function(excelfile, sheet, tableName) {
  targetrange <- excel_tablerange(excelfile, sheet=sheet, tableName)
  # print(targetrange)
  digits_before_colon <- str_extract(targetrange, "[0-9]+(?=:)")  # Capture numeric part before colon
  header_range <- str_replace(targetrange, "[0-9]+$", digits_before_colon)  # Capture numeric part before colon
  # print(header_range)
  header_only = read_excel(excelfile, sheet=sheet, range = header_range)
  return(names(header_only))
}


readtable_excel <- function(excelfile, sheet, tableName) {
  targetrange <- excel_tablerange(excelfile, sheet=sheet, tableName)
  loadtable = read_excel(excelfile, sheet=sheet, range = targetrange, col_types = 'text')
  
  print('raw loading')
  glimpse(loadtable)
  loadtable <- buildcolnames(loadtable)
  
  print('build colnames')
  glimpse(loadtable)
  
  return(loadtable)
}




# # testers
# list.files('db')
# excelfile = 'db/spendlog.xlsx'
# excel_meta(excelfile)
# 
# # With That i can then specify which sheet tablename to take
# excelfile = excelfile
# sheet = 'abc'
# tableName = 'Table135'

# test minora functions
# excel_tablerange(excelfile, sheet=sheet, tableName)
# excel_cols(excelfile, sheet=sheet, tableName)

# test major function
# loadtable <-readtable_excel(excelfile, sheet=sheet, tableName)
# glimpse(loadtable)

# schema = list(
#  "date" = 'date',
#  "amt" = 'num'
# )

# # testers


# excelfile <- file.path(schema, item)
# 
# fixedschema =list(
#   "date" = 'date',
#   "amt" = 'num',
#   "food" = 'num',
#   "feed_others" = 'num',
#   "opexp" = 'num',
#   "play" = 'num',
#   "public_transport" = 'num',
#   "taxi_transport" = 'num',
#   "endeavours" = 'num',
#   "wastemoney" = 'num',
#   "investments" = 'num',
#   "owe_to_people" = 'num',
#   "wastemoney" = 'num',
#   "owe_to_people" = 'num',
#   "lendings" = 'num',
#   "prepaid_expenses" = 'num'
# )

# excelfile <- '/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/DB_Accounting/Finance - Log.xlsx'
# sheet = 'log'
# tableName ='Table1'
# i = 2
# schema = fixedschema
# targetcol = names(fixedschema)[i]
# dates <- as.Date(as.numeric(hehe$date[140]), origin = "1899-12-30")


# excelfile
# sheet='log'
# tableName = 'Table1'
# schema = fixedschema
# targetcol = 'play'

schemaed_readtable_excel <- function(excelfile, sheet=sheet, tableName, schema) {
  
  loadtable <-readtable_excel(excelfile, sheet=sheet, tableName)
  
  for (targetcol in names(schema)) {
    coltyping = schema[targetcol]
    print(paste('doing', targetcol))
    
    if (!targetcol %in% names(loadtable)) {
      warning(sprintf('%s column not in this table', targetcol))
    } else {
      if (coltyping == 'date') {
        showcols <- loadtable %>% 
          filter(!is.na(.data[[targetcol]]))
        datesample <- showcols[[targetcol]][1]
        
        
        print(showcols)
                 
        if (str_detect(datesample, '\\-')) {type = 'a'}
        if (!is.na(as.numeric(datesample))) {type = 'b'}
        
        if (type=='a') {
          loadtable[[targetcol]] <- ymd(loadtable[[targetcol]])
        }
          
        if (type=='b') {
        loadtable[[targetcol]] <- loadtable[[targetcol]] %>% as.numeric() %>% as.Date(origin = "1899-12-30")
        }
      
      }
      if (coltyping == 'num') {loadtable[[targetcol]] <- as.numeric(loadtable[[targetcol]])}
    }
  }
  print('type casted dataframe')
  glimpse(loadtable)
  
  return(loadtable)
  
}

#############
# example runs
#############
# excelfile = 'db/spendlog.xlsx'
# hehe <- schemaed_readtable_excel(excelfile, sheet='abc', tableName = 'Table135',
#                          schema = list(
#                            "date_of_spend" = 'date',
#                            "spend_amt" = 'num'
#                          ))
# 
# 
# excelfile <- '/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/DB_Accounting/Finance - Log.xlsx'
# 
# excel_meta(excelfile)
# financelog <- schemaed_readtable_excel(excelfile, sheet='Log', tableName = 'financelog',
#                                  schema = list(
#                                    "date" = 'date',
#                                    "amt" = 'num',
#                                    "food" = 'num',
#                                    "feed_others" = 'num',
#                                    "opexp" = 'num',
#                                    "play" = 'num',
#                                    "public_transport" = 'num',
#                                    "taxi_transport" = 'num',
#                                    "endeavours" = 'num',
#                                    "wastemoney" = 'num',
#                                    "investments" = 'num',
#                                    "owe_to_people" = 'num',
#                                    "wastemoney" = 'num',
#                                    "owe_to_people" = 'num',
#                                    "lendings" = 'num',
#                                    "prepaid_expenses" = 'num'
#                                  )
# )








