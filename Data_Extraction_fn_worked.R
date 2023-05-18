# Function for importing all packages
# Function for Data Extraction frm Hive Data Lake or from local repository which is stored from hive or combined
# author : vks varma
# Date : 24/4/2022


import_packages <- function() {
  options(stringsAsFactors = F)
  options(buildtools.check = NULL)
  
  packages_required <- c("brms", "readxl", "reshape2", "dplyr", "readr", "data.table",
                         "sqldf", "tidyr", "xlsx", "rstudioapi", "glmnet", "glinternet", 
                         "DMwR", "caret", "stringi", "stringr", "data.table", "zoo", "lubridate",
                         "openxlsx","RcppBDT","DT","foreach","iterators","doParallel","AzureKeyVault",
                         "AzureStor"
  )
  
  check <- lapply(packages_required, require, character.only = TRUE)
  if (sum(check!=T)) {
    stop('Something went wrong with libraries')
  }
  rm(check,packages_required)
}

Data_Extraction <- function(azure_path,hive_retailer,Start_date,End_date,extraction_point = 'remote',prev_data_path = '') {
  import_packages()
  ## Extracting Data Only from Hive
  Data_Extraction_Hive <- function(azure_path,hive_retailer,Start_date = (Sys.Date() - years(5)),End_date = Sys.Date(),append_prev_data = F) {
    
    # configurations setting
    configuration_setup <- function() {
      # Configuration inputs
      account_name = "npusprdatalakesta"
      vault_name = "npusdvtpoidkey"
      client_secret_name = "npus-pr-srm-trade-promo-tool-secret"
      client_id_name = "npus-pr-srm-trade-promo-tool-spn-id"
      tenant_id_name = "tenant-id"
      
      # Extracting the credentials required for setting up connection
      vault <- key_vault(url = sprintf("https://%s.vault.azure.net",vault_name), as_managed_identity = T)
      client_secret=vault$secrets$get(client_secret_name)$value
      client_id=vault$secrets$get(name =client_id_name)$value
      tenant_id=vault$secrets$get(name = tenant_id_name)$value  
      
      # Creating the token (for 1 hour) for accessing the data lake domain
      token <- AzureAuth::get_azure_token(sprintf("https://%s.dfs.core.windows.net",account_name),
                                          tenant_id, 
                                          client_id, 
                                          client_secret)
      return(list(account_name,token))
    }
    
    # Read function
    auth_read<-function(path, file ,header=T,data.table=T,quote = ""){
      p<-path
      f<-file
      
      endp<-AzureStor::adls_endpoint(sprintf("https://%s.dfs.core.windows.net/restricted-published",account_name), token=token)
      cont <- AzureStor::storage_container(endp, p)
      fname <- tempfile()
      # file.remove(paste0(tempdir(),"\\",list.files(tempdir())))
      file.remove(paste0(tempdir(),"\\",setdiff(list.files(tempdir()),list.files(tempdir(),pattern = 'rs-'))))
      AzureStor::storage_download(cont, f, fname)
      return(fread(fname,header=header,data.table=data.table,quote = quote))
    }
    
    # List retailer names
    auth_list_retailer_names<-function(path){
      p<-path
      # Establish endpoint connection
      cont<-AzureStor::storage_container(sprintf("https://%s.dfs.core.windows.net/restricted-published",account_name), token=token)
      nmes <- AzureStor::list_storage_files(container = cont, p)$name
      # Clean the retailer names - remove the URL path
      cl_nmes <- sub(pattern = p, replacement = "",x = nmes)
      # Clean the retailer names - remove RMS from the name
      cl_nmes <- sub(pattern = "RMS", replacement = "",x = cl_nmes)
      # Extract the retailer names applying strsplit
      list_retailer_names <- unique(vapply(strsplit(cl_nmes,"_"), `[`, 1, FUN.VALUE=character(1)))
      return(list_retailer_names)
    }
    
    # List file names
    auth_list_file_names<-function(path){
      p<-path
      # Establish endpoint connection
      cont<-AzureStor::storage_container(sprintf("https://%s.dfs.core.windows.net/restricted-published",account_name), token=token)
      nmes <- AzureStor::list_storage_files(container = cont, p)$name
      # Clean the retailer names - remove the URL path
      cl_nmes <- sub(pattern = p, replacement = "",x = nmes)
      
      return(cl_nmes)
    }
    
    #setting up things
    config <- configuration_setup()
    account_name <- config[[1]]
    token <- config[[2]]
    if (append_prev_data == F) {
      list_file_names <- auth_list_file_names(path=azure_path)
      hive_paths = list_file_names[list_file_names %like% paste0('^',hive_retailer,'RMS') ]
      product_catalog_path = list_file_names[list_file_names %like% 'Product_Catalog_RMS' ]
    }else{
      list_file_names <- list.files(prev_data_path)
      hive_paths = list_file_names[list_file_names %like% paste0('^',hive_retailer,'RMS') ]
      product_catalog_path = list_file_names[list_file_names %like% 'Product_Catalog_RMS' ]
    }
    
    # print(Retailer)
    print("#####----Data Prepration---####")
    # print(hive_paths)
    db_data <- data.frame()
    for (hive_path in hive_paths) {
      print(hive_path)
      if (append_prev_data == F) {
        upc_level_data <- auth_read(file=hive_path,path=azure_path,header=T,data.table=T)
      }else{
        upc_level_data <- fread(paste0(prev_data_path,'/',hive_path),header = T,data.table=T,quote = "")
      }
      upc_level_data$UPC <- as.character(upc_level_data$UPC)
      upc_level_data$ConsumerUPC <- as.character(upc_level_data$ConsumerUPC)
      upc_level_data$DateID <- as.Date(upc_level_data$DateID,format = "%m/%d/%Y")
      ##To make the weekend day as saturday
      upc_level_data$DateID <- upc_level_data$DateID - 1
      db_data <- rbindlist(list(db_data,upc_level_data))
      rm(upc_level_data)
      gc()
    }
    
    db_data = setDT(db_data)
    db_data$UPC <- as.numeric(db_data$UPC)
    db_data$ConsumerUPC <- as.numeric(db_data$ConsumerUPC)
    
    #Renaming hive columns to align with Shiloh columns
    setnames(db_data,'UPC','Product_UPC')
    setnames(db_data,'Category','CATEGORY')
    setnames(db_data,'ItemDescription','ITEM')
    setnames(db_data,'PPG','PPGName')
    setnames(db_data,'DateID','WeekEndDt')
    setnames(db_data,'SalesDollars','Dollars')
    setnames(db_data,'SalesUnits','Units')
    setnames(db_data,'Segment','PPG_Category')
    #setnames(db_data,'Brand','BRAND GROUP(C)')
    
    ##Adding Brand Group to db_data
    print(product_catalog_path)
    if (append_prev_data == F) {
      ProductMapping <- auth_read(file=product_catalog_path,path=azure_path,header=T,data.table=T,quote = "\"")
    }else{
      ProductMapping <- fread(paste0(prev_data_path,'/',product_catalog_path),header = T,data.table=T)
    }
    
    db_data_copy <- db_data
    db_data <- db_data_copy
    ProductMapping_Filtered <- unique(ProductMapping[,c('NPP CATEGORY','NPP SEGMENT',
                                                        'NPP BRAND','ITEM',
                                                        'NPP BRAND GROUP','NPP PRICE CLASS',
                                                        "NPP PACK SIZE RANGE"
                                                        )])
    db_data <- db_data %>%  mutate(ConsumerUPC = as.integer(ConsumerUPC))
    db_data <- merge(db_data,ProductMapping_Filtered,by.x = c('CATEGORY','PPG_Category','Brand','ITEM'),
                     by.y = c('NPP CATEGORY','NPP SEGMENT','NPP BRAND','ITEM'),all.x = T) #,allow.cartesian = T
    setnames(db_data,'NPP BRAND GROUP','BRAND GROUP(C)')
    db_data$year <- year(db_data$WeekEndDt)
    db_data_temp <- db_data %>% select(c(L5CustomerName,CATEGORY,`BRAND GROUP(C)`,PPGName,year,`NPP PRICE CLASS`,`NPP PACK SIZE RANGE` , Dollars, BaseDollars , BaseUnits,Units))
    
    db_data_temp <- db_data %>% group_by(L5CustomerName,PPG_Category,`BRAND GROUP(C)`,PPGName,year,`NPP PRICE CLASS`,`NPP PACK SIZE RANGE`) %>% summarise(sales = sum(Dollars,na.rm = T),units = sum(Units,na.rm = T))
    
    write.csv(db_data,"ppg_x_year.csv")
    write.xlsx(db_data_temp, 'ppg x year.xlsx')
    db_data1 <- db_data[is.na(db_data$`BRAND GROUP(C)`),]
    db_data_1 <- db_data[!is.na(db_data$`BRAND GROUP(C)`),]
    
    #Replacing NA Brands with "A/O Main"
    db_data$`BRAND GROUP(C)` <- ifelse(is.na(db_data$`BRAND GROUP(C)`)==T,
                                       "A/O Brand",(db_data$`BRAND GROUP(C)`))
    
    # 
    # if (nrow(db_data1)>0) {
      #need to commend out this later point
      # db_data2 <- db_data1[is.na(db_data1$`BRAND GROUP(C)`),]
      # db_data2$`BRAND GROUP(C)`<- NULL
      # ProductMapping_old <- fread(paste0('E:/Tiger/HiveData/','/','Product_Catalog_RMS_PrdcRef_031422.csv'),header = T,data.table=T)
      # ProductMapping_Filtered_old <- unique(ProductMapping_old[,c('NPP CATEGORY','NPP BRAND','NPP BRAND GROUP','NPP SEGMENT','ITEM')])
      # tmp1 <- merge(db_data2,ProductMapping_Filtered_old,by.x = c('CATEGORY','PPG_Category','Brand','ITEM'),
      #               by.y = c('NPP CATEGORY','NPP SEGMENT','NPP BRAND','ITEM'),all.x = T)
      # setnames(tmp1,'NPP BRAND GROUP','BRAND GROUP(C)')
      # db_data1 <- tmp1[is.na(tmp1$`BRAND GROUP(C)`),]
      # db_data_1 <- bind_rows(db_data_1,tmp1)
    # }
    # db_data <- db_data_1
    # db_data <- db_data %>% group_by(L5CustomerName,PPG_Category,Vendor,Brand) %>% fill(`BRAND GROUP(C)`,.direction = 'updown')
    
    if (nrow(db_data1)/nrow(db_data)>0.20){
      stop('Something is wrong with Brand Mapping.')
    }
    
    db_data[db_data$PPG_Category == 'CAT LITTER',]$PPG_Category <- 'LITTER'
    db_data$PPG_Category <- gsub(' ','_',db_data$PPG_Category)
    db_data$Retailer <- ip_hive_check[ip_hive_check$HiveDataName == hive_retailer,]$ShilohDataName
    
    db_data <- db_data %>% filter(WeekEndDt>= Start_date & WeekEndDt < End_date)
    
    return(db_data)
    
  }
  
  if (extraction_point == 'local') {
    db_data <- Data_Extraction_Hive(azure_path,hive_retailer,Start_date,End_date,append_prev_data = T)
  }else{
    db_data <- Data_Extraction_Hive(azure_path,hive_retailer,Start_date,End_date)
    if (prev_data_path != '') {
      db_data_prev <- Data_Extraction_Hive(azure_path,hive_retailer,Start_date,min(db_data$WeekEndDt),append_prev_data = T)
      db_data <- bind_rows(db_data,db_data_prev)
    }
  }
  return(db_data)
}

### Function Calls
# import_packages()

# Inputs Required (example)
# Retailer <- 'Total US xAOC + Pet Retail'
# azure_path = 'solutions/demand_planning/data_harmonization/output_no_walmart/'
# ip_hive_check <- fread('E:/Tiger/NPP_Sales_ModelRefresh_2022Q1/5. Others/Supported Files/RetailerNameMapping.csv')
# hive_retailer <- ip_hive_check[ip_hive_check$ShilohDataName == Retailer,]$HiveDataName
# Start_date <- '2020-04-01'
# End_date <- '2022-04-01'
# prev_hive_data_path <- 'E:/Tiger/HiveData/'


## Suppose Wanted to import Data from Remote Data Lake Only
# db_data <- Data_Extraction(azure_path,hive_retailer,Start_date,End_date,extraction_point = 'remote')

## Suppose Wanted to import Data from Local Repository Only
# db_data <- Data_Extraction(azure_path,hive_retailer,Start_date,End_date,extraction_point = 'local',prev_data_path = prev_hive_data_path)

## Suppose Wanted to import Data from Both places combinely considering first 7 weeks from prev data
# db_data <- Data_Extraction(azure_path,hive_retailer,Start_date,End_date,extraction_point = 'remote',prev_data_path = prev_hive_data_path)
