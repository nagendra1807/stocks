library(RPostgreSQL)

Top500<-read.csv("C:/Users/Nagendra.B/Downloads/ind_nifty500list.csv")
Top500_ID<-as.character(Top500$Symbol)

ssnames<-read.csv("NSE-Stock-LIST-1411-Stocks-Generated-on-25may2017.csv")
ssnames$Sector<-as.character(ssnames$Sector)
ssnames$Sector[which(is.na(ssnames$Sector))]<- "others"

from<-"2016/01/01"
to<-Sys.Date()

getnsedata<-function(from,to){
  # Check whether the directory exists or not, if not exist create a directory named as Zipfiles in the current working directory.
  if(dir.exists(paste(getwd(),"/ZipFiles",sep = ""))){
    directoryPath<-paste(getwd(),"/ZipFiles/",sep = "")
    directoryPath_ind<-paste(getwd(),"/IndicesFiles/",sep = "")
  }else{
    directoryPath<-paste(dir.create(paste(getwd(),"/ZipFiles",sep = "")),"/",sep = "")#"D:/Stocks/Test Data/ZipFiles/"
    directoryPath_ind<-paste(dir.create(paste(getwd(),"/IndicesFiles",sep = "")),"/",sep = "")
  }
  # Getting the seq dates from "fromdate" to "todate"
  date_vector<-seq(as.Date(from),as.Date(to) , "days")
  # For loop for fetching day by day stocks data
  for(i in 1:length(date_vector)){
    # Checking whether the day was saturday or sunday
    if(weekdays(date_vector[i]) == "Saturday" | weekdays(date_vector[i]) == "Sunday"){
      print(paste("Weakend : ",weekdays(date_vector[i]),sep = ""))
    }else{
      # Extract Month;day;year,form the given date
      month<-toupper(format(as.Date(date_vector[i]), "%b"))
      day<-format(as.Date(date_vector[i]), "%d")
      year<-format(as.Date(date_vector[i]), "%Y")
      # URL 
      url<-paste("https://www.nseindia.com/content/historical/EQUITIES/",year,"/",month,"/cm",day,month,year,"bhav.csv.zip",sep = "")
      urlind<-paste("https://www.nseindia.com/content/indices/ind_close_all_",format(as.Date(date_vector[i]),"%d%m%Y"),".csv",sep = "")
      # Get the filename from the url
      filename<-basename(url)
      filenameind<-basename(urlind)
      # assaign the filepath to filename variable
      filename<-paste(directoryPath,filename,sep = "")
      filenameind<-paste(directoryPath_ind,filenameind,sep = "")
      # Check whether the directory exists or not, if not exist create a directory named as Csvfiles in the current working directory.
      if(dir.exists(paste(getwd(),"/CsvFiles",sep = ""))){
        exdir <- paste(getwd(),"/CsvFiles",sep = "")#"D:/Stocks/Test Data/CsvFiles"
      }else{
        exdir <- dir.create(paste(getwd(),"/CsvFiles",sep = ""))#"D:/Stocks/Test Data/CsvFiles"
      }
      # Exception handling block, while downloading the data from url or if the url returns 
      tryCatch({
        download.file(url,filename,quiet=T)
        download.file(urlind,filenameind)
        unzip(zipfile= filename,exdir = exdir)
      },error=function(e){cat("ERROR :",conditionMessage(e), "\n")},
      warning = function(w) {cat("Warning : ",conditionMessage(w),"\n")})
    }
  }
}

getnsedata(from,to)
#******************************************************#
#               Reading Stocks data                    #
#******************************************************#
read_all_stock_files<-function(){
  temp <- list.files(path = "CsvFiles",pattern = "*.CSV",full.names = T,ignore.case = T)
  if(length(temp)>0){
    S_Data<-data.frame()
    for(i in 1:length(temp)){
      S_Data1<-read.csv(temp[i])
      print(i)
      S_Data1<-S_Data1[!(is.na(S_Data1$SERIES)),]
      S_Data1<-S_Data1[S_Data1$SERIES == "EQ",]
      S_Data1$X<-NULL
      S_Data1$TIMESTAMP<-as.Date(as.character(S_Data1$TIMESTAMP),"%d-%b-%Y")
      S_Data1$SECTOR<-"others"
      S_Data1$Split<-NA
      S_Data1$Bonus<-NA
      S_Data1$Split_Val<-NA
      S_Data1$Bonus_Val<-NA
      S_Data<-rbind(S_Data,S_Data1)
      file.remove(temp[i])
    }
    return(S_Data)
  }
}
#S_Data<-read_all_stock_files()
SymbId<-as.character(S_Data$SYMBOL)
SymbId<-unique(SymbId)

for(i in 1:nrow(ssnames)){
  val<-which(as.character(ssnames$Symbol[i]) == as.character(S_Data$SYMBOL))
  S_Data$SECTOR[val]<-as.character(ssnames$Sector[i])
}

CA_Data<-function(){
  
  if(nrow(Splitdata)>0){
    for(i in 1:nrow(Splitdata)){
      var<-which(as.character(Splitdata$Symbol[i]) == as.character(S_Data$SYMBOL) & Splitdata$Ex.Date[i] == S_Data$TIMESTAMP)
      S_Data$Split[var]<-"SS"
      S_Data$Split_Val[var]<-Splitdata$Split[i]
    }
  }
  if(nrow(Bonusdata)>0){
    for(i in 1:nrow(Bonusdata)){
      var<-which(as.character(Bonusdata$Symbol[i]) == as.character(S_Data$SYMBOL) & Bonusdata$Ex.Date[i] == S_Data$TIMESTAMP)
      S_Data$Bonus[var]<-"XB"
      S_Data$Bonus_Val[var]<-Bonusdata$Bonus[i]
    }
  }
  return(S_Data)
  #stockdb(SymbId,S_Data,"teststock",SS500_ID)
}
#S_Data<-CA_Data()
#*****************************************************#
########          Adjusted calculation         ########
#*****************************************************#
stockdb<-function(codes,data,tablename,topstocks){
  #drv <- dbDriver("PostgreSQL")
  #con <- dbConnect(drv, dbname = "StockDB",
  #                 host = "192.168.2.103", port = 5432,
  #                user = "postgres", password = "postgres")
  for(i in 1:length(codes)){
    filterdata<-data[which(codes[i] == data$SYMBOL),]
    val<-which(filterdata$Split == "SS")
    if(length(val)>0){
      if(dbExistsTable(con,tablename)) {
        Query<-paste('SELECT "SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TIMESTAMP","TOTALTRADES","ISIN","SECTOR","Split","Bonus","Split_Val","Bonus_Val" FROM ',tablename,' WHERE "SYMBOL" = ',"'",codes[i],"'",sep = "")
        filterdatadb<-dbGetQuery(con,Query)
        filterdata<-rbind(filterdatadb,filterdata)
        print("Binded")
        Query<-paste('DELETE FROM ',tablename,' WHERE "SYMBOL" = ',"'",codes[i],"'",sep = "")
        dbGetQuery(con,Query)
        print("Deleted")
      }
    }
    filterdata<-filterdata[order(filterdata$TIMESTAMP,decreasing = T),]
    val<-which(filterdata$Split == "SS")
    filterdata$SS<-1
    if(length(val) ==1){
      if(nrow(filterdata) != val){
        filterdata$SS[(val+1):nrow(filterdata)]<-filterdata$Split_Val[val]
      }
    }else if(length(val) >1){
      for(k in 1:length(val)){
        if(nrow(filterdata) != val[k]){
          filterdata$SS[(val[k]+1):nrow(filterdata)]<-(filterdata$Split_Val[val])*filterdata$SS[val[k]]
        }
      }
    }
    val<-which(filterdata$Bonus == "XB")
    if(length(val)>0){
      if(dbExistsTable(con,tablename)) {
        Query<-paste('SELECT "SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TIMESTAMP","TOTALTRADES","ISIN","SECTOR","Split","Bonus","Split_Val","Bonus_Val","SS" FROM ',tablename,' WHERE "SYMBOL" = ',"'",codes[i],"'",sep = "")
        filterdatadb<-dbGetQuery(con,Query)
        filterdata<-rbind(filterdatadb,filterdata)
        print("Binded")
        Query<-paste('DELETE FROM ',tablename,' WHERE "SYMBOL" = ',"'",codes[i],"'",sep = "")
        dbGetQuery(con,Query)
        print("Deleted")
      }
    }
    filterdata<-filterdata[order(filterdata$TIMESTAMP,decreasing = T),]
    val<-which(filterdata$Bonus == "XB")
    filterdata$XB<-1
    if(length(val) ==1){
      print("--------print(nrow(filterdata))----------")
      print(val)
      print(nrow(filterdata))
      print(nrow(filterdata) !=val)
      print("------------------")
      if(nrow(filterdata) != val){
        filterdata$XB[(val+1):nrow(filterdata)]<-filterdata$Bonus_Val[val]
      }
    }else if(length(val) >1){
      for(k in 1:length(val)){
        if(nrow(filterdata) != val[k]){
          filterdata$XB[(val[k]+1):nrow(filterdata)]<-(filterdata$Bonus_Val[val])*filterdata$XB[val[k]]
        }
      }
    }
    filterdata$Adj_Open<-filterdata$OPEN*filterdata$SS*filterdata$XB
    filterdata$Adj_High<-filterdata$HIGH*filterdata$SS*filterdata$XB
    filterdata$Adj_Low<-filterdata$LOW*filterdata$SS*filterdata$XB
    filterdata$Adj_Close<-filterdata$CLOSE*filterdata$SS*filterdata$XB
    if(length(which(codes[i] == topstocks))==1){
      filterdata$Top500<-"Y"
    }else{
      filterdata$Top500<-"N"
    }
    
    #names(filterdata)
    #filterdata<-filterdata[,c()]
    filename<-paste("D:/Stocks/Test Data/DBData/Stock_",codes[i],".csv")
    write.csv(filterdata,filename,row.names = F)
    dbWriteTable(con,tablename, filterdata,append=TRUE, row.names=FALSE)
  }
  print("Loaded Successfully")
  #dbDisconnect(con)
  #dbUnloadDriver(drv)
}