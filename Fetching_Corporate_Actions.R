corporateactions<-function(fromdate){
  if(dir.exists(paste(getwd(),"/Announcement",sep = ""))){
    directoryPath<-paste(getwd(),"/Announcement/",sep = "")
  }else{
    directoryPath<-paste(dir.create(paste(getwd(),"/Announcement",sep = "")),"/",sep = "")
  }
  if(weekdays(fromdate)=="Saturday"){
    if(length(seq(as.Date(fromdate),Sys.Date() , "days"))< 4){
      date<-max(seq(as.Date(fromdate),Sys.Date() , "days"))
    }else{
      date<-max(seq(as.Date(fromdate),Sys.Date() , "days"))-1
    }
  }else{
    date<-min(seq(as.Date(fromdate),Sys.Date() , "days"))
  }
  if(as.Date(date) == Sys.Date()){
    print("enttered")
    urls<-"https://www.nseindia.com/corporates/datafiles/CA_TODAY_SPLIT.csv"
    urlb<-"https://www.nseindia.com/corporates/datafiles/CA_TODAY_BONUS.csv"
  }else if(Sys.Date()-7 <= as.Date(date) & as.Date(date) < Sys.Date()){
    urls<-"https://www.nseindia.com/corporates/datafiles/CA_LAST_1_WEEK_SPLIT.csv"
    urlb<-"https://www.nseindia.com/corporates/datafiles/CA_LAST_1_WEEK_BONUS.csv"
  }
  filename<-"Bonus.csv"#basename(urlb)
  filename<-paste(directoryPath,filename,sep = "")
  download.file(urlb,filename)
  filename<-"Splits.csv"#basename(urls)
  filename<-paste(directoryPath,filename,sep = "")
  download.file(urls,filename)
  
}



Bonusdatafun<-function(){
  Bonusdata<-read.csv(paste(getwd(),"/Announcement/Bonus.csv",sep = ""))
  if(nrow(Bonusdata)>0){
    Bonusdata<-Bonusdata[c(1,2,5:8)]
    Bonusdata$Ex.Date<-as.Date(as.character(Bonusdata$Ex.Date),"%d-%b-%Y")
    for(i in 1:nrow(Bonusdata)){
      X<-strsplit(as.character(Bonusdata$Purpose[i]),split =":")[[1]][1]
      Y<-strsplit(as.character(Bonusdata$Purpose[i]),split =":")[[1]][2]
      Y<-strsplit(trimws(strsplit(Y,split = '/')[[1]][1]),split = " ")[[1]][1]
      X<-strsplit(strsplit(X,split = "Bonus |Bon |Bonus - |Bonus Shares In The Ratio Of |Bonus Issue ")[[1]][2],split = " ")[[1]][1]
      if(X>Y){
        Bonusdata$Bonus[i]<-(1/(as.numeric(X)+as.numeric(Y)))
      }else{
        Bonusdata$Bonus[i]<-1-(1/(as.numeric(X)+as.numeric(Y)))
      }
    }
  }
  return(Bonusdata)
}
Splitdatafun<-function(){
  Splitdata<-read.csv(paste(getwd(),"/Announcement/Splits.csv",sep = ""))
  if(nrow(Splitdata)>0){
    Splitdata<-Splitdata[c(1,2,5:8)]
    Splitdata$Ex.Date<-as.Date(as.character(Splitdata$Ex.Date),"%d-%b-%Y")
    for(i in 1:nrow(Splitdata)){
      X<-strsplit(as.character(Splitdata$Purpose[i]),split = "Rs |Rs.|Re |To Re.")[[1]]
      if(length(X)>1){
        X<-X[(length(X)-1):length(X)]
        From<-strsplit(trimws(X[1]),split = "/-| ")[[1]][1]
        To<-strsplit(trimws(X[2]),split = "/-| ")[[1]][1]
        Splitdata$Split[i]<-as.numeric(To)/as.numeric(From)
        remove(X,From,To)
      }else{
        X<-strsplit(X,split = "From|To")[[1]]
        X<-X[(length(X)-1):length(X)]
        From<-strsplit(trimws(X[1]),split = "/-| ")[[1]][1]
        X2<-as.numeric(strsplit(trimws(X[2]),split = "/-| ")[[1]])
        To<-X2[!is.na(X2)]
        Splitdata$Split[i]<-as.numeric(To)/as.numeric(From)
        remove(X2,Y,X)
      }
    }
  }
  return(Splitdata)
}










Splitdata<-read.csv("C:/Users/Nagendra.B/Downloads/CA_MORE_THAN_24_MONTHS_SPLIT.csv")
Splitdata<-Splitdata[c(1,2,5:8)]
Splitdata$Ex.Date<-as.Date(as.character(Splitdata$Ex.Date),"%d-%b-%Y")

Bonusdata<-read.csv("C:/Users/Nagendra.B/Downloads/CA_MORE_THAN_24_MONTHS_BONUS.csv")
Bonusdata<-Bonusdata[c(1,2,5:8)]
Bonusdata$Ex.Date<-as.Date(as.character(Bonusdata$Ex.Date),"%d-%b-%Y")