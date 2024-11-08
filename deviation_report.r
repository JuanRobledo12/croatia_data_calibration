#target iso code  
 iso_code3 <- "HRV"
#define year of comparison and reference primary id of simulation
 refy <- 2015
 ref_primary_id <- 0

#load mapping table 
 mapping <- read.csv("/source_files/mapping.csv"))

#load raw simulation
 slt <-  read.csv("/simulations_output/sisepuede_results_sisepuede_run_2024.csv")

#vs <- colnames(slt) 
#slt[slt$time_period==0 & slt$primary_id==0, subset(vs,grepl("co2e_co2_agrc",vs)==TRUE)]

#estimate emission totals for initial year 
 slt$Year <- slt$time_period + 2015 
 for (i in 1:nrow(mapping))
 {
   # i<- 1
    vars <- unlist(strsplit(mapping$Vars[i],":"))
    if (length(vars)>1) {
    mapping$simulation[i] <- as.numeric(rowSums(slt[slt$primary_id==ref_primary_id &  slt$Year==refy,vars]))
    } else {
     mapping$simulation[i] <- as.numeric(slt[slt$primary_id==ref_primary_id &  slt$Year==refy,vars])   
    }
  }

#now load edgar and compare
edgar <- read.csv("source_files/CSC-GHG_emissions-April2024_to_calibrate.csv")
edgar <- subset(edgar,Code==iso_code3)
edgar$Edgar_Class<- paste(edgar$CSC.Subsector,edgar$Gas,sep=":")

#melt edgar data 
library(data.table)
id_varsEd <- c("Edgar_Class")
measure.vars_Ed <- subset(colnames(edgar),grepl("X",colnames(edgar))==TRUE)
edgar <- data.table(edgar)
edgar <- melt(edgar, id.vars = id_varsEd, measure.vars =measure.vars_Ed)
edgar <- data.frame(edgar)
edgar$Year <- as.numeric(gsub("X","",edgar$variable))
edgar$Edgar_value <- edgar$value
edgar$value <- NULL 
edgar <- edgar[edgar$Year==refy,c("Edgar_Class","Edgar_value")]

#merge both 
#report 1
 report_1 <- aggregate(list(simulation=mapping$simulation),list(Subsector=mapping$Subsector,Edgar_Class=mapping$Edgar_Class),sum)
 report_1 <- merge(report_1,edgar,by="Edgar_Class",all.x=TRUE)
 report_1$diff <- (report_1$simulation-report_1$Edgar_value)/report_1$Edgar_value
 report_1$Year <- refy
 write.csv(report_1, "/scaled_results/detailed_diff_report.csv")

#report 2 
 report_2 <- aggregate(list(simulation=report_1$simulation,Edgar_value=report_1$Edgar_value),list(Subsector=report_1$Subsector),sum,na.rm=TRUE)
 report_2$diff <- (report_2$simulation-report_2$Edgar_value)/report_2$Edgar_value
 report_2$Year <- refy
 write.csv(report_2, "/scaled_results/sector_diff_report.csv")