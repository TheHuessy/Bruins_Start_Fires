library(DBI)
library(RPostgreSQL)
#Connect to Postgres
pw <- "write!2017!dapa"
drv <- dbDriver("PostgreSQL")
#Connect to boston analytics db
con <- dbConnect(drv,
                 dbname ="boston_analytics",
                 host = "boston-analytics-production.c3e0ztsewyar.us-east-1.rds.amazonaws.com", port = 6666,
                 user = "data_analyst_write_user", password = pw)

#Pull all fire department calls
FCalls <- dbGetQuery(con, "SELECT *
                     FROM internal_data.cad_agency_event
                     WHERE ag_id = 'BFD'")
#add a date stamp
FCalls$DISP.DATE <- as.Date(as.POSIXct(FCalls$ad_sec, origin="1970-01-01"))

#Bring in and clean Bruins playoff schedules
#SOURCE:https://www.hockey-reference.com/teams/BOS/2015_games.html

S.1415 <- read.csv("H:/My Documents/Side Projects/Bruins Start Fires/2014-2015Season.csv", stringsAsFactors = FALSE)
S.1516 <- read.csv("H:/My Documents/Side Projects/Bruins Start Fires/2015-2016Season.csv", stringsAsFactors = FALSE)
S.1617 <- read.csv("H:/My Documents/Side Projects/Bruins Start Fires/2016-2017Season.csv", stringsAsFactors = FALSE)
S.1718 <- read.csv("H:/My Documents/Side Projects/Bruins Start Fires/2017-2018Season.csv", stringsAsFactors = FALSE)
P.1617 <- read.csv("H:/My Documents/Side Projects/Bruins Start Fires/2016-2017Playoff.csv", stringsAsFactors = FALSE)
P.1718 <- read.csv("H:/My Documents/Side Projects/Bruins Start Fires/2017-2018Playoff.csv", stringsAsFactors = FALSE)

############################
#       2014-2015          #
############################

#Change date to date stamp
S.1415$Date <- as.Date(S.1415$Date)
#Pull out columns we don't need
S.1415 <- S.1415[,2:7]
#Add game decision point lead (won by X pts or lost by x pts)
#GF is goals for bruins (goals for)
#GA is goals for the other guys (goals against)
S.1415$Dec.Pts <- S.1415$GF-S.1415$GA
#Clean up home/away variable
S.1415$HOME <- "HOME"
S.1415$HOME[which(S.1415$X == "@")] <- "AWAY"
#Add whether or not the data is from the season or from playoffs
S.1415$TYPE <- "SEASON"

############################
#       2015-2016          #
############################

#Change date to date stamp
S.1516$Date <- as.Date(S.1516$Date)
#Pull out columns we don't need
S.1516 <- S.1516[,2:7]
#Add game decision point lead (won by X pts or lost by x pts)
#GF is goals for bruins (goals for)
#GA is goals for the other guys (goals against)
S.1516$Dec.Pts <- S.1516$GF-S.1516$GA
#Clean up home/away variable
S.1516$HOME <- "HOME"
S.1516$HOME[which(S.1516$X == "@")] <- "AWAY"
#Add whether or not the data is from the season or from playoffs
S.1516$TYPE <- "SEASON"

############################
#       2016-2017          #
############################

#Change date to date stamp
S.1617$Date <- as.Date(S.1617$Date)
#Pull out columns we don't need
S.1617 <- S.1617[,2:7]
#Add game decision point lead (won by X pts or lost by x pts)
#GF is goals for bruins (goals for)
#GA is goals for the other guys (goals against)
S.1617$Dec.Pts <- S.1617$GF-S.1617$GA
#Clean up home/away variable
S.1617$HOME <- "HOME"
S.1617$HOME[which(S.1617$X == "@")] <- "AWAY"
#Add whether or not the data is from the season or from playoffs
S.1617$TYPE <- "SEASON"


############################
#       2017-2018          #
############################

#Change date to date stamp
S.1718$Date <- as.Date(S.1718$Date)
#Pull out columns we don't need
S.1718 <- S.1718[,2:7]
#Add game decision point lead (won by X pts or lost by x pts)
#GF is goals for bruins (goals for)
#GA is goals for the other guys (goals against)
S.1718$Dec.Pts <- S.1718$GF-S.1718$GA
#Clean up home/away variable
S.1718$HOME <- "HOME"
S.1718$HOME[which(S.1718$X == "@")] <- "AWAY"
#Add whether or not the data is from the season or from playoffs
S.1718$TYPE <- "SEASON"

############################
#        PLAYOFFS          #
############################
#       2016-2017          #
############################

#Change date to date stamp
P.1617$Date <- as.Date(P.1617$Date)
#Pull out columns we don't need
P.1617 <- P.1617[,2:7]
#Add game decision point lead (won by X pts or lost by x pts)
#GF is goals for bruins (goals for)
#GA is goals for the other guys (goals against)
P.1617$Dec.Pts <- P.1617$GF-P.1617$GA
#Clean up home/away variable
P.1617$HOME <- "HOME"
P.1617$HOME[which(P.1617$X == "@")] <- "AWAY"
#Add whether or not the data is from the PLAYOFF or from playoffs
P.1617$TYPE <- "PLAYOFF"


############################
#       2017-2018          #
############################

#Change date to date stamp
P.1718$Date <- as.Date(P.1718$Date)
#Pull out columns we don't need
P.1718 <- P.1718[,2:7]
#Add game decision point lead (won by X pts or lost by x pts)
#GF is goals for bruins (goals for)
#GA is goals for the other guys (goals against)
P.1718$Dec.Pts <- P.1718$GF-P.1718$GA
#Clean up home/away variable
P.1718$HOME <- "HOME"
P.1718$HOME[which(P.1718$X == "@")] <- "AWAY"
#Add whether or not the data is from the PLAYOFF or from playoffs
P.1718$TYPE <- "PLAYOFF"


Bruins <- rbind(S.1415, S.1516, S.1617, S.1718, P.1617, P.1718)
Bruins <- Bruins[order(Bruins$Date),]


Bruins$FCALLS <- 0
Bruins$MEDCALLS <- 0
Bruins$JUMPER <- 0
Bruins$OTHERS <- 0
for (i in Bruins$Date){
  fc <- nrow(FCalls[which(FCalls$DISP.DATE == i),])
  mc <- nrow(FCalls[which(FCalls$DISP.DATE == i & FCalls$tycod == "MEDICAL"),])
  jc <- nrow(FCalls[which(FCalls$DISP.DATE == i & FCalls$tycod == "JUMPER"),])
  oc <- nrow(FCalls[which(FCalls$DISP.DATE == i & FCalls$tycod != "MEDICAL" & FCalls$tycod != "JUMPER"),])
  
  Bruins$FCALLS[which(Bruins$Date == i)] <- fc
  Bruins$MEDCALLS[which(Bruins$Date == i)] <- mc
  Bruins$JUMPER[which(Bruins$Date == i)] <- jc
  Bruins$OTHERS[which(Bruins$Date == i)] <- oc
  print(paste("Finished", which(Bruins$Date == i), "of", length(Bruins$Date)))
}



winlose.Tot <- lm(data = Bruins, formula = Bruins$FCALLS ~ factor(Bruins$X.1) + abs(Bruins$Dec.Pts) + factor(Bruins$HOME))
summary(winlose.Tot)

winlose.J <- lm(data = Bruins, formula = Bruins$JUMPER ~ factor(Bruins$X.1) + abs(Bruins$Dec.Pts) + factor(Bruins$HOME))
summary(winlose.J)

winlose.MED <- lm(data = Bruins, formula = Bruins$MEDCALLS ~ factor(Bruins$X.1) + abs(Bruins$Dec.Pts) + factor(Bruins$HOME))
summary(winlose.MED)
