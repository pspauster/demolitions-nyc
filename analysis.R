library(tidyverse)
library(RPostgreSQL)
library(DBI)

demolitions <-read_csv(URLencode("https://data.cityofnewyork.us/resource/ipu4-2q9a.csv?$query=SELECT * WHERE job_type='DM' LIMIT 10000000"))

pluto <- read_csv("https://data.cityofnewyork.us/resource/64uk-42ks.csv?$limit=10000000")

demo_clean <- demolitions %>% 
  mutate(bbl = paste0(str_sub(community_board, start = 1, end = 1), block, str_sub(lot, start = 2, end = 5)),
         filing_date = mdy(filing_date),
         job_date = mdy(job_start_date)
         )
  
demo_filing <- demo_clean %>% 
  group_by(bbl) %>% 
  summarize(filing_date = min(filing_date))

demo_complete <- demo_clean %>% 
  filter(permit_status %in% c("ISSUED", "RE-ISSUED")) %>% 
  group_by(bbl) %>% 
  summarize(start_date = min(job_date))

drv <- dbDriver("PostgreSQL")
dsn_database = "nycdb"   # Specify the name of your Database
dsn_hostname = "nyc-db.cluster-custom-ckvyf7p6u5rl.us-east-1.rds.amazonaws.com"  # Specify host name e.g.:"aws-us-east-1-portal.4.dblayer.com"
dsn_port = "5432"                # Specify your port number. e.g. 98939
dsn_uid = "anon"         # Specify your username. e.g. "admin"
dsn_pwd = "tenantpower"        # Specify your password. e.g. "xxx"

tryCatch({
  drv <- dbDriver("PostgreSQL")
  print("Connecting to Database…")
  connec <- dbConnect(drv, 
                      dbname = dsn_database,
                      host = dsn_hostname, 
                      port = dsn_port,
                      user = dsn_uid, 
                      password = dsn_pwd)
  print("Database Connected!")
},
error=function(cond) {
  print("Unable to connect to Database.")
})

connec <- dbConnect(drv, 
                    dbname = dsn_database,
                    host = dsn_hostname, 
                    port = dsn_port,
                    user = dsn_uid, 
                    password = dsn_pwd)

rs_approx_pluto <- dbGetQuery(connec, "SELECT * 
                    FROM pluto_latest
                    WHERE unitsres >= 6 AND yearbuilt <= 1975
                    ")

demos <- dbGetQuery(connec, "
                    SELECT *
                    FROM dobjobs
                    WHERE jobtype = 'DM'")

rs_list <- dbGetQuery(connec, "
                    SELECT *
                    FROM
                      rentstab_summary as rs
                    ")

demos_now <- dbGetQuery(connec, "
                    SELECT *
                    FROM dob_now_jobs
                    WHERE jobtype = 'Demolition'")

demos_rs_approx <- inner_join(rs_approx_pluto, demos, by = "bbl")

demos_approx_year_sum <- demos_rs_approx %>% 
  group_by(date = floor_date(prefilingdate, unit = "year")) %>% 
  summarize(count = n())

#nycdb is outdated!
ggplot(demos_approx_year_sum %>%  filter(year(date) < 2024 & year(date) >2016))+
  geom_line(mapping = aes(x = date, y = count))

################

#use demo_clean for the join

demos_rs <- inner_join(rs_list, demo_filing, by = c("ucbbl" = "bbl"))

comp_demos_rs <-inner_join(rs_list, demo_complete, by = c("ucbbl" = "bbl"))

rs_demos_sum <- demos_rs %>% 
  group_by(date = floor_date(filing_date, unit = "year")) %>% 
  summarize(count = n(),
            units = sum(unitsstab2017))

comp_rs_demos_sum <- demos_rs %>% 
  group_by(date = floor_date(filing_date, unit = "year")) %>% 
  summarize(count = n(),
            units = sum(unitsstab2017))

rs_demos_sum %>% filter(year(date) < 2024 & year(date) >2016) %>% write_csv("demos_summary.csv")

ggplot(rs_demos_sum %>%  filter(year(date) < 2024 & year(date) >2010))+
  geom_line(mapping = aes(x = date, y = count))+
  ylim(0, 80)

ggplot(rs_demos_sum %>%  filter(year(date) < 2024 & year(date) >2010))+
  geom_line(mapping = aes(x = date, y = units))

bk_rs_demos_sum <- demos_rs %>% 
  group_by(date = floor_date(filing_date, unit = "year"), borough) %>% 
  summarize(count = n())

ggplot(bk_rs_demos_sum %>%  filter(year(date) < 2024 & year(date) >2016))+
  geom_line(mapping = aes(x = date, y = count, color = borough))+
  ylim(0, 80)

ggplot(bk_rs_demos_sum %>%  filter(year(date) < 2024 & year(date) >2016 & borough == "BROOKLYN"))+
  geom_line(mapping = aes(x = date, y = count))
