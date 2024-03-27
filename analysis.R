library(tidyverse)
library(RPostgreSQL)
library(DBI)

demolitions <-read_csv(URLencode("https://data.cityofnewyork.us/resource/ipu4-2q9a.csv?$query=SELECT * WHERE job_type='DM' LIMIT 10000000"))

demo_now <- read_csv(URLencode("https://data.cityofnewyork.us/resource/rbx6-tga4.csv?$query= SELECT *"))

filings <- read_csv(URLencode("https://data.cityofnewyork.us/resource/ic3t-wcy2.csv?$query=SELECT * WHERE job_type='DM' LIMIT 10000000"))

dob_now <- read_csv(URLencode("https://data.cityofnewyork.us/resource/w9ak-ipjd.csv?$query=SELECT * WHERE job_type='Full Demolition' LIMIT 10000000"))

pluto <- read_csv("https://data.cityofnewyork.us/resource/64uk-42ks.csv?$limit=10000000")

demo_clean <- demolitions %>% 
  mutate(bbl = paste0(str_sub(community_board, start = 1, end = 1), block, str_sub(lot, start = 2, end = 5)),
         filing_date = mdy(filing_date),
         job_date = mdy(job_start_date)
         ) %>% 
  group_by(bbl) %>% 
  summarize(filing = first(filing_date),
            )

jobs_clean <- filings %>% 
  transmute(bbl = paste0(str_sub(doc__, start = 2, end =2), block, str_sub(lot, start = 2, end = 5)),
         filing_date = mdy(pre__filing_date),
         job_date = mdy(fully_permitted)
  )

now_clean <- dob_now %>% 
  mutate(bbl = paste0(str_sub(commmunity_board, start = 1, end =1), "0", str_pad(block, 4, pad = "0", side = "left"), str_pad(lot, 4, pad = "0", side = "left")),
         filing_date = ymd_hms(filing_date),
         job_date = ymd(permit_issue_date)
  )

now_sum <- now_clean %>% 
  group_by(year = floor_date(filing_date, "year")) %>% 
  summarize(buildings = n())

job_sum <- jobs_clean %>% 
  group_by(year = floor_date(filing_date, "year")) %>% 
  summarize(buildings = n())

filing_clean <- bind_rows(
  jobs_clean,
  now_clean)

demo_filing <- filing_clean %>% 
  group_by(bbl) %>% 
  summarize(filing_date = max(filing_date),
            permit_date = max(job_date)) %>% 
  distinct()

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

# rs_approx_pluto <- dbGetQuery(connec, "SELECT * 
#                     FROM pluto_latest
#                     WHERE unitsres >= 6 AND yearbuilt <= 1975
#                     ")
# 
# demos <- dbGetQuery(connec, "
#                     SELECT *
#                     FROM dobjobs
#                     WHERE jobtype = 'DM'")

rs_list <- dbGetQuery(connec, "
                    SELECT *
                    FROM
                      rentstab_summary as rs
                    ")

demos_jobs <- dbGetQuery(connec, "
                    SELECT bbl, prefilingdate, fullypermitted
                    FROM
                      dobjobs
                    WHERE jobtype = 'DM'
                    ") %>% 
  rename(filingdate = prefilingdate,
         permitissuedate = fullypermitted)

demos_now <- dbGetQuery(connec, "
                    SELECT bbl, filingdate, permitissuedate
                    FROM
                      dob_now_jobs
                    WHERE jobtype = 'Full Demolition'
                    ")

nycdb_together <- bind_rows(demos_jobs, demos_now)

nycdb_rs_join <- inner_join(rs_list, nycdb_together, c("ucbbl" = "bbl")) %>% 
  group_by(ucbbl) %>% 
  summarize(filing_date = max(filingdate),
            permit_date = max(permitissuedate),
            rs_units = max(unitsstab2017),
            rs_units_old = max(unitsstab2007))

nycdb_rs_join %>% mutate(time_till_permit = permit_date - filing_date) %>% 
  summarize(avg_time = mean(time_till_permit, na.rm = T),
            med_time = median(time_till_permit, na.rm = T),
            min_time = min(time_till_permit, na.rm = T),
            max_time = max(time_till_permit, na.rm = T),
            )

nycdb_rs_sum <- nycdb_rs_join %>% 
  group_by(date = year(filing_date)) %>% 
  summarize(buildings = n(),
            units = sum(rs_units, na.rm = T))

nycdb_rs_sum_permitted <- nycdb_rs_join %>% 
  group_by(date = year(permit_date)) %>% 
  summarize(buildings = n(),
            units = sum(rs_units, na.rm = T))

nycdb_rs_sum_permitted %>% filter(date < 2024 & date >2016) %>% write_csv("nycdb_demos_summary_permitted.csv")

nycdb_rs_sum %>% filter(date < 2024 & date >2016) %>% write_csv("nycdb_demos_summary.csv")



# demos_now <- dbGetQuery(connec, "
#                     SELECT *
#                     FROM dob_now_jobs
#                     WHERE jobtype = 'Demolition'")
# 
# demos_rs_approx <- inner_join(rs_approx_pluto, demos, by = "bbl")
# 
# demos_approx_year_sum <- demos_rs_approx %>% 
#   group_by(date = floor_date(prefilingdate, unit = "year")) %>% 
#   summarize(count = n())
# 
# #nycdb is outdated!
# ggplot(demos_approx_year_sum %>%  filter(year(date) < 2024 & year(date) >2016))+
#   geom_line(mapping = aes(x = date, y = count))

################

#use demo_clean for the join

demos_rs <- inner_join(rs_list, demo_filing, by = c("ucbbl" = "bbl"))

view(inner_join(rs_list, now_clean, by = c("ucbbl" = "bbl")))
view(inner_join(rs_list, jobs_clean, by = c("ucbbl" = "bbl")))

comp_demos_rs <- demos_rs %>% filter(!is.na(permit_date))

rs_demos_sum <- demos_rs %>% 
  group_by(date = format(floor_date(permit_date, unit = "year"), "%Y-%m-%d")) %>% 
  summarize(buildings = n(),
            units = sum(unitsstab2017))

comp_rs_demos_sum <- demos_rs %>% 
  group_by(date = format(floor_date(permit_date, unit = "year"), "%Y-%m-%d")) %>% 
  summarize(buildings = n(),
            units = sum(unitsstab2017))

rs_demos_sum %>% filter(year(date) < 2024 & year(date) >2016) %>% write_csv("demos_summary.csv")

ggplot(rs_demos_sum %>%  filter(year(date) < 2024 & year(date) >2010))+
  geom_line(mapping = aes(x = date, y = buildings))+
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
