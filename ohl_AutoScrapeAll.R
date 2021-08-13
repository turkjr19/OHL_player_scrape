library(tidyverse)
library(janitor)
library(lubridate)
library(stringr)
library(RJSONIO)
library(jsonlite)
library(RODBC)
library(DBI)
library(odbc)

## This script scrapes OHL website for lineups and events from gameIDs ##
# script is designed to be run daily with a windows batch file in
# task scheduler.  we need the logic below to determine if there is a game to scrape

# Modify this script to pull csv with season IDS from github

# pull in season IDs from github
# read in the csv from my Github for specific season
games <- read_csv("https://raw.githubusercontent.com/turkjr19/OHL_scrape/main/ohl_2021_preSeasonGameIDs.csv")


## LOGIC WITH ERROR MESSAGE ##
# get yesterday's date so we can scrape just those games
yesterday <- tibble(
   value = today()-1
 )

 # filter out games tibble by scrape_date game_date value
yesterday_games <- games %>%
  filter(date_played == yesterday$value)

# logic to check if there were games yesterday
# if there were no games stop and print
# if there were games continue on with lineups and events
if(nrow(yesterday_games) == 0){
  stop("no games yesterday - nothing to scrape")
}

# get Lineups and events
# read in the ohl game ids to iterate through
gameIDs <- yesterday_games

# get the number of games
pb_count <- nrow(gameIDs)

#set random system sleep variable
tmsleep <- sample(5:10,1)
# set progress bar
pb <- txtProgressBar(min = 0, max = pb_count, style = 3)

# connect to database
conn1 <- odbcConnect("SQLServer_DSN")

# iterative process to read each game id and get lineups
for (i in 1:nrow(gameIDs)) {
  game_ID <- (gameIDs[i, 1])
  game_id <- as.numeric(gameIDs[i,1])
  Sys.sleep(tmsleep)
  home_team <- NULL
  visitor_team <- NULL
  skater_lineups <- NULL
  
  str3 <- "https://cluster.leaguestat.com/feed/index.php?feed=gc&key=2976319eb44abe94&client_code=ohl&game_id="
  str4 <- "&lang_code=en&fmt=json&tab=gamesummary"
  game_url2 <- paste0(str3,game_ID,str4)
  url2 <- game_url2
  
  # Import pxp data from JSON
  # use jsonlite::fromJSON to handle NULL values
  json_data2 <- jsonlite::fromJSON(url2, simplifyDataFrame = TRUE)
  
  # create a tibble for hometeam lineup
  home_team <- as_tibble(json_data2[["GC"]][["Gamesummary"]][["home_team_lineup"]][["players"]]) %>% 
    mutate(game_id = game_id,
           home_away = "home") %>% 
    select(game_id, home_away, first_name, last_name)
  
  # create a tibble for visiting lineup
  visitor_team <- as_tibble(json_data2[["GC"]][["Gamesummary"]][["visitor_team_lineup"]][["players"]]) %>% 
    mutate(game_id = game_id,
           home_away = "visitor") %>%
    select(game_id, home_away, first_name, last_name)
  
  # create a tibble for game lineups
  skater_lineups <- bind_rows(home_team, visitor_team)
  
  # save lineups to database
  # sqlSave(conn1,
  #         skater_lineups,
  #         tablename = "ohl_game_lineups",
  #         rownames = F,
  #         append = T)
  
  setTxtProgressBar(pb, i)
  
}
  
#set random system sleep variable
tmsleep <- sample(5:10,1)
# set progress bar
pb <- txtProgressBar(min = 0, max = pb_count, style = 3)


# iterative process to read each game id and get events
  for (i in 1:nrow(gameIDs)) {
    game_ID <- (gameIDs[i, 1])
    game_id <- as.numeric(gameIDs[i,1])
    Sys.sleep(tmsleep)
    faceoffs <- NULL
    shots <- NULL
    goals <- NULL
    plus <- NULL
    minus <- NULL
    
    str5 <- "https://cluster.leaguestat.com/feed/index.php?feed=gc&key=2976319eb44abe94&client_code=ohl&game_id="
    str6 <- "&lang_code=en&fmt=json&tab=pxpverbose"
    game_url3 <- paste0(str5,game_ID,str6)
    url3 <- game_url3
    
    # Import pxp data from JSON
    # use jsonlite::fromJSON to handle NULL values
    json_data3 <- jsonlite::fromJSON(url3, simplifyDataFrame = TRUE)
  
  # create a tibble of all events
  events <- as_tibble(json_data3[["GC"]][["Pxpverbose"]]) %>%
    mutate(game_id = game_id) %>%
    select(game_id, everything())

  # get faceoff data
  faceoffs <- events %>%
    filter(event == "faceoff") %>%
    select(game_id, event, time_formatted, s, x_location, y_location, location_id,
           home_player_id, visitor_player_id, home_win, win_team_id)

  # get shot data
  shots <- events %>%
    filter(event == "shot") %>%
    select(game_id, event, time, s, period_id, team_id, x_location, y_location,
           shot_player_id = "player_id", home, shot_type, shot_type_description,
           shot_quality_description)

  # get goal data
  goals <- events %>%
    filter(event == "goal") %>%
    mutate(goal_id = as.integer(row_number())) %>%
    select(game_id, event, goal_id, period_id, time, s, team_id, x_location, y_location,
           goal_player_id, assist1_player_id, assist2_player_id,
           goal_type, power_play:game_tieing) %>%
    mutate(across(s:assist2_player_id, as.numeric)) %>%
    mutate(across(power_play:game_tieing, as.numeric)) %>%
    mutate(goal_id = as.numeric(goal_id),
           period_id = as.numeric(period_id)) %>%
    mutate(goal_type = case_when(
      power_play == 1 ~ "PP",
      short_handed == 1 ~ "SH",
      TRUE ~ "EV"
    ))

  # get player_ids who were minus for goals
  minus <- events %>%
    filter(event == "goal") %>%
    select(minus) %>%
    unnest_wider(col = c(minus)) %>%
    select(player_id) %>%
    unnest_wider(col = c(player_id), names_sep = "_minus") %>%
    mutate(across(everything(), as.numeric))

  # get player_ids who were plus for goals
  plus <- events %>%
    filter(event == "goal") %>%
    select(plus) %>%
    unnest_wider(col = c(plus)) %>%
    select(player_id) %>%
    unnest_wider(col = c(player_id), names_sep = "_plus") %>%
    mutate(across(everything(), as.numeric))

  # add plus and minus to goals tibble
  # produce final goals tibble
  goal_data <- goals %>%
    bind_cols(plus, minus)

  # save faceoffs to database
  # sqlSave(conn1,
  #         goal_data,
  #         tablename = "ohl_goal_data",
  #         rownames = F,
  #         append = T)
  
  
  setTxtProgressBar(pb, i)
  
}


