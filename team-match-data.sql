DROP TABLE team_match_data;

CREATE TABLE team_match_data AS WITH team_data AS (
SELECT * 
FROM Team_Attributes ta
JOIN Team t
ON ta.team_api_id = t.team_api_id),

	teamdate_data AS (
SELECT
*,
lead(team_api_id) OVER (PARTITION BY team_api_id ORDER BY "date") AS lead_team_api_id,
lead("date",1,"2016-05-25 00:00:00") OVER (PARTITION BY team_api_id ORDER BY "date") AS end_date
FROM team_data)


SELECT m.match_api_id, 
m."date" match_dt,
m.country_id,
c.name country_name,
m.league_id,
l.name league_name,
m.season,
m.home_team_api_id,
m.away_team_api_id,
td.team_api_id,
td.team_long_name,
td.team_short_name,
m.home_team_goal,
m.away_team_goal,
td."date" t_attr_start_dt,
td.end_date t_attr_end_dt,
td.buildUpPlaySpeed bu_play_speed,
td.buildUpPlaySpeedClass bu_play_speed_class,
td.buildUpPlayDribbling bu_play_dribbling,
td.buildUpPlayDribblingClass bu_play_dribbling_class,
td.buildUpPlayPassing bu_play_passing,
td.buildUpPlayPassingClass bu_play_passing_class,
td.buildUpPlayPositioningClass bu_play_positioning_class,
td.chanceCreationPassing cc_passing,
td.chanceCreationPassingClass cc_passing_class,
td.chanceCreationCrossing cc_crossing,
td.chanceCreationCrossingClass cc_crossing_class,
td.chanceCreationShooting cc_shooting,
td.chanceCreationShootingClass cc_shooting_class,
td.chanceCreationPositioningClass cc_positioning_class,
td.defencePressure defence_pressure,
td.defencePressureClass defence_pressure_class,
td.defenceAggression defence_aggression,
td.defenceAggressionClass defence_agression_class,
td.defenceTeamWidth defence_team_width,
td.defenceTeamWidthClass defence_team_width_class,
td.defenceDefenderLineClass defence_line_class
FROM Match m
JOIN teamdate_data td
ON td.team_api_id = m.home_team_api_id
OR td.team_api_id = m.away_team_api_id
JOIN League l
ON l.id = m.league_id
JOIN Country c
ON c.id = m.country_id
WHERE m."date"  BETWEEN td."date" AND td.end_date
ORDER BY td.team_api_id, td.end_date DESC;
