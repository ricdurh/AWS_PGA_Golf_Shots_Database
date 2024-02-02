CREATE TABLE weather (
    latitude DECIMAL,
    longitude DECIMAL,
    year INTEGER,
    event_name VARCHAR(90),
    tournament_id VARCHAR(12),
    start_date DATE,
    end_date DATE,
    temperature_2m DECIMAL,
    relative_humidity_2m DECIMAL,
    dew_point_2m DECIMAL,
    apparent_temperature DECIMAL,
    rain DECIMAL,
    weather_code VARCHAR(5),
    pressure_msl DECIMAL,
    surface_pressure DECIMAL,
    cloud_cover DECIMAL,
    wind_speed_10m DECIMAL,
    wind_speed_100m DECIMAL,
    wind_direction_10m DECIMAL,
    wind_direction_100m DECIMAL,
    wind_gusts_10m DECIMAL,
    soil_temperature_0_to_7cm DECIMAL,
    soil_moisture_7_to_28cm DECIMAL,
    elevation VARCHAR(10),
    Code  VARCHAR(5),
    Description VARCHAR(90),
    tournament_hourly VARCHAR(28),
    hourly_date VARCHAR(12) PRIMARY KEY
);


COPY weather
FROM 's3://pga-tournament-data-non-weekly-updates/final_hourly_dataframe.csv'
IAM_ROLE ##
CSV IGNOREHEADER 1;




CREATE TABLE tee_times (
    tournament_id VARCHAR(8),
    tt_id VARCHAR(9),
    roundStatus VARCHAR(20),
    roundInt INTEGER,
    roundDisplay VARCHAR(2),
    roundStatusColor VARCHAR(10),
    groupHole INTEGER,
    groupLocation VARCHAR(40),
    holeLocation VARCHAR(40),
    groupLocationCode VARCHAR(10),
    groupNumber VARCHAR(10),
    groupStatus VARCHAR(40),
    startTee VARCHAR(10),
    teeTime_utc TIMESTAMPTZ,
    courseId VARCHAR(5),
    playerLastName VARCHAR(20),
    playerId VARCHAR(6),
    latitude DECIMAL,
    longitude DECIMAL,
    local_time TIMESTAMPTZ,
    local_time_format VARCHAR(12),
    ttid_playerid varchar(15) PRIMARY KEY
);



COPY tee_times
FROM 's3://pga-tournament-data-non-weekly-updates/df_tt.csv'
IAM_ROLE ##
CSV IGNOREHEADER 1
BLANKSASNULL;


DROP TABLE strokes;

CREATE TABLE strokes (
    playerLastName VARCHAR(20),
    playerId VARCHAR(5),
    tournamentId VARCHAR(8),
    courseId VARCHAR(10),
    local_teetime TIMESTAMPTZ,
    startTee INTEGER,
    round_num INTEGER,
    holeNumber INTEGER,
    displayHole INTEGER,
    par INTEGER,
    status INTEGER,
    score INTEGER,
    fairwayCenterX DECIMAL,
    fairwayCenterY DECIMAL,
    fairwayCenterZ DECIMAL,
    teeOverviewX DECIMAL,
    teeOverviewY DECIMAL,
    teeOverviewZ DECIMAL,
    pinOverviewX DECIMAL,
    pinOverviewY DECIMAL,
    pinOverviewZ DECIMAL,
    yardage INTEGER,
    videoId VARCHAR(15),
    distance VARCHAR(15),
    distanceRemaining VARCHAR(15),
    toLocationCode VARCHAR(4),
    fromLocationCode VARCHAR(3),
    finalStroke VARCHAR(5),
    showMarker VARCHAR(5),
    markerText VARCHAR(4),
    playByPlay VARCHAR(90),
    playByPlayLabel VARCHAR(7),
    strokeNumber INTEGER,
    strokeType VARCHAR(11),
    fromX DECIMAL,
    fromY DECIMAL,
    fromZ DECIMAL,
    toX DECIMAL,
    toY DECIMAL,
    toZ DECIMAL,

    actualFlightTime INTEGER,
    apexHeight DECIMAL,
    ballSpeed DECIMAL,
    clubSpeed DECIMAL,
    smashFactor DECIMAL,
    launchSpin DECIMAL,
    ballTrajKind VARCHAR(6),
    ballTraj_measTimeInt0 DECIMAL,
    ballTraj_measTimeInt1 DECIMAL,
    ballTraj_spinRateFit0 DECIMAL,
    ballTraj_spinRateFit1 DECIMAL,
    ballTraj_spinRateFit2 DECIMAL,
    ballTraj_spinRateFit3 DECIMAL,
    ballTraj_valTimeInt0 DECIMAL,
    ballTraj_valTimeInt1 DECIMAL,
    ballTraj_xFit0 DECIMAL,
    ballTraj_xFit1 DECIMAL,
    ballTraj_xFit2 DECIMAL,
    ballTraj_xFit3 DECIMAL,
    ballTraj_xFit4 DECIMAL,
    ballTraj_xFit5 DECIMAL,
    ballTraj_xFit6 DECIMAL,
    ballTraj_yFit0 DECIMAL,
    ballTraj_yFit1 DECIMAL,
    ballTraj_yFit2 DECIMAL,
    ballTraj_yFit3 DECIMAL,
    ballTraj_yFit4 DECIMAL,
    ballTraj_yFit5 DECIMAL,
    ballTraj_yFit6 DECIMAL,
    ballTraj_zFit0 DECIMAL,
    ballTraj_zFit1 DECIMAL,
    ballTraj_zFit2 DECIMAL,
    ballTraj_zFit3 DECIMAL,
    ballTraj_zFit4 DECIMAL,
    ballTraj_zFit5 DECIMAL,
    ballTraj_zFit6 DECIMAL,
    tournament_player_shot VARCHAR(20)  PRIMARY KEY

);



COPY strokes
FROM 's3://pga-tournament-data-non-weekly-updates/strokes_df.csv'
IAM_ROLE ##
CSV IGNOREHEADER 1
BLANKSASNULL;

select * from sys_load_error_detail;
