IMPORT LNTaxi;
IMPORT Std;


#WORKUNIT('name','Taxi Data: Enrich');

validatedData := LNTaxi.Files.Validation.inFile;

withTimeEnrichment := PROJECT
    (
        validatedData,
        TRANSFORM
            (
                LNTaxi.Files.Enriched.YellowLayout,
                SELF.pickup_date := Std.Date.FromStringToDate(LEFT.tpep_pickup_datetime[..10], '%Y-%m-%d'),
                SELF.pickup_time := Std.Date.FromStringToTime(LEFT.tpep_pickup_datetime[12..], '%H:%M:%S'),
                SELF.pickup_minutes_after_midnight := Std.Date.Hour(SELF.pickup_time) * 60 + Std.Date.Minute(SELF.pickup_time),
                SELF.pickup_time_window := SELF.pickup_minutes_after_midnight DIV 15,
                SELF.pickup_time_hour := Std.Date.Hour(SELF.pickup_time),
                SELF.pickup_day_of_week := Std.Date.DayOfWeek(SELF.pickup_date),
                SELF.dropoff_date := Std.Date.FromStringToDate(LEFT.tpep_dropoff_datetime[..10], '%Y-%m-%d'),
                SELF.dropoff_time := Std.Date.FromStringToTime(LEFT.tpep_dropoff_datetime[12..], '%H:%M:%S'),
                SELF.dropoff_minutes_after_midnight := Std.Date.Hour(SELF.dropoff_time) * 60 + Std.Date.Minute(SELF.dropoff_time),
                SELF.dropoff_time_window := SELF.dropoff_minutes_after_midnight DIV 15,
                SELF.dropoff_time_hour := Std.Date.Hour(SELF.dropoff_time),
                SELF.dropoff_day_of_week := Std.Date.DayOfWeek(SELF.dropoff_date),
                SELF.trip_duration_minutes := MAP
                    (
                        SELF.dropoff_date = SELF.pickup_date        =>  SELF.dropoff_minutes_after_midnight - SELF.pickup_minutes_after_midnight + 1,
                        SELF.dropoff_date = SELF.pickup_date + 1    =>  SELF.dropoff_minutes_after_midnight + ((24 * 60) - SELF.pickup_minutes_after_midnight) + 1,
                        SELF.dropoff_date > SELF.pickup_date + 1    =>  ((Std.Date.DaysBetween(SELF.pickup_date, SELF.dropoff_date) - 1) * (60 * 24)) + SELF.dropoff_minutes_after_midnight + ((24 * 60) - SELF.pickup_minutes_after_midnight) + 1,
                        0
                    ),
                SELF.trip_distance_bucket := LEFT.trip_distance DIV 15,
                SELF := LEFT
            )
    );

OUTPUT(withTimeEnrichment, NAMED('enrichedData'));
// OUTPUT(validatedData(NOT is_valid_record), NAMED('validatedData'));
// OUTPUT(enrichedData,, LNTaxi.Files.GROUP_PREFIX + '::enriched_validated_data', OVERWRITE);