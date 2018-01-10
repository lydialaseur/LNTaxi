IMPORT Std;

EXPORT Files := MODULE

    EXPORT PREFIX := 'taxi';
    EXPORT PATH_PREFIX := '~' + PREFIX;
    
    EXPORT GROUP_PREFIX := '~4rontrow4life';

    //--------------------------------------------------------------------------

    EXPORT Weather := MODULE

        EXPORT FlatWeatherRec := RECORD
            Std.Date.Date_t         date;
            Std.Date.Seconds_t      minutes_after_midnight;
            STRING                  summary;
            UDECIMAL6_3             temperature;
            UDECIMAL6_3             precipIntensity;
            STRING                  precipType;
            UDECIMAL4_2             windSpeed;
            UDECIMAL4_2             visibility;
            UDECIMAL4_2             cloudCover;
        END;

        EXPORT PATH := PATH_PREFIX + '::weather_new_york_city';

        EXPORT inFile := DATASET(PATH, FlatWeatherRec, FLAT);

    END;

    //--------------------------------------------------------------------------

    EXPORT Raw := MODULE

        EXPORT YellowLayout := RECORD
            STRING  VendorID;
            STRING  tpep_pickup_datetime;
            STRING  tpep_dropoff_datetime;
            STRING  passenger_count;
            STRING  trip_distance;
            STRING  pickup_longitude;
            STRING  pickup_latitude;
            STRING  rate_code_id;
            STRING  store_and_fwd_flag;
            STRING  dropoff_longitude;
            STRING  dropoff_latitude;
            STRING  payment_type;
            STRING  fare_amount;
            STRING  extra;
            STRING  mta_tax;
            STRING  tip_amount;
            STRING  tolls_amount;
            STRING  improvement_surcharge;
            STRING  total_amount;
        END;

        EXPORT PATH := '~{'
            + PREFIX + '::raw::yellow_tripdata_2015-01.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-02.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-03.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-04.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-05.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-06.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-07.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-08.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-09.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-10.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-11.csv,'
            + PREFIX + '::raw::yellow_tripdata_2015-12.csv,'
            + PREFIX + '::raw::yellow_tripdata_2016-01.csv,'
            + PREFIX + '::raw::yellow_tripdata_2016-02.csv,'
            + PREFIX + '::raw::yellow_tripdata_2016-03.csv,'
            + PREFIX + '::raw::yellow_tripdata_2016-04.csv,'
            + PREFIX + '::raw::yellow_tripdata_2016-05.csv,'
            + PREFIX + '::raw::yellow_tripdata_2016-06.csv'
            + '}';

        EXPORT inFile := DATASET(PATH, YellowLayout, CSV(HEADING(1)));

    END;

    //--------------------------------------------------------------------------

    EXPORT ETL := MODULE

        EXPORT CoercedYellowLayout := RECORD
            UNSIGNED1   VendorID;
            STRING19    tpep_pickup_datetime;
            STRING19    tpep_dropoff_datetime;
            UNSIGNED1   passenger_count;
            DECIMAL10_2 trip_distance;
            DECIMAL9_6  pickup_longitude;
            DECIMAL9_6  pickup_latitude;
            UNSIGNED1   rate_code_id;
            STRING1     store_and_fwd_flag;
            DECIMAL9_6  dropoff_longitude;
            DECIMAL9_6  dropoff_latitude;
            UNSIGNED1   payment_type;
            DECIMAL8_2  fare_amount;
            DECIMAL8_2  extra;
            DECIMAL8_2  mta_tax;
            DECIMAL8_2  tip_amount;
            DECIMAL8_2  tolls_amount;
            DECIMAL8_2  improvement_surcharge;
            DECIMAL8_2  total_amount;
        END;

        EXPORT YellowLayout := RECORD
            UNSIGNED4   record_id;
            CoercedYellowLayout;
        END;

        EXPORT PATH := PATH_PREFIX + '::data';

        EXPORT inFile := DATASET(PATH, YellowLayout, FLAT);

    END;
    //--------------------------------------------------------------------------

    EXPORT Validation := MODULE

        EXPORT YellowLayout := RECORD
            ETL.YellowLayout;
            BOOLEAN     is_good_passenger_count;
            BOOLEAN     is_valid_vendor_id;
            BOOLEAN     is_valid_record;
            BOOLEAN     is_tip_amount_valid;
            BOOLEAN     lat_long_nonZero;
            BOOLEAN     appropriate_time;
            BOOLEAN     is_id_right;
            BOOLEAN     near_jfk;
            BOOLEAN     is_valid_total_amount;
            BOOLEAN     is_valid_range;
        END;

        EXPORT PATH := GROUP_PREFIX + '::validated_data';

        EXPORT inFile := DATASET(PATH, YellowLayout, FLAT);

    END;

    //--------------------------------------------------------------------------

    EXPORT Enriched := MODULE

        EXPORT YellowLayout := RECORD
            Validation.YellowLayout;
            Std.Date.Date_t     pickup_date;
            Std.Date.Time_t     pickup_time;
            Std.Date.Time_t     pickup_minutes_after_midnight;
            Std.Date.Time_t     pickup_time_window;
            Std.Date.Time_t     pickup_time_hour;
            Std.Date.Date_t     pickup_day_of_week;
            Std.Date.Time_t     dropoff_minutes_after_midnight;
            Std.Date.Time_t     dropoff_time_window;
            Std.Date.Time_t     dropoff_time_hour;
            Std.Date.Date_t     dropoff_day_of_week;
            Std.Date.Date_t     dropoff_date;
            Std.Date.Time_t     trip_duration_minutes;
            Std.Date.Time_t     dropoff_time;
            DECIMAL10_2         trip_distance_bucket;

            END;

        EXPORT PATH := GROUP_PREFIX + '::validated_enriched_data';

        EXPORT inFile := DATASET(PATH, YellowLayout, FLAT);

    END;

END;