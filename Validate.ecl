IMPORT LNTaxi;

#WORKUNIT('name','Taxi Data: Validation');

taxiData := LNTaxi.Files.ETL.inFile;

LNTaxi.Files.Validation.YellowLayout MakeValidationRec(LNTaxi.Files.ETL.YellowLayout inRec) := TRANSFORM
    SELF.is_good_passenger_count := inRec.passenger_count > 0;

    SELF.is_valid_vendor_id := inRec.VendorID IN [1,2];

    SELF.is_tip_amount_valid := (inRec.payment_type != 1 AND inRec.tip_amount  <= 0.0) 
                                    OR (inRec.payment_type = 1 AND inRec.tip_amount >= 0.0);

    SELF.lat_long_nonZero := inRec.dropoff_longitude != 0
                               AND inRec.dropoff_latitude != 0
                               AND inRec.pickup_longitude != 0
                               AND inRec.pickup_latitude != 0;

    SELF.appropriate_time := inRec.tpep_dropoff_datetime > inRec.tpep_pickup_datetime;

    SELF.is_id_right := inRec.payment_type IN [1,2,3,4,5,6];

    SELF.near_jfk := inRec.dropoff_longitude > -73.7600  AND inRec.dropoff_longitude < -73.7800
                    AND inRec.dropoff_latitude > 40.6300 AND inRec.dropoff_latitude < 40.6500;

    SELF.is_valid_total_amount := inRec.total_amount = sum(inRec.fare_amount, inRec.extra, inRec.mta_tax, inRec.tip_amount, inRec.tolls_amount,inRec.improvement_surcharge);
    
    SELF.is_valid_range := inRec.trip_distance < 350;  //SELECT RANGE of 250 Miles

    SELF.is_valid_record := SELF.is_good_passenger_count 
                                AND SELF.is_valid_vendor_id
                                AND SELF.is_tip_amount_valid
                                AND SELF.lat_long_nonZero
                                AND SELF.appropriate_time
                                AND SELF.is_id_right
                                AND SELF.near_jfk
                                AND SELF.is_valid_total_amount
                                AND SELF.is_valid_range;
    SELF := inRec;
END;


validatedData := PROJECT
    (
    taxiData, 
    MakeValidationRec(LEFT)
    );

// OUTPUT(validatedData, NAMED('validatedData'));
// OUTPUT(validatedData(NOT is_valid_record), NAMED('validatedData'));
OUTPUT(validatedData,, LNTaxi.Files.GROUP_PREFIX + '::validated_data', OVERWRITE);