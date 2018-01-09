IMPORT Taxi;
IMPORT Std;


#WORKUNIT('name','Taxi Data: Enrich');

validatedData := Taxi.Files.Validation.inFile;

Taxi.Files.Enriched.YellowLayout MakeEnrichedRec(Taxi.Files.Validation.YellowLayout inRec) := TRANSFORM

    SELF.pickup_date := Std.Date.FromStringToDate(inRec.tpep_pickup_datetime[..10], '%Y-%m-%d');
    SELF.pickup_time := Std.Date.FromStringToTime(inRec.tpep_pickup_datetime[12..], '%H:%M:%S');
    SELF := inRec;
END;


enrichedData := PROJECT
    (
    validatedData, 
    MakeEnrichedRec(LEFT)
    );

OUTPUT(enrichedData, NAMED('enrichedData'));
// OUTPUT(validatedData(NOT is_valid_record), NAMED('validatedData'));
// OUTPUT(enrichedData,, Taxi.Files.GROUP_PREFIX + '::enriched_validated_data', OVERWRITE);