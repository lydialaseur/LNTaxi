IMPORT LNTaxi;

#WORKUNIT('name', 'Taxi Data: Weather Stuff');


weatherData := LNTaxi.Files.Weather.inFile;
precipTypeData := LNTaxi.Files.PrecipTypes;

newWeatherData := JOIN
   (
       weatherData,
       precipTypeData,
       LEFT.precipType = RIGHT.precipType,
       TRANSFORM
           (
               LNTaxi.Files.Weather2.FlatWeatherRec,
               SELF.precipTypeID := RIGHT.id,
               SELF := LEFT
           ),
       LEFT OUTER
   );

OUTPUT(newWeatherData,,LNTaxi.Files.Weather2.PATH,OVERWRITE);