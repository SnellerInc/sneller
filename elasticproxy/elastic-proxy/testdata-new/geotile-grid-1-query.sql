WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
    ),

  "$bucket:gridSplit%0" AS
    (SELECT GEO_TILE_ES("$source"."OriginLocation"."lat","$source"."OriginLocation"."lon",7) AS "$key:gridSplit%0",
            COUNT(*) AS "$doc_count",
            SUM("$source"."FlightDelayMin") AS "sum_of_FlightDelayMin"
     FROM "$source"
     WHERE (((("$source"."OriginLocation"."lat" <= 55.77657) AND ("$source"."OriginLocation"."lon" >= -135)) AND ("$source"."OriginLocation"."lat" >= 40.9799)) AND ("$source"."OriginLocation"."lon" <= -90))
     GROUP BY GEO_TILE_ES("$source"."OriginLocation"."lat","$source"."OriginLocation"."lon",7)
     ORDER BY "$doc_count" DESC
     LIMIT 65535
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:gridSplit%0"
  ) AS "$bucket:gridSplit%0"