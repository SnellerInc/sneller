WITH
  "$source" AS
    (SELECT *
     FROM "test"."sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:3%0" AS
    (SELECT TIME_BUCKET("$source"."timestamp",864000) AS "$key:3%0",
            COUNT(*) AS "$doc_count",
            AVG("$source"."AvgTicketPrice") AS "2"
     FROM "$source"
     GROUP BY TIME_BUCKET("$source"."timestamp",864000)
     ORDER BY "$key:3%0" ASC
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:3%0"
  ) AS "$bucket:3%0"