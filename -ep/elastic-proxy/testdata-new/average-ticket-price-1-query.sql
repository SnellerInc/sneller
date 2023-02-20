WITH
  "$source" AS
    (SELECT *
     FROM "sample_flights" AS "$source"
     WHERE (("$source"."timestamp" >= `2022-03-01T00:00:00Z`) AND ("$source"."timestamp" <= `2022-07-01T00:00:00Z`))
    ),

  "$bucket:%0" AS
    (SELECT AVG("$source"."AvgTicketPrice") AS "1",
            FALSE AS "$dummy$"
     FROM "$source"
    )

SELECT
  (SELECT COUNT(*)
   FROM "$source"
  ) AS "$total_count",

  (SELECT *
   FROM "$bucket:%0"
  ) AS "$bucket:%0"