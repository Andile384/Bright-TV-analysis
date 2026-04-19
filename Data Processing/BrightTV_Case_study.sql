--- Joining the two tables to have one Set and analyze it
SELECT V.Channel2,
       UP.Province,

       --- Extracting the month/Day from the SA Local time
       MONTHNAME(from_utc_timestamp(V.RecordDate2,'Africa/Johannesburg')) as Month_Name,
       DAYNAME(from_utc_timestamp(V.RecordDate2, 'Africa/Johannesburg')) as Day_Name,

         --- streaming time buckets (UTC -SA)
     CASE
        WHEN HOUR(from_utc_timestamp(V.RecordDate2, 'Africa/Johannesburg')) BETWEEN  0 AND 11 THEN 'Morning Hours'
       WHEN  HOUR(from_utc_timestamp(V.RecordDate2, 'Africa/Johannesburg')) BETWEEN  12 AND 16 THEN 'Afternoon hours'
       ELSE 'Evening hours'
    END as Viewing_Hour_Buckets,

    Gender,
      
       --- replacing nulls with No Race
       CASE
          WHEN  UP.Race IS NULL 
          OR TRIM(UP.Race = '') 
          THEN 'No Race'
          ELSE UP.Race
      END AS Race,

       --- Age range conditions
       CASE
           WHEN UP.Age BETWEEN 0 AND 17 THEN 'Minor'
           WHEN UP.Age BETWEEN 18 AND 34 THEN 'Youth'
           WHEN UP.Age BETWEEN 35 AND 59 THEN 'Adult'
           ELSE 'Retired'
      END AS Age_Group,

      -- Aggregations functions
      COUNT(DISTINCT V.UserID0) AS Number_of_Users,
      COUNT(*)                  AS Number_of_Views,
      SUM(
        HOUR(V.`Duration 2`) * 3600
      + MINUTE(V.`Duration 2`) * 60
      + SECOND(V.`Duration 2`)
    ) AS Total_Consumption_Seconds       

---Joining Viewership and User Profiles table
FROM `Bright_coffe_shop_casestudy`.`default`.`Viewership` AS V 
LEFT JOIN `Bright_coffe_shop_casestudy`.`default`.`User_Profiles` AS UP
ON V.UserID0 = UP.UserID
GROUP BY V.Channel2,
       UP.Province,
       MONTHNAME(from_utc_timestamp(V.RecordDate2,'Africa/Johannesburg')),
       DAYNAME(from_utc_timestamp(V.RecordDate2, 'Africa/Johannesburg')),
     CASE
        WHEN HOUR(from_utc_timestamp(V.RecordDate2, 'Africa/Johannesburg')) BETWEEN  0 AND 11 THEN 'Morning Hours'
       WHEN  HOUR(from_utc_timestamp(V.RecordDate2, 'Africa/Johannesburg')) BETWEEN  12 AND 16 THEN 'Afternoon hours'
       ELSE 'Evening hours'
    END,
    Gender,
    CASE
          WHEN  UP.Race IS NULL 
          OR TRIM(UP.Race = '') 
          THEN 'No Race'
          ELSE UP.Race
      END,
       CASE
           WHEN UP.Age BETWEEN 0 AND 17 THEN 'Minor'
           WHEN UP.Age BETWEEN 18 AND 34 THEN 'Youth'
           WHEN UP.Age BETWEEN 35 AND 59 THEN 'Adult'
           ELSE 'Retired'
      END
ORDER BY UP.Province, Number_of_views DESC;
