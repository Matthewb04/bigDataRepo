WITH ReviewSentiment AS (
    SELECT 
        app_id, 
        app_name, 
        COUNT(*) AS total_reviews,  
        SUM(recommended = 'True') AS recommended_count,
        (SUM(recommended = 'True') / COUNT(*)) * 100 AS recommended_percentage,
        CASE 
            WHEN (SUM(recommended = 'True') / COUNT(*)) * 100 >= 95 THEN 'Overwhelmingly Positive'
            WHEN (SUM(recommended = 'True') / COUNT(*)) * 100 >= 80 THEN 'Very Positive'
            WHEN (SUM(recommended = 'True') / COUNT(*)) * 100 >= 70 THEN 'Mostly Positive'
            WHEN (SUM(recommended = 'True') / COUNT(*)) * 100 >= 40 THEN 'Mixed'
            WHEN (SUM(recommended = 'True') / COUNT(*)) * 100 >= 20 THEN 'Mostly Negative'
            ELSE 'Overwhelmingly Negative'
        END AS Review_Score
    FROM reviews
    GROUP BY app_id, app_name
),

AvgPlaytime AS (
    SELECT 
        app_ID,
        app_name,
        COUNT(*) AS total_reviews,
        SUM(author_playtime_at_review) AS totalMinutes,
        AVG(author_playtime_at_review) AS avgMinutes,
        AVG(author_playtime_at_review) / 60 AS avgHours
    FROM reviews
    GROUP BY app_id, app_name
)

SELECT 
    rs.app_id,
    rs.app_name,
    rs.Review_Score,
    ap.avgHours
FROM ReviewSentiment rs
JOIN AvgPlaytime ap ON rs.app_id = ap.app_id
ORDER BY ap.avgHours DESC;