-- TPC-DS Query 2: Report the increase of weekly web and catalog sales from one year to the next
-- year for each week.

WITH wscs AS (
    SELECT
        d_week_seq,
        SUM(CASE WHEN (d_day_name='Sunday') THEN sales_price ELSE NULL END) sun_sales,
        SUM(CASE WHEN (d_day_name='Monday') THEN sales_price ELSE NULL END) mon_sales,
        SUM(CASE WHEN (d_day_name='Tuesday') THEN sales_price ELSE NULL END) tue_sales,
        SUM(CASE WHEN (d_day_name='Wednesday') THEN sales_price ELSE NULL END) wed_sales,
        SUM(CASE WHEN (d_day_name='Thursday') THEN sales_price ELSE NULL END) thu_sales,
        SUM(CASE WHEN (d_day_name='Friday') THEN sales_price ELSE NULL END) fri_sales,
        SUM(CASE WHEN (d_day_name='Saturday') THEN sales_price ELSE NULL END) sat_sales
    FROM (
        SELECT d_week_seq, d_day_name, ws_ext_sales_price AS sales_price
        FROM web_sales, date_dim
        WHERE d_date_sk = ws_sold_date_sk
        UNION ALL
        SELECT d_week_seq, d_day_name, cs_ext_sales_price AS sales_price
        FROM catalog_sales, date_dim
        WHERE d_date_sk = cs_sold_date_sk
    ) x
    GROUP BY d_week_seq
)
SELECT
    d1.d_week_seq AS d_week_seq1,
    ROUND(sun_sales1/sun_sales2, 2) AS sun_ratio,
    ROUND(mon_sales1/mon_sales2, 2) AS mon_ratio,
    ROUND(tue_sales1/tue_sales2, 2) AS tue_ratio,
    ROUND(wed_sales1/wed_sales2, 2) AS wed_ratio,
    ROUND(thu_sales1/thu_sales2, 2) AS thu_ratio,
    ROUND(fri_sales1/fri_sales2, 2) AS fri_ratio,
    ROUND(sat_sales1/sat_sales2, 2) AS sat_ratio
FROM (
    SELECT d_week_seq, sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1,
           wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1, sat_sales sat_sales1
    FROM wscs, date_dim
    WHERE d_week_seq = wscs.d_week_seq AND d_year = 2001
) AS y, (
    SELECT d_week_seq, sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2,
           wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2, sat_sales sat_sales2
    FROM wscs, date_dim
    WHERE d_week_seq = wscs.d_week_seq AND d_year = 2001 + 1
) AS z, date_dim d1
WHERE y.d_week_seq = d1.d_week_seq
  AND z.d_week_seq = y.d_week_seq + 53
ORDER BY d_week_seq1
