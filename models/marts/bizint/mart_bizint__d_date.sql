WITH DATE_DIMENSION AS (
   {{ dbt_date.get_date_dimension('2015-01-01', '2022-12-31') }}
)
SELECT DATEDIFF('SEC', '1970-01-01', D.DATE_DAY) AS REPORT_DATE_SK,
       D.DATE_DAY                                AS REPORT_DATE,
       D.YEAR_NUMBER                             AS YEAR,
       D.MONTH_OF_YEAR                           AS MONTH,
       D.MONTH_NAME_SHORT                        AS MONTH_NAME,
       D.DAY_OF_MONTH                            AS DAY_OF_MON,
       D.DAY_OF_WEEK                             AS DAY_OF_WEEK,
       D.WEEK_OF_YEAR                            AS WEEK_OF_YEAR,
       D.DAY_OF_YEAR                             AS DAY_OF_YEAR
FROM DATE_DIMENSION D