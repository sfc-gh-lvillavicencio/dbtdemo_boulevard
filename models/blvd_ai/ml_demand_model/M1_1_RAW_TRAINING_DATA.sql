-- Boulevard Labor Demand Forecast — Feature Engineering (Model 1: All Demand)
-- Co-authored with CoCo
--
-- Grain:  one row per (LOCATION_ID × SERVICE_CATEGORY × TARGET_DATE)
-- Target: PROVIDER_HOURS_DEMANDED (total provider-active hours for that day)
--         DEMAND_COUNT kept as a secondary feature/target
--
-- Source data  : SCHED.PUBLISHED.*
-- Output table : BLVD_AI.ML_DEMAND_MODEL.LOCATION_SERVICE_TRAINING_RAW
-- Training window: TARGET_DATE >= '2023-01-01'
-- Filters applied (aligned with production query):
--   • STATE = 'final'                          — only completed appointments
--   • CANCELLED = FALSE OR reason=staff_cancel  — real demand incl. staff-initiated cancels
--   • SOURCE NOT IN ('external','migration')    — organic bookings only
--   • CLIENT_ID IS NOT NULL                    — exclude internal staff time-blockers
--   • Add-ons included (BASE_APPOINTMENT_SERVICE_ID not filtered) — all services consume hours
--   • _FIVETRAN_DELETED not filtered — soft deletes already excluded by the source view
--
-- NOTE: 'AND staff_requested' (from production reference query) is intentionally
-- OMITTED — that filter produces Model 2 (sticky demand). STAFF_REQUESTED_RATIO
-- is included as a feature instead.
{{ config(materialized='table') }}

WITH

-- -------------------------------------------------------
-- Holiday calendar (US federal + common observances, 2018-2030)
-- All 11 US federal holidays per year, with observed dates when
-- the actual date falls on a Saturday (→ Friday) or Sunday (→ Monday).
-- Previously missing: Columbus Day, Veterans Day, Juneteenth (from 2021).
-- -------------------------------------------------------
us_holidays AS (
    SELECT holiday_date::DATE AS holiday_date FROM (VALUES
        -- 2018  (10 federal holidays; Juneteenth not yet federal)
        ('2018-01-01'),  -- New Year's Day
        ('2018-01-15'),  -- MLK Day
        ('2018-02-19'),  -- Presidents' Day
        ('2018-05-28'),  -- Memorial Day
        ('2018-07-04'),  -- Independence Day
        ('2018-09-03'),  -- Labor Day
        ('2018-10-08'),  -- Columbus Day
        ('2018-11-12'),  -- Veterans Day observed (Nov 11 = Sunday)
        ('2018-11-22'),  -- Thanksgiving
        ('2018-12-25'),  -- Christmas
        -- 2019  (10 federal holidays)
        ('2019-01-01'),  -- New Year's Day
        ('2019-01-21'),  -- MLK Day
        ('2019-02-18'),  -- Presidents' Day
        ('2019-05-27'),  -- Memorial Day
        ('2019-07-04'),  -- Independence Day
        ('2019-09-02'),  -- Labor Day
        ('2019-10-14'),  -- Columbus Day
        ('2019-11-11'),  -- Veterans Day
        ('2019-11-28'),  -- Thanksgiving
        ('2019-12-25'),  -- Christmas
        -- 2020  (10 federal holidays)
        ('2020-01-01'),  -- New Year's Day
        ('2020-01-20'),  -- MLK Day
        ('2020-02-17'),  -- Presidents' Day
        ('2020-05-25'),  -- Memorial Day
        ('2020-07-04'),  -- Independence Day
        ('2020-09-07'),  -- Labor Day
        ('2020-10-12'),  -- Columbus Day
        ('2020-11-11'),  -- Veterans Day
        ('2020-11-26'),  -- Thanksgiving
        ('2020-12-25'),  -- Christmas
        -- 2021  (11 federal holidays; Juneteenth added mid-year as federal)
        ('2021-01-01'),  -- New Year's Day
        ('2021-01-18'),  -- MLK Day
        ('2021-02-15'),  -- Presidents' Day
        ('2021-05-31'),  -- Memorial Day
        ('2021-06-18'),  -- Juneteenth observed (Jun 19 = Saturday)
        ('2021-07-05'),  -- Independence Day observed (Jul 4 = Sunday)
        ('2021-09-06'),  -- Labor Day
        ('2021-10-11'),  -- Columbus Day
        ('2021-11-11'),  -- Veterans Day
        ('2021-11-25'),  -- Thanksgiving
        ('2021-12-24'),  -- Christmas observed (Dec 25 = Saturday)
        ('2021-12-31'),  -- New Year's Day 2022 observed (Jan 1 2022 = Saturday)
        -- 2022  (11 federal holidays; New Year's observed carried into 2021)
        ('2022-01-17'),  -- MLK Day (New Year's observed = 2021-12-31)
        ('2022-02-21'),  -- Presidents' Day
        ('2022-05-30'),  -- Memorial Day
        ('2022-06-20'),  -- Juneteenth observed (Jun 19 = Sunday)
        ('2022-07-04'),  -- Independence Day
        ('2022-09-05'),  -- Labor Day
        ('2022-10-10'),  -- Columbus Day
        ('2022-11-11'),  -- Veterans Day
        ('2022-11-24'),  -- Thanksgiving
        ('2022-12-26'),  -- Christmas observed (Dec 25 = Sunday)
        -- 2023  (11 federal holidays)
        ('2023-01-01'),  -- New Year's Day
        ('2023-01-16'),  -- MLK Day
        ('2023-02-20'),  -- Presidents' Day
        ('2023-05-29'),  -- Memorial Day
        ('2023-06-19'),  -- Juneteenth
        ('2023-07-04'),  -- Independence Day
        ('2023-09-04'),  -- Labor Day
        ('2023-10-09'),  -- Columbus Day
        ('2023-11-10'),  -- Veterans Day observed (Nov 11 = Saturday)
        ('2023-11-23'),  -- Thanksgiving
        ('2023-12-25'),  -- Christmas
        -- 2024  (11 federal holidays)
        ('2024-01-01'),  -- New Year's Day
        ('2024-01-15'),  -- MLK Day
        ('2024-02-19'),  -- Presidents' Day
        ('2024-05-27'),  -- Memorial Day
        ('2024-06-19'),  -- Juneteenth
        ('2024-07-04'),  -- Independence Day
        ('2024-09-02'),  -- Labor Day
        ('2024-10-14'),  -- Columbus Day
        ('2024-11-11'),  -- Veterans Day
        ('2024-11-28'),  -- Thanksgiving
        ('2024-12-25'),  -- Christmas
        -- 2025  (11 federal holidays)
        ('2025-01-01'),  -- New Year's Day
        ('2025-01-20'),  -- MLK Day
        ('2025-02-17'),  -- Presidents' Day
        ('2025-05-26'),  -- Memorial Day
        ('2025-06-19'),  -- Juneteenth
        ('2025-07-04'),  -- Independence Day
        ('2025-09-01'),  -- Labor Day
        ('2025-10-13'),  -- Columbus Day
        ('2025-11-11'),  -- Veterans Day
        ('2025-11-27'),  -- Thanksgiving
        ('2025-12-25'),  -- Christmas
        -- 2026  (11 federal holidays)
        ('2026-01-01'),  -- New Year's Day
        ('2026-01-19'),  -- MLK Day
        ('2026-02-16'),  -- Presidents' Day
        ('2026-05-25'),  -- Memorial Day
        ('2026-06-19'),  -- Juneteenth
        ('2026-07-04'),  -- Independence Day
        ('2026-09-07'),  -- Labor Day
        ('2026-10-12'),  -- Columbus Day
        ('2026-11-11'),  -- Veterans Day
        ('2026-11-26'),  -- Thanksgiving
        ('2026-12-25'),  -- Christmas
        -- 2027  (11 federal holidays)
        ('2027-01-01'),  -- New Year's Day
        ('2027-01-18'),  -- MLK Day
        ('2027-02-15'),  -- Presidents' Day
        ('2027-05-31'),  -- Memorial Day
        ('2027-06-18'),  -- Juneteenth observed (Jun 19 = Saturday)
        ('2027-07-05'),  -- Independence Day observed (Jul 4 = Sunday)
        ('2027-09-06'),  -- Labor Day
        ('2027-10-11'),  -- Columbus Day
        ('2027-11-11'),  -- Veterans Day
        ('2027-11-25'),  -- Thanksgiving
        ('2027-12-24'),  -- Christmas observed (Dec 25 = Saturday)
        -- 2028  (11 federal holidays; leap year)
        ('2028-01-01'),  -- New Year's Day
        ('2028-01-17'),  -- MLK Day
        ('2028-02-21'),  -- Presidents' Day
        ('2028-05-29'),  -- Memorial Day
        ('2028-06-19'),  -- Juneteenth
        ('2028-07-04'),  -- Independence Day
        ('2028-09-04'),  -- Labor Day
        ('2028-10-09'),  -- Columbus Day
        ('2028-11-10'),  -- Veterans Day observed (Nov 11 = Saturday)
        ('2028-11-23'),  -- Thanksgiving
        ('2028-12-25'),  -- Christmas
        -- 2029  (11 federal holidays)
        ('2029-01-01'),  -- New Year's Day
        ('2029-01-15'),  -- MLK Day
        ('2029-02-19'),  -- Presidents' Day
        ('2029-05-28'),  -- Memorial Day
        ('2029-06-19'),  -- Juneteenth
        ('2029-07-04'),  -- Independence Day
        ('2029-09-03'),  -- Labor Day
        ('2029-10-08'),  -- Columbus Day
        ('2029-11-12'),  -- Veterans Day observed (Nov 11 = Sunday)
        ('2029-11-22'),  -- Thanksgiving
        ('2029-12-25'),  -- Christmas
        -- 2030  (11 federal holidays)
        ('2030-01-01'),  -- New Year's Day
        ('2030-01-21'),  -- MLK Day
        ('2030-02-18'),  -- Presidents' Day
        ('2030-05-27'),  -- Memorial Day
        ('2030-06-19'),  -- Juneteenth
        ('2030-07-04'),  -- Independence Day
        ('2030-09-02'),  -- Labor Day
        ('2030-10-14'),  -- Columbus Day
        ('2030-11-11'),  -- Veterans Day
        ('2030-11-28'),  -- Thanksgiving
        ('2030-12-25')   -- Christmas
    ) AS t(holiday_date)
),

-- -------------------------------------------------------
-- 1. Qualifying appointments
-- -------------------------------------------------------
qualifying_appts AS (
    SELECT
        a.ID                    AS APPOINTMENT_ID,
        a.LOCATION_ID,
        -- APPOINTMENTS.TIME is in the location's local timezone (stored in APPOINTMENTS.TZ).
        -- Convert to UTC first so TARGET_DATE is anchored to a single reference timezone
        -- across all locations. Without this, cross-location date comparisons are skewed:
        -- e.g. a 10pm Friday appointment in LA would count as a Friday demand row while
        -- the same absolute moment in New York would count as a Saturday row.
        CONVERT_TIMEZONE(a.TZ, 'UTC', a.TIME)::DATE AS TARGET_DATE,
        a.TZ,
        a.BOOKED_BY_ID,
        a.BOOKED_BY_CLIENT_ID,
        a.SOURCE,
        a.INSERTED_AT           AS BOOKED_AT
    FROM SCHED.PUBLISHED.APPOINTMENTS a
    LEFT JOIN SCHED.PUBLISHED.APPOINTMENT_CANCELLATIONS ac
        ON  ac.APPOINTMENT_ID = a.ID
    WHERE a.STATE = 'final'
      AND (a.CANCELLED = FALSE OR ac.REASON = 'staff_cancel')
      AND a.SOURCE NOT IN ('external', 'migration')
      AND a.CLIENT_ID IS NOT NULL
    ),

-- -------------------------------------------------------
-- 2. Daily demand aggregates — raw per-day actuals
--    Applies all four APPOINTMENT_SERVICE_OPTIONS deltas.
--
--    ⚠️  DESIGN NOTE — train/serve alignment:
--    This CTE computes the LABELS (PROVIDER_HOURS_DEMANDED, DEMAND_COUNT) and
--    the per-day behavioral values (booking channel ratios, pricing) as same-day
--    actuals. The behavioral values are NOT used directly as model features here;
--    they are passed to rolling_demand, which converts them into 30-day trailing
--    averages (ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) before exposing them
--    as features. This ensures that at training time the feature reflects the
--    recent historical pattern ending one day before TARGET_DATE — the same
--    window that is available at inference time (last 30 actual days).
-- -------------------------------------------------------
daily_demand_raw AS (
    SELECT
        qa.LOCATION_ID,
        --scat_cu_2.service_group       AS SERVICE_CATEGORY,
        scat_cu.Service_classification:labels[0]::string  AS SERVICE_CATEGORY,
        qa.TARGET_DATE,

        -- ── Demand count (kept as feature and secondary target) ──
        COUNT(*)                                                AS DEMAND_COUNT,

        -- ── Average lead time: how far ahead clients book this service ──
        AVG(DATEDIFF('day', qa.BOOKED_AT::DATE, qa.TARGET_DATE)) AS AVG_LEAD_TIME_DAYS,

        -- ── Booking channel features ──
        ROUND(SUM(CASE WHEN qa.BOOKED_BY_ID IS NOT NULL THEN 1 ELSE 0 END)
              * 1.0 / NULLIF(COUNT(*), 0), 4)                   AS BOOKED_INTERNALLY_RATIO,
        ROUND(SUM(CASE WHEN qa.BOOKED_BY_CLIENT_ID IS NOT NULL THEN 1 ELSE 0 END)
              * 1.0 / NULLIF(COUNT(*), 0), 4)                   AS SELF_BOOKED_RATIO,
        -- Online booking ratio: specifically via the public booking widget
        ROUND(SUM(CASE WHEN qa.SOURCE = 'sched_booking_widget' THEN 1 ELSE 0 END)
              * 1.0 / NULLIF(COUNT(*), 0), 4)                   AS BOOKED_ONLINE_RATIO,

        -- ── Provider preference: sticky vs. portable demand ──
        ROUND(SUM(CASE WHEN asvc.STAFF_REQUESTED THEN 1 ELSE 0 END)
              * 1.0 / NULLIF(COUNT(*), 0), 4)                   AS STAFF_REQUESTED_RATIO,

        -- ── Pricing / discount ──
        AVG(asvc.PRICE + COALESCE(aso.PRICE_DELTA, 0))          AS AVG_PRICE_CENTS,
        AVG(COALESCE(asvc.DISCOUNT_CENTS, 0))                   AS AVG_DISCOUNT_CENTS,
        AVG(COALESCE(asvc.DISCOUNT_PERCENTAGE, 0))              AS AVG_DISCOUNT_PCT,
        ROUND(SUM(CASE WHEN COALESCE(asvc.DISCOUNT_CENTS, 0) > 0 THEN 1 ELSE 0 END)
              * 1.0 / NULLIF(COUNT(*), 0), 4)                   AS DISCOUNT_ADOPTION_RATIO,

        -- ── Effective duration (base + delta) ──
        AVG(asvc.DURATION + COALESCE(aso.DURATION_DELTA, 0))    AS AVG_DURATION_MIN,

        -- ── PRIMARY TARGET: provider-active hours (§6.1) ─────────────────
        --    Effective formula across all four delta-adjusted components:
        --      active = (duration+Δ) + (finish+Δ) + (post_staff+Δ)
        --               + (post_client+Δ) only if requires_focus
        SUM(
            (asvc.DURATION            + COALESCE(aso.DURATION_DELTA,             0))
          + (asvc.FINISH_DURATION     + COALESCE(aso.FINISH_DURATION_DELTA,      0))
          + (asvc.POST_STAFF_DURATION + COALESCE(aso.POST_STAFF_DURATION_DELTA,  0))
          + CASE WHEN asvc.REQUIRES_FOCUS
                 THEN (asvc.POST_CLIENT_DURATION + COALESCE(aso.POST_CLIENT_DURATION_DELTA, 0))
                 ELSE 0 END
        ) / 60.0                                                AS PROVIDER_HOURS_DEMANDED  -- in hours

    FROM qualifying_appts qa
    JOIN SCHED.PUBLISHED.APPOINTMENT_SERVICES asvc
        ON  asvc.APPOINTMENT_ID = qa.APPOINTMENT_ID
    JOIN SCHED.PUBLISHED.SERVICES svc
        ON  svc.ID = asvc.SERVICE_ID
        AND svc.ACTIVE = TRUE
    -- Join SERVICE_CATEGORIES to resolve the category name from CATEGORY_ID
    JOIN SCHED.PUBLISHED.SERVICE_CATEGORIES scat
        ON  scat.ID = svc.CATEGORY_ID
        AND scat.ACTIVE = TRUE
    JOIN BLVD_AI.ML_DEMAND_CURATED.SERVICE_CATEGORIES_CURATED scat_cu
        ON  scat.name = scat_cu.NAME 
    --JOIN BLVD_AI.ML_DEMAND_CURATED.SERVICE_CATEGORIES_CURATED_GROUP_LEVELS scat_cu_2
    --    ON  scat_cu.Service_classification:labels[0]::string = scat_cu_2.original_category 
    LEFT JOIN SCHED.PUBLISHED.APPOINTMENT_SERVICE_OPTIONS aso
        ON  aso.APPOINTMENT_SERVICE_ID = asvc.ID     
    GROUP BY qa.LOCATION_ID, SERVICE_CATEGORY, qa.TARGET_DATE
    HAVING SERVICE_CATEGORY IS NOT NULL  -- exclude rows with unclassified service categories early
),

-- -------------------------------------------------------
-- 3. Rolling lag features + behavioral 30-day averages
--
--    ⚠️  TRAIN/SERVE ALIGNMENT — behavioral features:
--    Booking channel ratios, pricing, and lead time are computed here as 30-day
--    trailing averages (ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), NOT as
--    same-day actuals. This makes training and serving consistent:
--
--    Training (TARGET_DATE = D):  window = [D-30, D-1]  — all historical ✓
--    Serving  (any forecast T+k): window = last 30 actual days before TODAY ✓
--
--    The model never sees the same-day actual value of these behavioral signals;
--    it always sees a trailing average representing recent behavioural patterns.
--
--    LABELS (PROVIDER_HOURS_DEMANDED, DEMAND_COUNT) remain as same-day actuals
--    because they are prediction targets, not input features.
--
--    ⚠️  SAME-DAY LAG NOTE (COUNT_APPTS_LAG_7D, COUNT_APPTS_L7D):
--    For forecast dates beyond T+7, these lag windows partially or entirely
--    reference future dates. The serving code (4_DEMAND_FORECAST.py) substitutes
--    COUNT_APPTS_DOW_AVG as a proxy rather than using 0 or autoregressive
--    predictions. See compute_lag_features() for details.
-- -------------------------------------------------------
rolling_demand AS (
    SELECT
        LOCATION_ID,
        --SERVICE_CATEGORY_ID,
        SERVICE_CATEGORY,
        TARGET_DATE,
        DEMAND_COUNT,
        PROVIDER_HOURS_DEMANDED,
        AVG_DURATION_MIN,

        -- ── Rolling demand & hours lags (all windows: PRECEDING AND 1 PRECEDING → no leakage) ──
        --
        -- At serving time, lags that reference future dates are substituted with DOW historical
        -- averages by compute_lag_features() in 4_DEMAND_FORECAST.py.

        -- ── Source: DEMAND_COUNT ────────────────────────────────────────────────────────────
        -- Rolling sums
        COALESCE(SUM(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING), 0) AS COUNT_APPTS_L7D,
        COALESCE(SUM(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS COUNT_APPTS_L30D,
        -- DOW historical average (52-week window)
        COALESCE(AVG(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY, DAYOFWEEK(TARGET_DATE)
            ORDER BY TARGET_DATE ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING), 0) AS COUNT_APPTS_DOW_AVG,
        -- Same-day exact lags (7, 14, 28 days = same calendar DOW; 364 = same week last year)
        COALESCE(LAG(DEMAND_COUNT,  7) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS COUNT_APPTS_LAG_7D,
        COALESCE(LAG(DEMAND_COUNT, 14) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS COUNT_APPTS_LAG_14D,
        COALESCE(LAG(DEMAND_COUNT, 28) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS COUNT_APPTS_LAG_28D,
        COALESCE(LAG(DEMAND_COUNT,364) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS COUNT_APPTS_LAG_364D,
        -- Trend: avg last 2 weeks minus avg weeks 3-4
        COALESCE(AVG(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING), 0)
        - COALESCE(AVG(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 28 PRECEDING AND 15 PRECEDING), 0) AS COUNT_APPTS_TREND_4W,

        -- ── Source: PROVIDER_HOURS_DEMANDED ─────────────────────────────────────────────────
        -- Direct historical signal for the target variable. Captures service-mix drift
        -- (same appointment count but more/fewer hours) that count-based lags cannot detect.
        -- Rolling sums
        COALESCE(SUM(PROVIDER_HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING), 0) AS HOURS_PROVIDER_L7D,
        COALESCE(SUM(PROVIDER_HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS HOURS_PROVIDER_L30D,
        -- DOW historical average (52-week window)
        COALESCE(AVG(PROVIDER_HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY, DAYOFWEEK(TARGET_DATE)
            ORDER BY TARGET_DATE ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING), 0) AS HOURS_PROVIDER_DOW_AVG,
        -- Same-day exact lags
        COALESCE(LAG(PROVIDER_HOURS_DEMANDED,  7) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS HOURS_PROVIDER_LAG_7D,
        COALESCE(LAG(PROVIDER_HOURS_DEMANDED, 14) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS HOURS_PROVIDER_LAG_14D,
        COALESCE(LAG(PROVIDER_HOURS_DEMANDED, 28) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS HOURS_PROVIDER_LAG_28D,
        COALESCE(LAG(PROVIDER_HOURS_DEMANDED,364) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS HOURS_PROVIDER_LAG_364D,
        -- Trend: avg last 2 weeks minus avg weeks 3-4
        COALESCE(AVG(PROVIDER_HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING), 0)
        - COALESCE(AVG(PROVIDER_HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 28 PRECEDING AND 15 PRECEDING), 0) AS HOURS_PROVIDER_TREND_4W,

        -- ── Source: DEMAND_COUNT + PROVIDER_HOURS_DEMANDED ──────────────────────────────────
        -- Realized hours per booking (30-day trailing): explicit service-mix signal.
        -- Complements the static AVG_SERVICE_DURATION_MIN (current menu) with actual behaviour.
        COALESCE(
            SUM(PROVIDER_HOURS_DEMANDED) OVER (
                PARTITION BY LOCATION_ID, SERVICE_CATEGORY
                ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)
            / NULLIF(SUM(DEMAND_COUNT) OVER (
                PARTITION BY LOCATION_ID, SERVICE_CATEGORY
                ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0),
        0) AS HOURS_PROVIDER_PER_APPT_L30D,

        -- ── Behavioral features — 30-day trailing averages (train/serve aligned) ──
        -- These are NOT same-day actuals. Each is computed as the average over the
        -- 30 rows preceding TARGET_DATE in this location × category time series.
        -- At inference time the serving code uses the last 30 actual calendar days.

        -- Lead time: how far in advance clients book (trailing 30-day avg)
        COALESCE(AVG(AVG_LEAD_TIME_DAYS) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS AVG_LEAD_TIME_DAYS,

        -- Booking channel patterns (trailing 30-day avg)
        COALESCE(AVG(BOOKED_INTERNALLY_RATIO) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS BOOKED_INTERNALLY_RATIO,
        COALESCE(AVG(SELF_BOOKED_RATIO) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS SELF_BOOKED_RATIO,
        COALESCE(AVG(BOOKED_ONLINE_RATIO) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS BOOKED_ONLINE_RATIO,
        COALESCE(AVG(STAFF_REQUESTED_RATIO) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS STAFF_REQUESTED_RATIO,

        -- Pricing / discount patterns (trailing 30-day avg)
        COALESCE(AVG(AVG_PRICE_CENTS) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS AVG_PRICE_CENTS,
        COALESCE(AVG(AVG_DISCOUNT_CENTS) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS AVG_DISCOUNT_CENTS,
        COALESCE(AVG(AVG_DISCOUNT_PCT) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS AVG_DISCOUNT_PCT,
        COALESCE(AVG(DISCOUNT_ADOPTION_RATIO) OVER (
            PARTITION BY LOCATION_ID, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS DISCOUNT_ADOPTION_RATIO

    FROM daily_demand_raw
),

-- -------------------------------------------------------
-- 4. Location metadata + provider tenure
-- -------------------------------------------------------
location_info AS (
    SELECT
        l.ID                  AS LOCATION_ID,
        l.LICENSE_TIER,
        l.LATITUDE,
        l.LONGITUDE,
        l.TZ                  AS LOCATION_TZ,
        l.INSERTED_AT::DATE   AS LOCATION_CREATED_DATE,
        -- Address fields extracted from JSON variant
        l.ADDRESS:country::STRING   AS LOCATION_COUNTRY,
        l.ADDRESS:state::STRING     AS LOCATION_STATE,
        l.ADDRESS:province::STRING  AS LOCATION_PROVINCE,
        l.ADDRESS:city::STRING      AS LOCATION_CITY,
        l.ADDRESS:zip::STRING       AS LOCATION_ZIP,
        -- ZIP3 = first 3 chars of postal code; captures regional demand patterns with manageable cardinality.
        -- Kept as STRING categorical feature (Canadian postal codes start with letters).
        LEFT(l.ADDRESS:zip::STRING, 3) AS LOCATION_ZIP3,  -- kept as STRING: Canadian zips start with letters (e.g. M5H)
        COUNT(sl.STAFF_ID)    AS LOCATION_STAFF_COUNT,
        -- Average provider tenure in days (how established is the team)
        AVG(DATEDIFF('day', s.INSERTED_AT::DATE, CURRENT_DATE())) AS AVG_PROVIDER_TENURE_DAYS
    FROM SCHED.PUBLISHED.LOCATIONS l
    LEFT JOIN SCHED.PUBLISHED.STAFF_LOCATIONS sl
        ON  sl.LOCATION_ID = l.ID AND sl.ACTIVE = TRUE
    LEFT JOIN SCHED.PUBLISHED.STAFF s
        ON  s.ID = sl.STAFF_ID
        AND s.ACTIVE = TRUE
    WHERE l.DELETED_AT IS NULL
    GROUP BY l.ID, l.LICENSE_TIER,
             l.LATITUDE, l.LONGITUDE, l.TZ, l.INSERTED_AT::DATE,
             l.ADDRESS:country::STRING, l.ADDRESS:state::STRING,
             l.ADDRESS:province::STRING, l.ADDRESS:city::STRING, l.ADDRESS:zip::STRING,
             LEFT(l.ADDRESS:zip::STRING, 3)
),

-- -------------------------------------------------------
-- 5. Service menu stats per (location × service_category)
--    AVG duration/price from current service rules cascade.
--    NUM_QUALIFIED_PROVIDERS: staff with a bookable rule for any
--    service in this category at this location.
-- -------------------------------------------------------
service_stats AS (
    SELECT
        l.ID                                     AS LOCATION_ID,
        scat_ss.NAME                                         AS SERVICE_CATEGORY,
        AVG(COALESCE(sr.SET_DURATION,  svc.DURATION))       AS AVG_MENU_DURATION_MIN,
        AVG(COALESCE(sr.SET_PRICE,     svc.DEFAULT_PRICE))  AS AVG_MENU_PRICE_CENTS,
        -- How many distinct services exist in this category at this location
        COUNT(DISTINCT svc.ID)                              AS NUM_SERVICES_IN_CATEGORY,
        -- How many staff members can perform at least one service in this category
        COUNT(DISTINCT sr_staff.STAFF_ID)                   AS NUM_QUALIFIED_PROVIDERS
    FROM SCHED.PUBLISHED.LOCATIONS l
    JOIN SCHED.PUBLISHED.SERVICES svc
        ON  svc.BUSINESS_ID = l.BUSINESS_ID
        AND svc.ACTIVE = TRUE
    JOIN SCHED.PUBLISHED.SERVICE_CATEGORIES scat_ss
        ON  scat_ss.ID = svc.CATEGORY_ID
        AND scat_ss.ACTIVE = TRUE
    -- Location-level rule for avg price/duration
    LEFT JOIN SCHED.PUBLISHED.SERVICE_RULES sr
        ON  sr.LOCATION_ID   = l.ID
        AND sr.SERVICE_ID    = svc.ID
        AND sr.DELETED_AT    IS NULL
        AND sr.STAFF_ID      IS NULL
        AND sr.STAFF_ROLE_ID IS NULL
    -- Staff-level rule to count qualified providers
    LEFT JOIN SCHED.PUBLISHED.SERVICE_RULES sr_staff
        ON  sr_staff.LOCATION_ID = l.ID
        AND sr_staff.SERVICE_ID  = svc.ID
        AND sr_staff.STAFF_ID    IS NOT NULL
        AND sr_staff.SET_BOOKABLE = TRUE
        AND sr_staff.DELETED_AT  IS NULL
    GROUP BY l.ID, scat_ss.NAME
),

-- -------------------------------------------------------
-- 6. Services offered per location (breadth of menu)
-- -------------------------------------------------------
services_per_location AS (
    SELECT
        l.ID  AS LOCATION_ID,
        COUNT(DISTINCT svc.ID) AS NUM_SERVICES_OFFERED
    FROM SCHED.PUBLISHED.LOCATIONS l
    JOIN SCHED.PUBLISHED.SERVICES svc
        ON  svc.BUSINESS_ID = l.BUSINESS_ID
        AND svc.ACTIVE = TRUE
    GROUP BY l.ID
),

-- -------------------------------------------------------
-- 7. Scheduled provider capacity per location
--    SHIFT_RULES is not in the production schema (SCHED.PUBLISHED).
--    Capacity is derived from active staff at each location via STAFF_LOCATIONS.
--    NUM_PROVIDERS_SCHEDULED = all active staff (upper bound on available providers).
--    TOTAL_AVAILABLE_HOURS   = staff_count × 8.0 (standard 8-hour workday assumed).
--    This is a static per-location value; for a date-sensitive capacity feature,
--    a scheduling or timesheet table would be required.
-- -------------------------------------------------------
scheduled_capacity AS (
    SELECT
        sl.LOCATION_ID,
        COUNT(DISTINCT sl.STAFF_ID)           AS NUM_PROVIDERS_SCHEDULED,
        COUNT(DISTINCT sl.STAFF_ID) * 8.0     AS TOTAL_AVAILABLE_HOURS
    FROM SCHED.PUBLISHED.STAFF_LOCATIONS sl
    JOIN SCHED.PUBLISHED.STAFF s
        ON  s.ID = sl.STAFF_ID
        AND s.ACTIVE = TRUE
    WHERE sl.ACTIVE = TRUE
    GROUP BY sl.LOCATION_ID
),

-- -------------------------------------------------------
-- 8. Nearest holiday distance per target_date (replaces correlated subquery)
-- -------------------------------------------------------
holiday_proximity AS (
    SELECT
        rd.LOCATION_ID,
        rd.SERVICE_CATEGORY,
        rd.TARGET_DATE,
        CASE WHEN MIN(ABS(DATEDIFF('day', rd.TARGET_DATE, h.holiday_date))) = 0
             THEN 1 ELSE 0 END AS IS_HOLIDAY,
        COALESCE(MIN(ABS(DATEDIFF('day', rd.TARGET_DATE, h.holiday_date))), 60) AS DAYS_TO_NEAREST_HOLIDAY
    FROM rolling_demand rd
    LEFT JOIN us_holidays h
        ON ABS(DATEDIFF('day', rd.TARGET_DATE, h.holiday_date)) <= 30
    GROUP BY rd.LOCATION_ID, rd.SERVICE_CATEGORY, rd.TARGET_DATE
),

-- -------------------------------------------------------
-- 9. Salesforce account metadata per location
--    Joins SALESFORCE.PUBLISHED.ACCOUNT via LOCATIONS.SALESFORCE_ACCOUNT_ID.
--    Covers all 25 columns from the production reference query plus SALES_VERTICAL_C
--    and EST_MONTHLY_APPOINTMENTS_C as extra signals.
-- -------------------------------------------------------
salesforce_account AS (
    SELECT
        l.ID                                                          AS LOCATION_ID,
        -- Extra vertical encoding (more specific than INDUSTRY)
        COALESCE(sa.SALES_VERTICAL_C,             'Unknown')          AS SF_VERTICAL,
        -- Business profile
        sa.INDUSTRY                                                   AS SF_INDUSTRY,
        sa.TYPE                                                       AS SF_ACCOUNT_TYPE,
        sa.SEGMENT_C                                                  AS SF_SEGMENT,
        sa.PLAN_TYPE_C                                                AS SF_PLAN_TYPE,
        -- Capacity
        COALESCE(sa.NO_OF_ACTIVE_LOCATIONS_C,     1)                  AS SF_ACTIVE_LOCATIONS,
        -- Revenue / size
        COALESCE(sa.TOTAL_ARR_C,                  0)                  AS SF_TOTAL_ARR,
        -- SF_TOTAL_GMV excluded: corr=0.83 with SF_TOTAL_ARR — redundant; ARR is the standard metric
        COALESCE(sa.AVG_GMV_PER_LOCATION_C,       0)                  AS SF_AVG_GMV_PER_LOC,
        COALESCE(sa.GPV_PER_APPOINTMENT_C,        0)                  AS SF_GPV_PER_APPT,
        COALESCE(sa.TAKE_RATE_C,                  0)                  AS SF_TAKE_RATE,
        COALESCE(sa.INITIAL_ACV_C,                0)                  AS SF_INITIAL_ACV,
        -- SF_NUM_EMPLOYEES excluded: 23% populated in location context, corr=0.29 with ARR — sparse + weak signal
        -- Utilization: STAFF_UTILIZATION_C excluded — CRM snapshot not aligned
        -- to TARGET_DATE; redundant with computed UTILIZATION_RATE_RECENT
        -- Extra demand signal
        COALESCE(sa.EST_MONTHLY_APPOINTMENTS_C,   0)                  AS SF_EST_MONTHLY_APPTS,
        -- Business structure (100% populated in production)
        COALESCE(sa.ENTERPRISE_C,         FALSE)::INTEGER             AS SF_IS_ENTERPRISE,
        COALESCE(sa.FRANCHISE_C,          FALSE)::INTEGER             AS SF_IS_FRANCHISE,
        COALESCE(sa.CORPORATE_ACCOUNT_C,  FALSE)::INTEGER             AS SF_IS_CORPORATE,
        -- Platform
        COALESCE(sa.BLVD_PAYMENT_PROCESSING_C, FALSE)::INTEGER        AS SF_PAYMENT_PROCESSING,
        -- Maturity
        COALESCE(sa.TENURE_C,                     0)                  AS SF_ACCOUNT_TENURE,
        sa.CREATED_DATE                                               AS SF_CREATED_DATE
        -- SF_LIVE_ON (ACCOUNT_LIVE_ON_C) excluded: corr=0.85 with SF_ACCOUNT_TENURE
    FROM SCHED.PUBLISHED.LOCATIONS l
    LEFT JOIN SALESFORCE.PUBLISHED.ACCOUNT sa
        ON  sa.ID = l.SALESFORCE_ACCOUNT_ID
    WHERE l.DELETED_AT IS NULL
),

-- -------------------------------------------------------
-- 10. Marketing message counts per (location × day)
--     Combines email (MARKETING_COMMUNICATIONS) and SMS
--     (MARKETING_SMS_COMMUNICATIONS) into a single daily total per channel.
--     Used by CTE 11 to compute rolling window features.
-- -------------------------------------------------------
marketing_msgs_daily AS (
    SELECT LOCATION_ID, SENT_AT::DATE AS MSG_DATE, COUNT(*) AS MSG_COUNT, 'email' AS CHANNEL
    FROM SCHED.PUBLISHED.MARKETING_COMMUNICATIONS
    WHERE LOCATION_ID IS NOT NULL
      AND SENT_AT IS NOT NULL
    GROUP BY LOCATION_ID, SENT_AT::DATE

    UNION ALL

    SELECT LOCATION_ID, SENT_AT::DATE AS MSG_DATE, COUNT(*) AS MSG_COUNT, 'sms' AS CHANNEL
    FROM SCHED.PUBLISHED.MARKETING_SMS_COMMUNICATIONS
    WHERE LOCATION_ID IS NOT NULL
      AND SENT_AT IS NOT NULL
    GROUP BY LOCATION_ID, SENT_AT::DATE
),

-- -------------------------------------------------------
-- 11. Rolling marketing message counts per (location × target_date)
--
--     ⚠️  TRAIN/SERVE ALIGNMENT:
--     Windows are strictly BEFORE TARGET_DATE (MSG_DATE < TARGET_DATE).
--     EMAIL_MSGS_L30D: email sends in [D-30, D-1] — full consideration cycle.
--     SMS_MSGS_L7D:    SMS sends  in [D-7,  D-1] — immediate response window.
--     TOTAL_MSGS_L30D: all channels combined in [D-30, D-1] — volume signal.
--
--     At serving time (4_DEMAND_FORECAST.py), load_marketing_meta() computes
--     the same windows anchored to CURRENT_DATE() instead of TARGET_DATE.
--
--     Marketing is LOCATION-level (not per SERVICE_CATEGORY) — the same
--     counts broadcast to all categories at a location on a given TARGET_DATE.
-- -------------------------------------------------------
marketing_window AS (
    SELECT
        rd.LOCATION_ID,
        rd.TARGET_DATE,
        COALESCE(SUM(CASE
            WHEN md.CHANNEL = 'email'
             AND md.MSG_DATE >= DATEADD('day', -30, rd.TARGET_DATE)
             AND md.MSG_DATE <  rd.TARGET_DATE
            THEN md.MSG_COUNT ELSE 0 END), 0) AS EMAIL_MSGS_L30D,
        COALESCE(SUM(CASE
            WHEN md.CHANNEL = 'sms'
             AND md.MSG_DATE >= DATEADD('day', -7,  rd.TARGET_DATE)
             AND md.MSG_DATE <  rd.TARGET_DATE
            THEN md.MSG_COUNT ELSE 0 END), 0) AS SMS_MSGS_L7D,
        COALESCE(SUM(CASE
             WHEN md.MSG_DATE >= DATEADD('day', -30, rd.TARGET_DATE)
             AND  md.MSG_DATE <  rd.TARGET_DATE
            THEN md.MSG_COUNT ELSE 0 END), 0) AS TOTAL_MSGS_L30D
    FROM (SELECT DISTINCT LOCATION_ID, TARGET_DATE FROM rolling_demand) rd
    LEFT JOIN marketing_msgs_daily md
        ON  md.LOCATION_ID = rd.LOCATION_ID
        AND md.MSG_DATE   >= DATEADD('day', -30, rd.TARGET_DATE)
        AND md.MSG_DATE   <  rd.TARGET_DATE
    GROUP BY rd.LOCATION_ID, rd.TARGET_DATE
),

-- -------------------------------------------------------
-- 12. ZIP3-level competitor rolling features
--     Inner subquery: daily totals across all locations in same ZIP3 + category.
--     Outer SELECT: rolling windows over those ZIP3-grain daily rows.
--     ⚠️  Totals include self — competitor-only values derived by subtraction in SELECT.
--     ⚠️  No leakage: all windows use ROWS BETWEEN n PRECEDING AND 1 PRECEDING.
-- -------------------------------------------------------
zip3_rolling AS (
    SELECT
        LOCATION_ZIP3,
        SERVICE_CATEGORY,
        TARGET_DATE,
        ZIP3_NUM_LOCATIONS,

        -- ── Source: ZIP3 DEMAND_COUNT ──────────────────────────────────────────
        -- Rolling sums
        COALESCE(SUM(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING), 0) AS ZIP3_COUNT_APPTS_L7D,
        COALESCE(SUM(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS ZIP3_COUNT_APPTS_L30D,
        -- DOW historical average (52-week window) — market-level, kept as total (see SELECT comment)
        COALESCE(AVG(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY, DAYOFWEEK(TARGET_DATE)
            ORDER BY TARGET_DATE ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING), 0) AS ZIP3_COUNT_APPTS_DOW_AVG,
        -- Same-day exact lags
        COALESCE(LAG(DEMAND_COUNT,  7) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP3_COUNT_APPTS_LAG_7D,
        COALESCE(LAG(DEMAND_COUNT, 28) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP3_COUNT_APPTS_LAG_28D,

        -- ── Source: ZIP3 HOURS_DEMANDED ────────────────────────────────────────
        -- Rolling sums
        COALESCE(SUM(HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING), 0) AS ZIP3_HOURS_PROVIDER_L7D,
        COALESCE(SUM(HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS ZIP3_HOURS_PROVIDER_L30D,
        -- DOW historical average (52-week window) — market-level
        COALESCE(AVG(HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY, DAYOFWEEK(TARGET_DATE)
            ORDER BY TARGET_DATE ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING), 0) AS ZIP3_HOURS_PROVIDER_DOW_AVG,
        -- Same-day exact lags
        COALESCE(LAG(HOURS_DEMANDED,  7) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP3_HOURS_PROVIDER_LAG_7D,
        COALESCE(LAG(HOURS_DEMANDED, 28) OVER (
            PARTITION BY LOCATION_ZIP3, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP3_HOURS_PROVIDER_LAG_28D

    FROM (
        SELECT
            li.LOCATION_ZIP3,
            rd.SERVICE_CATEGORY,
            rd.TARGET_DATE,
            COUNT(DISTINCT rd.LOCATION_ID)   AS ZIP3_NUM_LOCATIONS,
            SUM(rd.DEMAND_COUNT)             AS DEMAND_COUNT,
            SUM(rd.PROVIDER_HOURS_DEMANDED)  AS HOURS_DEMANDED
        FROM daily_demand_raw rd
        JOIN location_info li ON li.LOCATION_ID = rd.LOCATION_ID
        WHERE li.LOCATION_ZIP3 IS NOT NULL
        GROUP BY li.LOCATION_ZIP3, rd.SERVICE_CATEGORY, rd.TARGET_DATE
    )
),

-- -------------------------------------------------------
-- 13. Full-ZIP competitor rolling features (tightest competition cluster)
--     Same structure as zip3_rolling but partitioned by LOCATION_ZIP (full code).
--     Full ZIP typically groups 1-5 locations — strongest direct competition signal.
-- -------------------------------------------------------
zip_rolling AS (
    SELECT
        LOCATION_ZIP,
        SERVICE_CATEGORY,
        TARGET_DATE,
        ZIP_NUM_LOCATIONS,

        -- ── Source: ZIP DEMAND_COUNT ───────────────────────────────────────────
        -- Rolling sums
        COALESCE(SUM(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING), 0) AS ZIP_COUNT_APPTS_L7D,
        COALESCE(SUM(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS ZIP_COUNT_APPTS_L30D,
        -- DOW historical average (52-week window) — market-level
        COALESCE(AVG(DEMAND_COUNT) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY, DAYOFWEEK(TARGET_DATE)
            ORDER BY TARGET_DATE ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING), 0) AS ZIP_COUNT_APPTS_DOW_AVG,
        -- Same-day exact lags
        COALESCE(LAG(DEMAND_COUNT,  7) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP_COUNT_APPTS_LAG_7D,
        COALESCE(LAG(DEMAND_COUNT, 28) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP_COUNT_APPTS_LAG_28D,

        -- ── Source: ZIP HOURS_DEMANDED ─────────────────────────────────────────
        -- Rolling sums
        COALESCE(SUM(HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 7  PRECEDING AND 1 PRECEDING), 0) AS ZIP_HOURS_PROVIDER_L7D,
        COALESCE(SUM(HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY
            ORDER BY TARGET_DATE ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING), 0) AS ZIP_HOURS_PROVIDER_L30D,
        -- DOW historical average (52-week window) — market-level
        COALESCE(AVG(HOURS_DEMANDED) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY, DAYOFWEEK(TARGET_DATE)
            ORDER BY TARGET_DATE ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING), 0) AS ZIP_HOURS_PROVIDER_DOW_AVG,
        -- Same-day exact lags
        COALESCE(LAG(HOURS_DEMANDED,  7) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP_HOURS_PROVIDER_LAG_7D,
        COALESCE(LAG(HOURS_DEMANDED, 28) OVER (
            PARTITION BY LOCATION_ZIP, SERVICE_CATEGORY ORDER BY TARGET_DATE), 0) AS ZIP_HOURS_PROVIDER_LAG_28D

    FROM (
        SELECT
            li.LOCATION_ZIP,
            rd.SERVICE_CATEGORY,
            rd.TARGET_DATE,
            COUNT(DISTINCT rd.LOCATION_ID)   AS ZIP_NUM_LOCATIONS,
            SUM(rd.DEMAND_COUNT)             AS DEMAND_COUNT,
            SUM(rd.PROVIDER_HOURS_DEMANDED)  AS HOURS_DEMANDED
        FROM daily_demand_raw rd
        JOIN location_info li ON li.LOCATION_ID = rd.LOCATION_ID
        WHERE li.LOCATION_ZIP IS NOT NULL
        GROUP BY li.LOCATION_ZIP, rd.SERVICE_CATEGORY, rd.TARGET_DATE
    )
)
SELECT
-- ============================================================
-- COLUMN USAGE LEGEND
--   [TRAIN+PRED]  Used as model input (FEATURE_COLS in 3_DEMAND_MODEL.py
--                 and spine builder in 4_DEMAND_FORECAST.py).
--   [REFERENCE]   Stored in LOCATION_SERVICE_TRAINING_RAW for EDA / debugging only.
--                 NOT in FEATURE_COLS. The feature view (2_DEMAND_FEATURES.py)
--                 may encode or discard these.
--   [TARGET]      Prediction label. Only known for completed past dates.
--                 Never an input feature.
--
-- SOURCE CATEGORIES (secondary axis):
--   Temporal   : derived from TARGET_DATE or a static holiday calendar
--   Location   : static/slow-changing LOCATIONS attributes
--   Provider   : workforce / staffing signals
--   Service    : service catalog (menu breadth, duration, price)
--   Demand     : rolling lag features from historical appointments
--   Behavioural: booking channel and pricing patterns (30-day rolling avg)
--   CRM        : Salesforce account attributes (static per-location)
-- ============================================================

-- ==============================================================
-- SECTION 1: ROW IDENTIFIERS
-- ==============================================================
    rd.LOCATION_ID,
    --rd.SERVICE_CATEGORY_ID,  -- stable UUID; also encoded to SERVICE_CATEGORY_ID_ENC as a feature
    rd.SERVICE_CATEGORY,
    rd.TARGET_DATE,

-- ==============================================================
-- SECTION 2: [TARGET] — Prediction labels
--   Known only for completed past dates.
--   Never passed to the model as input features.
-- ==============================================================

    -- Source: Demand — computed from appointment hours (daily_demand_raw CTE)
    rd.PROVIDER_HOURS_DEMANDED,   -- PRIMARY target: total provider-active hours for the day
    rd.DEMAND_COUNT,              -- SECONDARY target: appointment count; also drives lag features

-- ==============================================================
-- SECTION 3: [TRAIN+PRED] — Model input features
--   Available at both training time (historical rows in LOCATION_SERVICE_TRAINING_RAW)
--   and prediction time (forecast spine in 4_DEMAND_FORECAST.py).
-- ==============================================================

    -- ── 3.1 Temporal / Calendar  ───────────────────────────────────────────
    -- Derived from TARGET_DATE or a static holiday calendar.
    -- Always computable for any past or future date — zero leakage risk.
    DAYOFWEEK(rd.TARGET_DATE)                               AS DAY_OF_WEEK,        -- 0=Sun … 6=Sat
    DAYOFMONTH(rd.TARGET_DATE)                              AS DAY_OF_MONTH,       -- 1-31
    MONTH(rd.TARGET_DATE)                                   AS MONTH_NUM,          -- 1-12
    QUARTER(rd.TARGET_DATE)                                 AS QUARTER,            -- 1-4
    WEEKOFYEAR(rd.TARGET_DATE)                              AS WEEK_OF_YEAR,       -- 1-52
    DAYOFYEAR(rd.TARGET_DATE)                               AS DAY_OF_YEAR,        -- 1-366
    CASE WHEN DAYOFWEEK(rd.TARGET_DATE) IN (0, 6)
         THEN 1 ELSE 0 END                                  AS IS_WEEKEND,
    hp.IS_HOLIDAY,                                                                 -- 1 if US federal holiday
    hp.DAYS_TO_NEAREST_HOLIDAY,                                                    -- 60 if none within ±14 days

    -- ── 3.2 Location — Physical attributes  ───────────────────────────────
    -- Static or very slowly changing (lat/lon, timezone never change;
    -- license tier and creation date rarely change).
    -- ⚠️ LICENSE_TIER is a raw string here; the CASE encoding previously done in this
    -- SQL and in 2_DEMAND_FEATURES.py has been moved into DemandForecastModel.predict().
    DATEDIFF('day', li.LOCATION_CREATED_DATE, rd.TARGET_DATE) AS LOCATION_AGE_DAYS,  -- days since location opened
    li.LATITUDE,
    li.LONGITUDE,
    -- Geographic region features: raw strings — encoded inside DemandForecastModel.predict()
    -- at serving time using maps frozen from the training split.
    li.LOCATION_STATE,                 -- e.g. 'CA', 'NY', 'IL', 'AZ'
    li.LOCATION_CITY,                  -- e.g. 'Beverly Hills', 'Chicago'
    -- ZIP3 = first 3 chars of postal code; STRING categorical feature.
    -- Manageable cardinality (~737 distinct); tree models handle natively.
    li.LOCATION_ZIP3,                  -- e.g. '902', '100', 'M5H', 'V6B'
    -- LICENSE_TIER: raw string — encoded inside DemandForecastModel.predict().
    -- Encoding was previously done here (CASE) and in 2_DEMAND_FEATURES.py;
    -- moved into the model so encoding is versioned with the model artifact.
    li.LICENSE_TIER,                   -- 'starter', 'growth', or 'scale'
    -- TZ_ENC removed: APPOINTMENTS.TZ exists solely to interpret APPOINTMENTS.TIME
    -- (which is in location-local time). It is a conversion aid, not a demand signal.
    -- Geography is already covered by LOCATION_STATE_ENC and LOCATION_CITY_ENC.
    -- Raw LOCATION_TZ is kept in Section 4 [REFERENCE] for timezone conversion use.

    -- ── 3.3 Provider / Workforce  ─────────────────────────────────────────
    -- ⚠️ Current-state snapshots — no SCD history available to reconstruct
    -- staffing as of TARGET_DATE. Slowly-changing: leakage severity is low.
    li.LOCATION_STAFF_COUNT,           -- active staff today (proxy for historical headcount)
    li.AVG_PROVIDER_TENURE_DAYS,       -- avg days since staff INSERTED_AT as of TARGET_DATE ✓
    sc.NUM_PROVIDERS_SCHEDULED,        -- active staff count × 1 (capacity upper bound)
    sc.TOTAL_AVAILABLE_HOURS,          -- staff count × 8h assumed shift
    -- Utilization: recent demand hours / available hours per day
    -- Numerator (COUNT_APPTS_L7D) is correctly windowed; denominator is current-state.
    CASE WHEN COALESCE(sc.TOTAL_AVAILABLE_HOURS, 0) > 0
         THEN ROUND(
                (rd.COUNT_APPTS_L7D / 7.0 * COALESCE(sss.AVG_MENU_DURATION_MIN, 60) / 60.0)
                / NULLIF(sc.TOTAL_AVAILABLE_HOURS, 0), 4)
         ELSE 0 END                                         AS UTILIZATION_RATE_RECENT,

    -- ── 3.4 Service / Menu  ───────────────────────────────────────────────
    -- ⚠️ Current-state snapshot: uses present-day SERVICE_RULES and SERVICES.
    -- No point-in-time catalog history available.
    COALESCE(spl.NUM_SERVICES_OFFERED, 0)                   AS NUM_SERVICES_OFFERED,
    COALESCE(sss.AVG_MENU_DURATION_MIN,   60)               AS AVG_SERVICE_DURATION_MIN,   -- minutes
    COALESCE(sss.AVG_MENU_PRICE_CENTS,  5000)               AS AVG_SERVICE_PRICE_CENTS,    -- cents
    COALESCE(sss.NUM_QUALIFIED_PROVIDERS, 0)                AS NUM_QUALIFIED_PROVIDERS,    -- staff with bookable rule

    -- ── 3.5 Demand — Historical lags  ─────────────────────────────────────
    -- All window functions use ROWS BETWEEN n PRECEDING AND 1 PRECEDING —
    -- no same-day data, no leakage for training rows.
    -- For far-out predictions (days >7 ahead), same-day lags substitute
    -- COUNT_APPTS_DOW_AVG via compute_lag_features() in 4_DEMAND_FORECAST.py.
    rd.COUNT_APPTS_L7D,                   -- total appointments in last 7 days
    rd.COUNT_APPTS_L30D,                  -- total appointments in last 30 days
    rd.COUNT_APPTS_DOW_AVG,            -- historical avg on same day-of-week (52-week window)
    rd.COUNT_APPTS_LAG_7D,             -- demand exactly 7 days prior  (same DOW)
    rd.COUNT_APPTS_LAG_14D,             -- demand exactly 14 days prior
    rd.COUNT_APPTS_LAG_28D,             -- demand exactly 28 days prior
    rd.HOURS_PROVIDER_LAG_7D,              -- provider hours 7 days prior
    rd.COUNT_APPTS_LAG_364D,     -- demand 52 weeks ago (364 days)
    rd.COUNT_APPTS_TREND_4W,                       -- avg last 2 weeks minus avg weeks 3-4
    -- New: PROVIDER_HOURS_DEMANDED rolling features
    rd.HOURS_PROVIDER_L7D,                      -- total provider hours in last 7 days
    rd.HOURS_PROVIDER_L30D,                     -- total provider hours in last 30 days
    rd.HOURS_PROVIDER_LAG_14D,              -- provider hours exactly 14 days prior
    rd.HOURS_PROVIDER_LAG_28D,              -- provider hours exactly 28 days prior
    rd.HOURS_PROVIDER_LAG_364D,      -- provider hours 52 weeks ago
    rd.HOURS_PROVIDER_TREND_4W,                 -- hours trend: avg 2w minus avg weeks 3-4
    rd.HOURS_PROVIDER_PER_APPT_L30D,     -- realized hrs/booking (service-mix signal)
    rd.HOURS_PROVIDER_DOW_AVG,             -- historical avg hours on same DOW (52-week)

    -- ── 3.5a Competitor Demand — ZIP3 (broader local market, ~5–15 locations) ──
    -- Captures whether demand at nearby locations in the same ZIP3 is rising or
    -- falling for this service category. Competitor-only = market total − self.
    -- GREATEST clamps to 0 to guard against rounding/NULL-coalescing edge cases.
    -- DOW_AVG and NUM_LOCATIONS are market-level totals (include self):
    --   DOW_AVG  → market size proxy on that day-of-week
    --   NUM_LOCATIONS → market concentration (1 = monopoly, N = competitive)
    -- ⚠️  Not yet wired into 4_DEMAND_FORECAST.py — stored for EDA / future use.
    GREATEST(z3r.ZIP3_COUNT_APPTS_L7D    - rd.COUNT_APPTS_L7D,   0) AS COMP_ZIP3_COUNT_APPTS_L7D,
    GREATEST(z3r.ZIP3_COUNT_APPTS_L30D   - rd.COUNT_APPTS_L30D,  0) AS COMP_ZIP3_COUNT_APPTS_L30D,
    GREATEST(z3r.ZIP3_COUNT_APPTS_LAG_7D  - rd.COUNT_APPTS_LAG_7D,  0) AS COMP_ZIP3_COUNT_APPTS_LAG_7D,
    GREATEST(z3r.ZIP3_COUNT_APPTS_LAG_28D - rd.COUNT_APPTS_LAG_28D, 0) AS COMP_ZIP3_COUNT_APPTS_LAG_28D,
    GREATEST(z3r.ZIP3_HOURS_PROVIDER_L7D  - rd.HOURS_PROVIDER_L7D,  0) AS COMP_ZIP3_HOURS_PROVIDER_L7D,
    GREATEST(z3r.ZIP3_HOURS_PROVIDER_L30D - rd.HOURS_PROVIDER_L30D, 0) AS COMP_ZIP3_HOURS_PROVIDER_L30D,
    GREATEST(z3r.ZIP3_HOURS_PROVIDER_LAG_7D  - rd.HOURS_PROVIDER_LAG_7D,  0) AS COMP_ZIP3_HOURS_PROVIDER_LAG_7D,
    GREATEST(z3r.ZIP3_HOURS_PROVIDER_LAG_28D - rd.HOURS_PROVIDER_LAG_28D, 0) AS COMP_ZIP3_HOURS_PROVIDER_LAG_28D,
    z3r.ZIP3_COUNT_APPTS_DOW_AVG,          -- market-level DOW avg (incl. self)
    z3r.ZIP3_HOURS_PROVIDER_DOW_AVG,       -- market-level hours DOW avg (incl. self)
    COALESCE(z3r.ZIP3_NUM_LOCATIONS, 1)    AS ZIP3_NUM_LOCATIONS,  -- 1 = solo in zip3

    -- ── 3.5b Competitor Demand — Full ZIP (tightest cluster, typically 1–5 locations) ──
    -- Same logic as 3.5a but at the full zip-code grain — strongest direct competition.
    -- ⚠️  Not yet wired into 4_DEMAND_FORECAST.py — stored for EDA / future use.
    GREATEST(zr.ZIP_COUNT_APPTS_L7D    - rd.COUNT_APPTS_L7D,   0)  AS COMP_ZIP_COUNT_APPTS_L7D,
    GREATEST(zr.ZIP_COUNT_APPTS_L30D   - rd.COUNT_APPTS_L30D,  0)  AS COMP_ZIP_COUNT_APPTS_L30D,
    GREATEST(zr.ZIP_COUNT_APPTS_LAG_7D  - rd.COUNT_APPTS_LAG_7D,  0) AS COMP_ZIP_COUNT_APPTS_LAG_7D,
    GREATEST(zr.ZIP_COUNT_APPTS_LAG_28D - rd.COUNT_APPTS_LAG_28D, 0) AS COMP_ZIP_COUNT_APPTS_LAG_28D,
    GREATEST(zr.ZIP_HOURS_PROVIDER_L7D  - rd.HOURS_PROVIDER_L7D,  0) AS COMP_ZIP_HOURS_PROVIDER_L7D,
    GREATEST(zr.ZIP_HOURS_PROVIDER_L30D - rd.HOURS_PROVIDER_L30D, 0) AS COMP_ZIP_HOURS_PROVIDER_L30D,
    GREATEST(zr.ZIP_HOURS_PROVIDER_LAG_7D  - rd.HOURS_PROVIDER_LAG_7D,  0) AS COMP_ZIP_HOURS_PROVIDER_LAG_7D,
    GREATEST(zr.ZIP_HOURS_PROVIDER_LAG_28D - rd.HOURS_PROVIDER_LAG_28D, 0) AS COMP_ZIP_HOURS_PROVIDER_LAG_28D,
    zr.ZIP_COUNT_APPTS_DOW_AVG,            -- market-level DOW avg (incl. self)
    zr.ZIP_HOURS_PROVIDER_DOW_AVG,         -- market-level hours DOW avg (incl. self)
    COALESCE(zr.ZIP_NUM_LOCATIONS, 1)      AS ZIP_NUM_LOCATIONS,   -- 1 = solo in zip

    -- ── 3.6 Behavioural — Booking channel patterns  ───────────────────────
    -- 30-day trailing averages (train/serve aligned).
    -- Training: rolling window before TARGET_DATE (no same-day leakage).
    -- Serving: last 60 actual days from load_location_service_meta().
    rd.AVG_LEAD_TIME_DAYS,             -- how far in advance clients book (days)
    rd.BOOKED_INTERNALLY_RATIO,        -- fraction booked by staff on behalf of client
    rd.SELF_BOOKED_RATIO,              -- fraction where client initiated the booking (any channel)
    rd.BOOKED_ONLINE_RATIO,            -- fraction booked via online booking widget (SOURCE='sched_booking_widget')
    rd.STAFF_REQUESTED_RATIO,          -- fraction where client chose a specific provider

    -- ── 3.7 Behavioural — Pricing & discount patterns  ────────────────────
    -- 30-day trailing averages (same train/serve alignment as 3.6).
    rd.AVG_PRICE_CENTS,
    rd.AVG_DISCOUNT_CENTS,
    rd.AVG_DISCOUNT_PCT,
    rd.DISCOUNT_ADOPTION_RATIO,

    -- ── 3.7 Marketing — Communication volume  ──────────────────────────────
    -- [TRAIN+PRED] Location-level message counts in trailing windows.
    -- Marketing is not per SERVICE_CATEGORY; all categories at a location
    -- share the same signal (joined on LOCATION_ID + TARGET_DATE only).
    -- Response curves: email ~30 days, SMS ~7 days.
    -- ⚠️ COALESCE to 0: locations with no marketing activity in the window
    --    get 0 — this is meaningful (no expected lift from marketing).
    mw.EMAIL_MSGS_L30D,            -- email sends in last 30 days before TARGET_DATE
    mw.SMS_MSGS_L7D,               -- SMS sends in last 7 days before TARGET_DATE
    mw.TOTAL_MSGS_L30D,            -- combined (email + SMS) in last 30 days

    -- ── 3.8 CRM / Salesforce — Account scale & revenue  ──────────────────
    -- Static account-level attributes joined via LOCATIONS.SALESFORCE_ACCOUNT_ID.
    -- These are CRM snapshots; slowly-changing business attributes, not rolling windows.
    sfa.SF_ACTIVE_LOCATIONS,           -- active locations in account (scale proxy)
    sfa.SF_TOTAL_ARR,                  -- total annual recurring revenue
    -- SF_TOTAL_GMV excluded: corr=0.83 with SF_TOTAL_ARR — redundant; ARR is the standard metric
    sfa.SF_AVG_GMV_PER_LOC,           -- per-location GMV baseline
    sfa.SF_GPV_PER_APPT,              -- gross payment volume per appointment (avg ticket size)
    sfa.SF_TAKE_RATE,                  -- platform take rate
    sfa.SF_INITIAL_ACV,               -- contract value at account signup
    -- SF_NUM_EMPLOYEES excluded: 23% populated in location context, corr=0.29 with ARR — sparse + weak

    -- ── 3.9 CRM / Salesforce — Business structure & maturity  ────────────
    -- Boolean flags are 100% populated in production (reliable signals).
    -- SF_VERTICAL, SF_SEGMENT, SF_PLAN_TYPE, SF_INDUSTRY, SF_ACCOUNT_TYPE are
    -- raw string sources; their encoded versions (SF_VERTICAL_ENC, SF_SEGMENT_ENC,
    -- SF_PLAN_TYPE_ENC, SF_INDUSTRY_ENC, SF_ACCOUNT_TYPE_ENC) are computed in
    -- 2_DEMAND_FEATURES.py and are the actual FEATURE_COLS inputs.
    -- Raw strings stored here for traceability alongside their encoded counterparts.
    sfa.SF_VERTICAL,                   -- → SF_VERTICAL_ENC in feature view
    sfa.SF_SEGMENT,                    -- → SF_SEGMENT_ENC  in feature view
    sfa.SF_PLAN_TYPE,                  -- → SF_PLAN_TYPE_ENC in feature view
    sfa.SF_IS_ENTERPRISE,              -- 1 = enterprise account; 0 = no
    sfa.SF_IS_FRANCHISE,               -- 1 = franchise; 0 = no
    sfa.SF_IS_CORPORATE,               -- 1 = corporate; 0 = no
    sfa.SF_PAYMENT_PROCESSING,         -- 1 = Boulevard payment processing enabled
    sfa.SF_ACCOUNT_TENURE,             -- months as Boulevard customer (days in production)
    sfa.SF_EST_MONTHLY_APPTS,          -- CRM-estimated monthly appointments (demand prior)
    -- Industry and account type: encoded as SF_INDUSTRY_ENC and SF_ACCOUNT_TYPE_ENC
    -- downstream in 2_DEMAND_FEATURES.py.
    sfa.SF_INDUSTRY,                   -- → SF_INDUSTRY_ENC in feature view
    sfa.SF_ACCOUNT_TYPE,               -- → SF_ACCOUNT_TYPE_ENC in feature view

-- ==============================================================
-- ==============================================================
-- SECTION 4: [REFERENCE] — Stored for EDA / analysis
--   NOT in FEATURE_COLS. Could be added in a future model iteration.
-- ==============================================================

    -- ── 4.1 Location — Low-signal or high-cardinality geography  ───────────────
    -- COUNTRY: almost always 'US' — negligible signal.
    -- PROVINCE: NULL for all US locations.
    -- ZIP: full 5-digit code; use ZIP3 (Section 3.2) instead.
    -- LOCATION_TZ: raw timezone string kept for CONVERT_TIMEZONE() use in EDA.
    --   Not a model feature — APPOINTMENTS.TZ is a conversion aid for local TIME,
    --   not a demand signal (geography captured by LOCATION_STATE_ENC / LOCATION_CITY_ENC).
    li.LOCATION_COUNTRY,
    li.LOCATION_PROVINCE,
    li.LOCATION_ZIP,
    li.LOCATION_TZ,

    -- ── 4.2 CRM / Salesforce — Timestamps  ─────────────────────────────────
    -- Stored for EDA / tenure derivation. Age signals are already covered
    -- by SF_ACCOUNT_TENURE and LOCATION_AGE_DAYS.
    sfa.SF_CREATED_DATE              -- when the CRM record was created

FROM rolling_demand rd
JOIN location_info li
    ON  li.LOCATION_ID = rd.LOCATION_ID
LEFT JOIN service_stats sss
    ON  sss.LOCATION_ID      = rd.LOCATION_ID
    AND sss.SERVICE_CATEGORY = rd.SERVICE_CATEGORY
LEFT JOIN services_per_location spl
    ON  spl.LOCATION_ID = rd.LOCATION_ID
LEFT JOIN scheduled_capacity sc
    ON  sc.LOCATION_ID = rd.LOCATION_ID
LEFT JOIN holiday_proximity hp
    ON  hp.LOCATION_ID      = rd.LOCATION_ID
    AND hp.SERVICE_CATEGORY = rd.SERVICE_CATEGORY
    AND hp.TARGET_DATE      = rd.TARGET_DATE
LEFT JOIN salesforce_account sfa
    ON  sfa.LOCATION_ID = rd.LOCATION_ID
LEFT JOIN marketing_window mw
    ON  mw.LOCATION_ID  = rd.LOCATION_ID
    AND mw.TARGET_DATE  = rd.TARGET_DATE
LEFT JOIN zip3_rolling z3r
    ON  z3r.LOCATION_ZIP3    = li.LOCATION_ZIP3
    AND z3r.SERVICE_CATEGORY = rd.SERVICE_CATEGORY
    AND z3r.TARGET_DATE      = rd.TARGET_DATE
LEFT JOIN zip_rolling zr
    ON  zr.LOCATION_ZIP      = li.LOCATION_ZIP
    AND zr.SERVICE_CATEGORY  = rd.SERVICE_CATEGORY
    AND zr.TARGET_DATE       = rd.TARGET_DATE
WHERE rd.TARGET_DATE >= '2023-01-01'
  AND rd.TARGET_DATE <  CURRENT_DATE()
  -- Exclude rows with missing key attributes that would degrade model quality
  AND li.LOCATION_CITY            IS NOT NULL
  AND li.LOCATION_ZIP3            IS NOT NULL
  AND li.LICENSE_TIER             IS NOT NULL
  AND li.AVG_PROVIDER_TENURE_DAYS IS NOT NULL
  AND sfa.SF_INDUSTRY             IS NOT NULL
  AND sfa.SF_SEGMENT              IS NOT NULL
  AND PROVIDER_HOURS_DEMANDED     IS NOT NULL
  AND PROVIDER_HOURS_DEMANDED / DEMAND_COUNT <= 8
ORDER BY rd.LOCATION_ID, rd.SERVICE_CATEGORY, rd.TARGET_DATE
