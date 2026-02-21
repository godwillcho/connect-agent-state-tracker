-- ============================================================
-- Amazon Connect Agent State Transitions - Athena Queries
-- ============================================================
-- These queries run against the agent_state_transitions table
-- populated by the headless 3P service in the Agent Workspace.
--
-- Data is PRE-COMPUTED: durations are in seconds, statuses are
-- categorized, and productivity flags are set. No complex
-- transformations needed - just query and go.
--
-- WorkGroup: <stack-name>-workgroup
-- Database:  <stack-name>_agent_state
-- Table:     agent_state_transitions
-- ============================================================


-- ────────────────────────────────────────────
-- 1. DAILY TIME-IN-STATUS PER AGENT
-- Total hours spent in each status for each agent on a given day
-- ────────────────────────────────────────────
SELECT
    agent_name,
    status_name,
    status_category,
    COUNT(*) AS transitions,
    SUM(duration_seconds) AS total_seconds,
    ROUND(SUM(duration_seconds) / 3600.0, 2) AS total_hours
FROM agent_state_transitions
WHERE year = 2026 AND month = 2 AND day = 20
  AND duration_seconds > 0
GROUP BY agent_name, status_name, status_category
ORDER BY agent_name, total_seconds DESC;


-- ────────────────────────────────────────────
-- 2. AGENT PRODUCTIVITY BREAKDOWN
-- Productive vs non-productive time per agent per day
-- ────────────────────────────────────────────
SELECT
    agent_name,
    date_str,
    ROUND(
        SUM(CASE WHEN is_productive THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS productive_hours,
    ROUND(
        SUM(CASE WHEN NOT is_productive THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS non_productive_hours,
    ROUND(SUM(duration_seconds) / 3600.0, 2) AS total_tracked_hours,
    ROUND(
        100.0 * SUM(CASE WHEN is_productive THEN duration_seconds ELSE 0 END)
        / NULLIF(SUM(duration_seconds), 0),
        1
    ) AS productivity_pct
FROM agent_state_transitions
WHERE year = 2026 AND month = 2
  AND duration_seconds > 0
GROUP BY agent_name, date_str
ORDER BY date_str DESC, agent_name;


-- ────────────────────────────────────────────
-- 3. AVERAGE DURATION PER STATUS
-- How long agents typically spend in each status before transitioning
-- ────────────────────────────────────────────
SELECT
    status_name,
    status_category,
    COUNT(*) AS transition_count,
    ROUND(AVG(duration_seconds), 0) AS avg_seconds,
    ROUND(AVG(duration_seconds) / 60.0, 1) AS avg_minutes,
    MIN(duration_seconds) AS min_seconds,
    MAX(duration_seconds) AS max_seconds,
    ROUND(STDDEV(duration_seconds), 0) AS stddev_seconds
FROM agent_state_transitions
WHERE year = 2026 AND month = 2 AND day = 20
  AND duration_seconds > 0
GROUP BY status_name, status_category
ORDER BY avg_seconds DESC;


-- ────────────────────────────────────────────
-- 4. BUSIEST HOURS ANALYSIS
-- Which hours of the day have the most contact-handling activity
-- ────────────────────────────────────────────
SELECT
    hour_of_day,
    COUNT(DISTINCT agent_name) AS active_agents,
    ROUND(
        SUM(CASE WHEN is_contact_handling THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS contact_handling_hours,
    ROUND(
        SUM(CASE WHEN status_name = 'Available' THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS available_hours,
    ROUND(
        SUM(CASE WHEN NOT is_productive THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS break_hours,
    COUNT(*) AS total_transitions
FROM agent_state_transitions
WHERE year = 2026 AND month = 2 AND day = 20
  AND duration_seconds > 0
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ────────────────────────────────────────────
-- 5. AGENT COMPARISON DASHBOARD
-- Side-by-side comparison of all agents for a date range
-- ────────────────────────────────────────────
SELECT
    agent_name,
    ROUND(SUM(duration_seconds) / 3600.0, 2) AS total_hours,
    ROUND(
        SUM(CASE WHEN is_contact_handling THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS contact_hours,
    ROUND(
        SUM(CASE WHEN status_name = 'Available' THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS available_hours,
    ROUND(
        SUM(CASE WHEN status_category = 'NOT_ROUTABLE' AND NOT is_contact_handling
            THEN duration_seconds ELSE 0 END) / 3600.0, 2
    ) AS break_hours,
    COUNT(*) AS total_transitions,
    ROUND(
        100.0 * SUM(CASE WHEN is_productive THEN duration_seconds ELSE 0 END)
        / NULLIF(SUM(duration_seconds), 0),
        1
    ) AS productivity_pct
FROM agent_state_transitions
WHERE year = 2026 AND month = 2
  AND day BETWEEN 17 AND 21
  AND duration_seconds > 0
GROUP BY agent_name
ORDER BY productivity_pct DESC;


-- ────────────────────────────────────────────
-- 6. STATUS TRANSITION FREQUENCY
-- Most common from-status -> to-status transitions
-- ────────────────────────────────────────────
SELECT
    previous_status_name AS from_status,
    new_status_name AS to_status,
    COUNT(*) AS transition_count,
    ROUND(AVG(duration_seconds), 0) AS avg_seconds_in_from_status,
    ROUND(AVG(duration_seconds) / 60.0, 1) AS avg_minutes_in_from_status
FROM agent_state_transitions
WHERE year = 2026 AND month = 2 AND day = 20
  AND previous_status_name != 'Unknown'
  AND duration_seconds > 0
GROUP BY previous_status_name, new_status_name
ORDER BY transition_count DESC
LIMIT 20;


-- ────────────────────────────────────────────
-- 7. DAY-OF-WEEK PATTERNS
-- How agent behavior varies by day of the week
-- ────────────────────────────────────────────
SELECT
    day_of_week,
    COUNT(DISTINCT agent_name) AS agents,
    COUNT(DISTINCT date_str) AS days_observed,
    ROUND(SUM(duration_seconds) / 3600.0, 1) AS total_hours,
    ROUND(
        SUM(CASE WHEN is_contact_handling THEN duration_seconds ELSE 0 END) / 3600.0, 1
    ) AS contact_hours,
    ROUND(
        100.0 * SUM(CASE WHEN is_productive THEN duration_seconds ELSE 0 END)
        / NULLIF(SUM(duration_seconds), 0),
        1
    ) AS productivity_pct
FROM agent_state_transitions
WHERE year = 2026 AND month = 2
  AND duration_seconds > 0
GROUP BY day_of_week
ORDER BY
    CASE day_of_week
        WHEN 'Monday'    THEN 1
        WHEN 'Tuesday'   THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday'  THEN 4
        WHEN 'Friday'    THEN 5
        WHEN 'Saturday'  THEN 6
        WHEN 'Sunday'    THEN 7
    END;
