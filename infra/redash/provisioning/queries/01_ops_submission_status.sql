SELECT
  submission_status,
  COUNT(*) AS submissions
FROM serving.submission_overview
GROUP BY submission_status
ORDER BY submissions DESC;
