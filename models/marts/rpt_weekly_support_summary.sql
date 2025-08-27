{{ config(materialized='table') }}

WITH weekly_metrics AS (
  SELECT 
    DATE_TRUNC('week', conversation_created_at) as week_start,
    
    -- Volume metrics
    COUNT(DISTINCT conversation_id) as total_conversations,
    COUNT(DISTINCT CASE WHEN is_support_team_assigned THEN conversation_id END) as support_conversations,
    
    -- Response time metrics
    COUNT(DISTINCT CASE WHEN responded_within_5min AND is_support_team_assigned THEN conversation_id END) as conversations_responded_5min,
    AVG(CASE WHEN is_support_team_assigned THEN first_response_time_minutes END) as avg_response_time_minutes,
    
    -- CSAT metrics
    COUNT(DISTINCT CASE WHEN csat_rating IS NOT NULL AND is_support_team_assigned THEN conversation_id END) as conversations_with_csat,
    AVG(CASE WHEN is_support_team_assigned THEN csat_rating END) as avg_csat_rating,
    
    -- Individual team member metrics
    COUNT(DISTINCT CASE WHEN assignee_id = '5217337' THEN conversation_id END) as heloise_conversations,
    COUNT(DISTINCT CASE WHEN assignee_id = '5391224' THEN conversation_id END) as justine_conversations,
    COUNT(DISTINCT CASE WHEN assignee_id = '5440474' THEN conversation_id END) as patrick_conversations,
    COUNT(DISTINCT CASE WHEN assignee_id = '5300290' THEN conversation_id END) as raphael_conversations
    
  FROM {{ ref('fct_conversations') }}
  WHERE conversation_created_at >= '2021-10-01' 
  GROUP BY 1
)

SELECT 
  week_start,
  total_conversations,
  support_conversations,
  conversations_responded_5min,
  CASE 
    WHEN support_conversations > 0 
    THEN ROUND(conversations_responded_5min * 100.0 / support_conversations, 2)
    ELSE 0 
  END as pct_responded_within_5min,
  ROUND(avg_response_time_minutes, 2) as avg_response_time_minutes,
  conversations_with_csat,
  ROUND(avg_csat_rating, 2) as avg_csat_rating,
  heloise_conversations,
  justine_conversations,
  patrick_conversations,
  raphael_conversations
FROM weekly_metrics
ORDER BY week_start DESC