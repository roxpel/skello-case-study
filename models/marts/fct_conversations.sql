{{ config(materialized='table') }}

WITH conversations_with_first_response AS (
  SELECT 
    c.conversation_id,
    c.conversation_created_at,
    
    -- Clean assignee fields to handle empty/null values
    CASE 
      WHEN c.assignee_id = '' OR c.assignee_id IS NULL THEN NULL
      ELSE c.assignee_id
    END as assignee_id,
    
    CASE 
      WHEN c.assignee_type = '' OR c.assignee_type IS NULL THEN NULL
      WHEN c.assignee_id = '' OR c.assignee_id IS NULL THEN NULL  -- set to null if assignee_id is null
      ELSE c.assignee_type
    END as assignee_type,
    
    c.state,
    c.priority,
    c.is_open,
    
    -- Clean CSAT rating to handle empty strings and convert to proper type
    CASE 
      WHEN c.csat_rating = '' OR c.csat_rating IS NULL THEN NULL
      ELSE TRY_CAST(c.csat_rating AS INTEGER)
    END as csat_rating,
    
    -- Clean CSAT remark to remove newlines/carriage returns that break CSV parsing
    CASE 
      WHEN c.csat_remark IS NULL OR c.csat_remark = '' THEN NULL
      ELSE TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(c.csat_remark, '\\r\\n|\\r|\\n', ' ', 1, 0),  -- Replace newlines with spaces
          '\\s+', ' ', 1, 0  -- Replace multiple spaces with single space
        )
      )
    END as csat_remark,
    
    -- Clean datetime fields
    CASE 
      WHEN c.csat_created_at = '' THEN NULL
      ELSE c.csat_created_at
    END as csat_created_at,
        
    -- First admin response time calculation (exclude bot 775489)
    MIN(
      CASE 
        WHEN p.author_type = 'admin'
        AND p.part_group = 'Message'
        AND p.author_id != '775489'
        THEN p.part_created_at
        ELSE NULL
      END
    ) as first_admin_response_at,
        
    -- Message counts
    COUNT(
      CASE 
        WHEN p.part_group = 'Message'
        AND p.author_type = 'user'
        THEN 1
      END
    ) as user_message_count,
        
    COUNT(
      CASE 
        WHEN p.part_group = 'Message'
        AND p.author_type = 'admin'
        AND p.author_id != '775489'
        THEN 1
      END
    ) as admin_message_count
      
  FROM {{ ref('stg_conversations') }} c
  LEFT JOIN {{ ref('stg_conversation_parts') }} p 
    ON c.conversation_id = p.conversation_id
  GROUP BY 
    c.conversation_id,
    c.conversation_created_at,
    c.assignee_id,
    c.assignee_type, 
    c.state,
    c.priority,
    c.is_open,
    c.csat_rating,
    c.csat_remark,
    c.csat_created_at
),

conversations_with_support_team AS (
  SELECT 
    *,
    -- Support team assignment check 
    CASE 
      WHEN assignee_id IN ('5217337', '5391224', '5440474', '5300290')
      THEN TRUE
      ELSE FALSE 
    END as is_support_team_assigned
  FROM conversations_with_first_response
),

conversations_with_metrics AS (
  SELECT 
    *,
    -- Response time in minutes to handle potential empty strings
    CASE 
      WHEN first_admin_response_at IS NOT NULL
      THEN DATEDIFF('minute', conversation_created_at, first_admin_response_at)
      ELSE NULL
    END as first_response_time_minutes,
        
    -- Response within 5 minutes flag
    CASE 
      WHEN first_admin_response_at IS NOT NULL
      AND DATEDIFF('minute', conversation_created_at, first_admin_response_at) <= 5
      THEN TRUE
      ELSE FALSE
    END as responded_within_5min
      
  FROM conversations_with_support_team
)

SELECT * FROM conversations_with_metrics