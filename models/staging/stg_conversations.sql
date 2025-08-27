{{ config(materialized='view') }}

WITH conversations_cleaned AS (
  SELECT 
    -- Primary identifiers (exclude test data)
    TRY_CAST(ID AS bigint) as conversation_id,
    
    -- Timestamps
    CREATED_AT::timestamp as conversation_created_at,
    UPDATED_AT::timestamp as conversation_updated_at,
    
    -- Assignee parsing (to handle empty strings and null)
    CASE 
      WHEN ASSIGNEE IS NOT NULL AND TRIM(ASSIGNEE) != '' AND ASSIGNEE != 'null'
      THEN parse_json(ASSIGNEE):"id"::string 
      ELSE NULL 
    END as assignee_id,
    
    CASE 
      WHEN ASSIGNEE IS NOT NULL AND TRIM(ASSIGNEE) != '' AND ASSIGNEE != 'null'
      THEN parse_json(ASSIGNEE):"type"::string 
      ELSE NULL 
    END as assignee_type,
    
    -- Conversation attributes
    STATE,
    PRIORITY,
    OPEN as is_open,
    READ as is_read,
    TYPE as conversation_type,
    
    -- Rating parsing (handle empty strings and null)
    CASE 
      WHEN CONVERSATION_RATING IS NOT NULL AND TRIM(CONVERSATION_RATING) != '' AND CONVERSATION_RATING != 'null'
      THEN parse_json(CONVERSATION_RATING):"rating"::int 
      ELSE NULL 
    END as csat_rating,
    
    CASE 
      WHEN CONVERSATION_RATING IS NOT NULL AND TRIM(CONVERSATION_RATING) != '' AND CONVERSATION_RATING != 'null'
      THEN parse_json(CONVERSATION_RATING):"remark"::string 
      ELSE NULL 
    END as csat_remark,
    
    CASE 
      WHEN CONVERSATION_RATING IS NOT NULL AND TRIM(CONVERSATION_RATING) != '' AND CONVERSATION_RATING != 'null'
      THEN parse_json(CONVERSATION_RATING):"created_at"::timestamp 
      ELSE NULL 
    END as csat_created_at,
    
    -- Tags (keeping as string for now)
    TAGS
    
  FROM {{ source('raw', 'conversations_raw') }}
  WHERE TRY_CAST(ID AS bigint) IS NOT NULL  -- Exclude test data with text IDs
)

SELECT * FROM conversations_cleaned
