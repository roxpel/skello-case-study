{{ config(materialized='view') }}

WITH conversation_parts_cleaned AS (
  SELECT 
    -- Primary identifiers (exclude test data with text)
    TRY_CAST(ID AS bigint) as conversation_part_id,
    TRY_CAST(CONVERSATION_ID AS bigint) as conversation_id,
    
    -- Timestamps
    CREATED_AT::timestamp as part_created_at,
    UPDATED_AT::timestamp as part_updated_at,
    NOTIFIED_AT::timestamp as part_notified_at,
    CONVERSATION_CREATED_AT::timestamp as conversation_created_at,
    CONVERSATION_UPDATED_AT::timestamp as conversation_updated_at,
    
    -- Author parsing
    parse_json(AUTHOR):"id"::string as author_id,
    parse_json(AUTHOR):"type"::string as author_type,
    
    -- Part attributes
    TYPE as part_type,
    PART_GROUP,
    
    -- Assignment parsing (for Assignment type parts)
    CASE 
      WHEN ASSIGNED_TO IS NOT NULL 
      AND TRIM(ASSIGNED_TO) != '' 
      AND ASSIGNED_TO != 'null'
      AND ASSIGNED_TO NOT LIKE '%None%'
      AND TRY_PARSE_JSON(ASSIGNED_TO) IS NOT NULL
      THEN TRY_PARSE_JSON(ASSIGNED_TO):"id"::string
      ELSE NULL 
    END as assigned_to_id,
    
    CASE 
      WHEN ASSIGNED_TO IS NOT NULL 
      AND TRIM(ASSIGNED_TO) != '' 
      AND ASSIGNED_TO != 'null'
      AND ASSIGNED_TO NOT LIKE '%None%' 
      AND TRY_PARSE_JSON(ASSIGNED_TO) IS NOT NULL
      THEN TRY_PARSE_JSON(ASSIGNED_TO):"type"::string 
      ELSE NULL 
    END as assigned_to_type
    
  FROM {{ source('raw', 'conversation_parts_raw') }}
  WHERE TRY_CAST(ID AS bigint) IS NOT NULL 
)

SELECT * FROM conversation_parts_cleaned