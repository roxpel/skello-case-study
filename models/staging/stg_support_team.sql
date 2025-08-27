{{ config(materialized='view') }}

-- Support team reference data from case study requirements
SELECT * FROM (
  VALUES 
    ('5217337', 'Héloise'),
    ('5391224', 'Justine'), 
    ('5440474', 'Patrick'),
    ('5300290', 'Raphael')
) AS t(admin_id, team_member_name)