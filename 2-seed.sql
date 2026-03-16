-- 2-seed.sql — Sample Data
-- Covers all edge cases:
--   - Multiple sources
--   - Active and closed applications
--   - Applications spanning multiple months
--   - Intentional duplicate interviews (to test dedup in stg_interviews)
--   - Intentional DQ errors (interview before application date)
--   - Dirty candidates rows (NULL name, NULL id) filtered by stg_candidates
--   - Dirty application rows (invalid role, NULL ids) filtered by stg_applications
--   - Dirty interview rows (invalid outcome) filtered by stg_interviews

-- Candidates
INSERT INTO dwh.raw_candidates VALUES
    (1,  'Alice Martin',    'LinkedIn',    '2024-01-05'),
    (2,  'Bob Chen',        'Referral',    '2024-01-10'),
    (3,  'Clara Soto',      'Career Page', '2024-02-01'),
    (4,  'David Kim',       'Job Board',   '2024-02-15'),
    (5,  'Elena Russo',     'LinkedIn',    '2024-03-01'),
    (6,  'Frank Müller',    'Referral',    '2024-03-20'),
    (7,  'Grace Okafor',    'Career Page', '2024-04-05'),
    (8,  'Hiro Tanaka',     'LinkedIn',    '2024-04-18'),
    (9,  'Irina Petrov',    'Job Board',   '2024-05-02'),
    (10, 'James O\'Brien',  'Referral',    '2024-05-15'),
    (11, NULL,              'LinkedIn',    '2024-06-01'),  -- missing name
    (12, NULL,              'Referral',    '2024-06-10'),  -- missing name
    (NULL, 'Ghost User',   'Job Board',    '2024-06-15'); -- missing candidate_id

-- Applications
INSERT INTO dwh.raw_applications VALUES
    (1,  1,  'Junior',    '2024-01-10', '2024-02-15', 55000),
    (2,  2,  'Senior',    '2024-01-20', '2024-03-10', 90000),
    (3,  5,  'Senior',    '2024-03-05', '2024-05-20', 95000),
    (4,  6,  'Executive', '2024-03-25', '2024-06-30', 150000),
    (5,  9,  'Junior',    '2024-05-05', '2024-07-01', 52000),
    (6,  3,  'Junior',    '2024-02-05', '2024-03-01', 50000),
    (7,  4,  'Senior',    '2024-02-20', '2024-04-10', 85000),
    (8,  7,  'Executive', '2024-04-10', '2024-06-15', 140000),
    (9,  8,  'Junior',    '2024-04-20', NULL,         48000),
    (10, 10, 'Senior',    '2024-05-18', NULL,         88000),
    (11, 1,  'Senior',    '2024-06-01', NULL,         92000),
    (12, 3,  'Executive', '2024-07-10', NULL,         145000),
    (13, 5,  'Junior',    '2024-07-15', NULL,         51000),
    (14, 2,  'Executive', '2024-08-01', NULL,         155000),
    (15, 4,  'Junior',    '2024-09-01', NULL,         49000),
    (16, 1,  'Intern',    '2024-09-10', NULL,         32000),  -- invalid role
    (17, 2,  'Trainee',   '2024-09-12', NULL,         28000),  -- invalid role
    (18, NULL, 'Junior',  '2024-09-15', NULL,         45000);  -- missing candidate_id

-- Interviews
INSERT INTO dwh.raw_interviews VALUES
    (1,  1,  '2024-01-20', 'Passed'),
    (2,  1,  '2024-02-01', 'Passed'),
    (3,  2,  '2024-02-01', 'Passed'),
    (4,  2,  '2024-02-20', 'Passed'),
    (5,  2,  '2024-03-05', 'Passed'),
    (6,  3,  '2024-03-15', 'Passed'),
    (7,  3,  '2024-04-10', 'Passed'),
    (8,  4,  '2024-04-10', 'Passed'),
    (9,  4,  '2024-05-05', 'Passed'),
    (10, 4,  '2024-06-01', 'Passed'),
    (11, 5,  '2024-05-20', 'Passed'),
    (12, 5,  '2024-06-10', 'Passed'),
    (13, 6,  '2024-02-15', 'Rejected'),
    (14, 7,  '2024-03-05', 'Passed'),
    (15, 7,  '2024-03-20', 'Rejected'),
    (16, 8,  '2024-04-20', 'No Show'),
    (17, 8,  '2024-05-10', 'Rejected'),
    (18, 9,  '2024-05-05', 'Passed'),
    (19, 10, '2024-06-01', 'Passed'),
    (20, 10, '2024-06-20', 'Passed'),
    (21, 1,  '2024-01-20', 'Passed'),    -- duplicate of interview_id 1
    (22, 2,  '2024-02-01', 'Passed'),    -- duplicate of interview_id 3
    (23, 6,  '2024-02-15', 'Rejected'),  -- duplicate of interview_id 13
    (24, 11, '2024-05-15', 'Passed'),    -- app 11 applied 2024-06-01
    (25, 12, '2024-06-20', 'Passed'),    -- app 12 applied 2024-07-10
    (26, 13, '2024-07-01', 'No Show'),   -- app 13 applied 2024-07-15
    (27, 9,  '2024-06-01', 'Cancelled'), -- invalid outcome
    (28, 10, '2024-07-01', 'Pending');   -- invalid outcome