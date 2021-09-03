SELECT 
DISTINCT (appts.appointment_id),
appts.group_id,
appts.doctor_id,
--appts.patient_stage,
CAST(appts.initial_completion_time AS DATE) AS initial_completion_date,

--extractor.extractor_id,
extractor.extractor_name,
-- extractor.employee_number,
-- bhr.scribe_id,
-- bhr.title,

first_note.first_note_date,

EXTRACT(DAY FROM CAST(appts.initial_completion_time AS DATE)-first_note.first_note_date) AS days_scribing,

providers_and_orgs.doctor_name,
providers_and_orgs.group_name,
--providers_and_orgs.specialty_id,
providers_and_orgs.specialty_name,
--providers_and_orgs.sub_specialty_id,
--providers_and_orgs.sub_specialty_name,

provider_experience.days_on_robin,

note_count.note_count AS daily_note_count,

procedures.procedures_string,
em.e_m_code AS em_code,

  (CASE 
    WHEN em.e_m_code IN ('99201', '99211', '99441') THEN 'Level 1' 
    WHEN em.e_m_code IN ('99202', '99212', '99442') THEN 'Level 2' 
    WHEN em.e_m_code IN ('99203', '99213', '99443') THEN 'Level 3' 
    WHEN em.e_m_code IN ('99204', '99214') THEN 'Level 4' 
    WHEN em.e_m_code IN ('99205', '99215') THEN 'Level 5' 
    END) AS em_level,

(CASE 
    WHEN em.e_m_code IN ('99201', '99202', '99203', '99204', '99205') THEN 'New' 
    WHEN em.e_m_code IN ('99211', '99212', '99213', '99214', '99215') THEN 'Established' 
    WHEN em.e_m_code IN ('99441', '99442', '99443') THEN 'Telehealth' 
    WHEN (em.e_m_code IS NULL AND procedures.procedures_string IS NULL) THEN 'Pre-Op' 
    WHEN em.e_m_code  = '99024' THEN 'Post-Op' 

    END) AS pt_stage,

  

worktime.worktime

FROM `prod-data-lake-clean.notes.appointments` AS appts

LEFT JOIN `prod-data-lake-clean.notes.notes_extractor` AS extractor 
    ON extractor.appointment_id = appts.appointment_id

LEFT JOIN `prod-data-lake-clean.employees.bhr_employees` AS bhr
    ON bhr.scribe_id  = extractor.extractor_id

LEFT JOIN `prod-data-lake-clean.scribes.first_note` AS first_note
     ON extractor.extractor_id = first_note.extractor_id

LEFT JOIN `prod-data-lake-clean.providers.providers_and_orgs` AS providers_and_orgs
    ON providers_and_orgs.doctor_id = appts.doctor_id

LEFT JOIN `prod-data-lake-clean.notes.worktime` AS worktime
    ON worktime.appointment_id = appts.appointment_id

LEFT JOIN `prod-data-lake-clean.coding.em_codes` AS em
    ON em.appointment_id = appts.appointment_id

LEFT JOIN `prod-data-lake-clean.coding.procedures_concatenated` AS procedures
    ON procedures.appointment_id = appts.appointment_id    

LEFT JOIN `derived-views.providers.provider_worktimes` AS provider_experience
    ON provider_experience.appointment_id = appts.appointment_id



LEFT JOIN (SELECT 

extractor_id,
extractor_name,
MAX(note_count.note_count) AS note_count,
completion_date

FROM

(SELECT 
scribe.extractor_id,
scribe.extractor_name,
notes.appointment_id,
CAST(notes.initial_completion_time AS DATE) AS completion_date,
ROW_NUMBER() OVER (PARTITION BY scribe.extractor_id ORDER BY notes.initial_completion_time ASC) AS note_count

FROM `prod-data-lake-clean.notes.notes_extractor` scribe

LEFT JOIN `prod-data-lake-clean.notes.appointments` notes
    ON notes.appointment_id =  scribe.appointment_id

ORDER BY scribe.extractor_id, note_count) AS note_count

GROUP BY extractor_id, extractor_name,completion_date 

ORDER BY completion_date
) AS note_count ON (note_count.extractor_id =  extractor.extractor_id AND note_count.completion_date = CAST(appts.initial_completion_time AS DATE))

WHERE (appts.initial_completion_time BETWEEN '2021-01-01' AND CURRENT_DATE()) 
AND bhr.title = 'Scribe'

ORDER BY initial_completion_date
