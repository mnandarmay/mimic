CREATE OR REPLACE TABLE first_icu_admissions_summary AS
WITH first_icu_admissions AS (
    SELECT 
        icu.subject_id, 
        icu.hadm_id, 
        icu.icustay_id, 
        icu.intime, 
        icu.outtime,
        icu.los,
        adm.hospital_expire_flag,
        ROW_NUMBER() OVER (PARTITION BY icu.subject_id ORDER BY icu.intime) AS rn

    FROM 
        icustays icu
    INNER JOIN 
        admissions adm ON icu.hadm_id = adm.hadm_id
    WHERE 
        adm.admission_type NOT IN ('NEWBORN')  
        AND icu.first_careunit NOT IN ('NICU', 'PICU')
),
vital_signs_duration AS (
    SELECT 
        ce.subject_id, 
        ce.icustay_id,
        COUNT(DISTINCT ce.charttime) AS hours_vital_signs 
    FROM 
        chartevents ce
    GROUP BY 
        ce.subject_id, 
        ce.icustay_id
),
lab_tests_duration AS (
    SELECT 
        le.subject_id, 
        le.hadm_id,
        COUNT(DISTINCT le.charttime) / (1.5/8) AS hours_lab_tests 
    FROM 
        labevents le
    GROUP BY 
        le.subject_id, 
        le.hadm_id
)
SELECT 
    fi.subject_id, 
    fi.hadm_id, 
    fi.icustay_id, 
    fi.intime, 
    fi.outtime, 
    fi.los,
    p.gender, 
    adm.ethnicity,
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, p.dob, adm.admittime) > 89 THEN 91.4 
        ELSE TIMESTAMPDIFF(YEAR, p.dob, adm.admittime) 
    END AS age,
    fi.hospital_expire_flag AS died_during_icu_stay,
    vsd.hours_vital_signs,
    ltd.hours_lab_tests
FROM 
    first_icu_admissions fi
INNER JOIN 
    patients p ON fi.subject_id = p.subject_id
INNER JOIN 
    admissions adm ON fi.hadm_id = adm.hadm_id
LEFT JOIN 
    vital_signs_duration vsd ON fi.subject_id = vsd.subject_id AND fi.icustay_id = vsd.icustay_id
LEFT JOIN 
    lab_tests_duration ltd ON fi.subject_id = ltd.subject_id AND fi.hadm_id = ltd.hadm_id
WHERE 
    fi.rn = 1
    AND fi.los >= 2 
    AND vsd.hours_vital_signs > 48 
    AND ltd.hours_lab_tests > 48;



CREATE OR REPLACE TABLE first_icu_admissions_measurements AS
WITH vital_signs_timeseries AS (
    SELECT 
        ce.subject_id, 
        ce.icustay_id,
        ce.charttime,
        CASE
            WHEN ce.itemid IN (223761, 678) THEN (ce.valuenum - 32) * 5.0 / 9.0 
            ELSE ce.valuenum
        END AS valuenum,
        CASE
            WHEN ce.itemid IN (220051, 220180, 225310, 8364, 8368, 8441, 8555) THEN 'Diastolic blood pressure'
            WHEN ce.itemid IN (225309, 220050, 220179, 6, 51, 455, 6701) THEN 'Systolic blood pressure'
            WHEN ce.itemid IN (456, 52, 6702, 220052, 220181, 225312) THEN 'Mean arterial pressure'
            WHEN ce.itemid IN (223762, 676, 223761, 678) THEN 'Temperature'
            WHEN ce.itemid IN (220277, 646, 6719) THEN 'Peripheral oxygen saturation'
            WHEN ce.itemid IN (220045, 211) THEN 'Heart rate'
            WHEN ce.itemid IN (220210, 8113, 3603, 224690, 615, 618) THEN 'Respiratory rate'
        END AS measurement
    FROM 
        chartevents ce
    INNER JOIN 
        first_icu_admissions_summary fp ON ce.subject_id = fp.subject_id AND ce.icustay_id = fp.icustay_id
    WHERE
        ce.itemid IN (
            220051, 220180, 225310, 8364, 8368, 8441, 8555, 
            225309, 220050, 220179, 6, 51, 455, 6701,
            456, 52, 6702, 220052, 220181, 225312,
            223762, 676, 223761, 678,
            220277, 646, 6719,
            220045, 211,
            220210, 8113, 3603, 224690, 615, 618
        )
),
lab_tests_timeseries AS (
    SELECT 
        le.subject_id, 
        le.hadm_id,
        le.charttime,
        le.valuenum,
        CASE
            WHEN le.itemid = 50862 THEN 'Albumin'
            WHEN le.itemid = 51006 THEN 'Blood urea nitrogen'
            WHEN le.itemid = 50885 THEN 'Bilirubin'
            WHEN le.itemid = 50813 THEN 'Lactate'
            WHEN le.itemid IN (50882, 50803) THEN 'Bicarbonate'
            WHEN le.itemid IN (51366, 51386, 51441, 51111, 51144, 51344) THEN 'Band neutrophil'
            WHEN le.itemid IN (50806, 50902) THEN 'Chloride'
            WHEN le.itemid = 50912 THEN 'Creatinine'
            WHEN le.itemid IN (50809, 50931) THEN 'Glucose'
            WHEN le.itemid IN (50810, 51221) THEN 'Hematocrit'
            WHEN le.itemid IN (50811, 51222) THEN 'Hemoglobin'
            WHEN le.itemid = 51265 THEN 'Platelet count'
            WHEN le.itemid IN (50822, 50971) THEN 'Potassium'
            WHEN le.itemid = 51275 THEN 'Partial thromboplastin time'
            WHEN le.itemid IN (50824, 50983) THEN 'Sodium'
            WHEN le.itemid IN (51300, 51301) THEN 'White blood cells'
        END AS measurement
    FROM 
        labevents le
    INNER JOIN 
        first_icu_admissions_summary fp ON le.subject_id = fp.subject_id AND le.hadm_id = fp.hadm_id
    WHERE
        le.itemid IN (
            50862, 51006, 50885, 50813, 50882, 50803,
            51366, 51386, 51441, 51111, 51144, 51344,
            50806, 50902, 50912, 50809, 50931, 50810,
            51221, 50811, 51222, 51265, 50822, 50971,
            51275, 50824, 50983, 51300, 51301
        )
),
combined_measurements AS (
    SELECT 
        vs.subject_id, 
        vs.icustay_id,
        vs.charttime,
        vs.measurement,
        vs.valuenum
    FROM 
        vital_signs_timeseries vs
    UNION ALL
    SELECT 
        lt.subject_id, 
        lt.hadm_id AS icustay_id,
        lt.charttime,
        lt.measurement,
        lt.valuenum
    FROM 
        lab_tests_timeseries lt
)
SELECT 
    cm.subject_id,
    cm.charttime,
    cm.measurement,
    cm.valuenum,
    fp.los,
    fp.gender,
    fp.ethnicity,
    fp.age,
    fp.died_during_icu_stay,
    fp.outtime
FROM 
    combined_measurements cm
JOIN 
    first_icu_admissions_summary fp ON cm.subject_id = fp.subject_id
ORDER BY 
    cm.subject_id, 
    cm.charttime;