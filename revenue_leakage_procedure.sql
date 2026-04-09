USE HealthcareETL;
GO

-- Create the Stored Procedure for Transformation
CREATE OR ALTER PROCEDURE Core.usp_ProcessRevenueLeakage
AS
BEGIN
    -- 1. Clear out the old production data
    TRUNCATE TABLE Core.Fact_Revenue_Leakage;

    -- 2. Transform and Load the new data
    INSERT INTO Core.Fact_Revenue_Leakage (
        Encounter_ID,
        Patient_Name,
        Encounter_Type,
        Description,
        Billed_Amount,
        Paid_Amount,
        Revenue_Leakage,
        Is_Total_Denial
    )
    SELECT 
        e.Id AS Encounter_ID,
        CONCAT(p.FIRST_NAME, ' ', p.LAST_NAME) AS Patient_Name,
        e.ENCOUNTERCLASS AS Encounter_Type,
        e.DESCRIPTION AS Description,
        e.TOTAL_CLAIM_COST AS Billed_Amount,
        e.PAYER_COVERAGE AS Paid_Amount,
        -- Calculate the exact dollar amount lost
        (e.TOTAL_CLAIM_COST - e.PAYER_COVERAGE) AS Revenue_Leakage,
        -- Flag if the insurance company paid absolutely nothing
        CASE WHEN e.PAYER_COVERAGE = 0 THEN 1 ELSE 0 END AS Is_Total_Denial
    FROM Staging.Encounters e
    JOIN Staging.Patients p ON e.PATIENT = p.Id
    WHERE e.TOTAL_CLAIM_COST > 0;
    
    PRINT 'Revenue Leakage processing completed successfully.';
END;
GO