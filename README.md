# Scala-Spark-Health-Record-Analyzer
Determines if individual patients have Diabetes, High Blood Pressure, and/or High Cholesterol
and aggregates the percent occurrence for each condition within the population.

## Flow of App

### Input
Accepts a file of health records, where each health record contains:
fasting blood sugar level (mg/DL), cholesterol figures (total, ldl, hdl, & triglycerides - all in mg/dL), 
and blood pressure in units: mm Hg, where the numerator is Systolic, and the denominator is Diastolic.


### Output
From the vital signs in each health record, the application will then determine a patient's results 
and save all to file.

The patient results are then aggregated to display:
 - % of patients with Diabetes
 - % of patients with High Blood pressure
 - % of patients with High Cholesterol
 - % of Healthy patients (% of patients without any of the above conditions)


## Future Work
- Group Patient Results by Age, Region, and Gender to derive insightful info
- Incorporate more precise rules based on age and gender to derive a patient health results

