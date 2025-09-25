# Databricks notebook source
# MAGIC %md
# MAGIC # Legacy CRM Historical Load - CORRECTED FINAL VERSION
# MAGIC ## Fixes:
# MAGIC ## 1. Proper cutoff dates (day BEFORE 4Ops starts)
# MAGIC ## 2. Better deduplication for lead-property events
# MAGIC ## 3. Comprehensive verification queries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Configuration
LEGACY_CRM_TABLE = "dsvc_uat_gov_fin.stg_d365.stg_d365_lead"
LEGACY_LEADPROPERTY_TABLE = "dsvc_uat_gov_fin.stg_d365.stg_d365_ah4r_leadproperty"
LEGACY_LEADSOURCE_TABLE = "dsvc_uat_gov_fin.stg_d365.stg_d365_ah4r_leadsource"
PROPERTY_DIM_TABLE = "dsvc_uat_gov_fin.enr_property.dim_property"
UAT_SCHEMA = "dsvc_uat_ungov.enr_outreach"
GEOGRAPHY_DIM_TABLE = "dsvc_uat_gov_fin.enr_property.dim_geography"

# Start date
start_date = datetime(2018, 1, 1)

# Define region cutoff dates - CORRECTED
# Legacy ends the day BEFORE 4Ops starts
REGION_CUTOFFS = {
    'Texas': datetime(2023, 9, 10),           # 4Ops starts 9/11
    'Western': datetime(2023, 9, 10),         # 4Ops starts 9/11
    'Southeast': datetime(2023, 10, 9),       # 4Ops starts 10/10
    'Midwest': datetime(2023, 10, 22),        # 4Ops starts 10/23
    'Carolinas': datetime(2023, 10, 22),      # 4Ops starts 10/23
    'Florida': datetime(2023, 11, 6),         # 4Ops starts 11/7
    'DEFAULT': datetime(2023, 8, 6)          # For leads without properties
}

print("LEGACY CRM FINAL LOAD - Region-Based Cutoffs")
print("="*60)
print(f"Start Date: {start_date.strftime('%Y-%m-%d')}")
print("\nLegacy CRM ENDS on these dates (4Ops starts next day):")
for region, cutoff in sorted(REGION_CUTOFFS.items()):
    if region != 'DEFAULT':
        print(f"  {region}: Legacy ends {cutoff.strftime('%Y-%m-%d')}")
print(f"\nLeads without properties: Legacy ends {REGION_CUTOFFS['DEFAULT'].strftime('%Y-%m-%d')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Delete ALL Previously Loaded Legacy CRM Data

# COMMAND ----------

print("Deleting all previous Legacy CRM data...")
# delete_leads = spark.sql(f"DELETE FROM {UAT_SCHEMA}.dim_lead WHERE lead_source_system = 'Legacy CRM'")
# delete_events = spark.sql(f"DELETE FROM {UAT_SCHEMA}.ft_lead_event WHERE lead_key >= 1000000")
delete_leads = spark.sql(f"TRUNCATE TABLE {UAT_SCHEMA}.dim_lead")
delete_events = spark.sql(f"TRUNCATE TABLE {UAT_SCHEMA}.ft_lead_event")
delete_communication = spark.sql(f"TRUNCATE TABLE {UAT_SCHEMA}.ft_lead_communication")
print("✅ Previous Legacy CRM data deleted completely")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Transformation Functions

# COMMAND ----------

@udf(returnType=StringType())
def capitalize_name(name):
    """Capitalize names properly"""
    if not name:
        return ''
    return ' '.join(word.capitalize() for word in name.strip().split())

@udf(returnType=StringType())
def format_email_lowercase(email):
    """Convert email to lowercase and trim"""
    if not email:
        return ''
    return email.strip().lower()

@udf(returnType=StringType())
def format_phone_e164(phone):
    """Convert phone to E.164 format"""
    import re
    if not phone:
        return ''
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"+{digits}"
    elif len(digits) > 0:
        return f"+{digits}"
    else:
        return ''

spark.udf.register("capitalize_name", capitalize_name)
spark.udf.register("format_email_lowercase", format_email_lowercase)
spark.udf.register("format_phone_e164", format_phone_e164)

print("✅ Transformation functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Lookup Tables with Region Mapping

# COMMAND ----------

# Load lead source lookup
leadsource_lookup = spark.sql(f"""
SELECT 
    ah4r_leadsourceid,
    ah4r_name as source_name
FROM {LEGACY_LEADSOURCE_TABLE}
WHERE statecode = 0
""")
leadsource_lookup.createOrReplaceTempView("leadsource_lookup")

# Check if region_name exists, otherwise map from state

property_region_lookup = spark.sql(f"""
SELECT
    p.property_id_crm,
    p.property_key,
    p.current_geo_key,
    g.region_name AS region
FROM {PROPERTY_DIM_TABLE} p
LEFT JOIN {GEOGRAPHY_DIM_TABLE} g
ON p.current_geo_key = g.geo_key
""")
property_region_lookup.createOrReplaceTempView("property_region_lookup")
print("✅ Loaded property_region_lookup via geography join (current_geo_key -> geo_key)")

property_region_lookup.createOrReplaceTempView("property_region_lookup")

print("✅ Loaded lookup tables")

# Show region distribution
# print("\nRegion distribution:")
# display(spark.sql("""
# SELECT 
#     region,
#     COUNT(*) as property_count,
    #COLLECT_SET(state) as states
# FROM property_region_lookup
# GROUP BY region
# ORDER BY property_count DESC
# """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Filter Lead Properties with Deduplication and Region Cutoffs

# COMMAND ----------

# Filter lead properties with proper deduplication BY EMAIL + PROPERTY
lead_property_filtered = spark.sql(f"""
WITH lead_emails AS (
    -- First get the email for each lead
    SELECT DISTINCT 
        leadid,
        LOWER(TRIM(emailaddress1)) as email_key
    FROM {LEGACY_CRM_TABLE}
    WHERE emailaddress1 IS NOT NULL AND emailaddress1 != ''
),
property_with_email AS (
    -- Join to get email for each lead property record
    SELECT 
        lp.ah4r_leadpropertyid,
        lp.ah4r_leadsid,
        lp.ah4r_propertyid,
        lp.createdon,
        lp.new_favoritesdatetime,
        --lp.ah4r_preferredlocation,
        lp.statecode,
        le.email_key,
        p.property_key,
        p.region,
        -- Use correct cutoff dates
        CASE 
            WHEN p.region IN ('Texas', 'Western') THEN DATE('2023-09-10')
            WHEN p.region = 'Southeast' THEN DATE('2023-10-09')
            WHEN p.region IN ('Midwest', 'Carolinas') THEN DATE('2023-10-22')
            WHEN p.region = 'Florida' THEN DATE('2023-11-06')
            ELSE DATE('2023-08-06')
        END as cutoff_date,
        -- DEDUPLICATE BY EMAIL + PROPERTY (not lead + property)
        lp.ah4r_chekedindatetime,
        ROW_NUMBER() OVER (
            PARTITION BY le.email_key, lp.ah4r_propertyid 
            ORDER BY lp.createdon ASC
        ) as rn
    FROM {LEGACY_LEADPROPERTY_TABLE} lp
    INNER JOIN lead_emails le ON lp.ah4r_leadsid = le.leadid
    LEFT JOIN property_region_lookup p ON lp.ah4r_propertyid = p.property_id_crm
    WHERE lp.createdon >= '{start_date.strftime('%Y-%m-%d')}'
        AND lp.statecode = 0
)
SELECT *
FROM property_with_email
WHERE date(createdon) <= cutoff_date
    AND rn = 1  -- Keep only first occurrence per email+property combination
""")
lead_property_filtered.createOrReplaceTempView("lead_property_filtered")

property_count = lead_property_filtered.count()
print(f"✅ Filtered {property_count:,} unique email-property combinations")

# Verify cutoffs by region
print("\nVerifying cutoff dates by region:")
cutoff_verify = spark.sql("""
SELECT 
    region,
    COUNT(*) as property_events,
    MIN(createdon) as earliest_date,
    MAX(createdon) as latest_date,
    MAX(cutoff_date) as cutoff_used
FROM lead_property_filtered
GROUP BY region
ORDER BY property_events DESC
""")
display(cutoff_verify)

# Check deduplication effectiveness
print("\nDeduplication check:")
dedup_stats = spark.sql(f"""
WITH before_dedup AS (
    SELECT COUNT(*) as total_before
    FROM {LEGACY_LEADPROPERTY_TABLE} lp
    WHERE lp.createdon >= '{start_date.strftime('%Y-%m-%d')}'
        AND lp.statecode = 0
)
SELECT 
    b.total_before,
    {property_count} as total_after,
    b.total_before - {property_count} as removed_duplicates
FROM before_dedup b
""")
display(dedup_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Filter Leads Based on Property Activity

# COMMAND ----------

# Get all leads with proper cutoffs
leads_filtered = spark.sql(f"""
WITH lead_property_dates AS (
    SELECT 
        ah4r_leadsid,
        MAX(createdon) as latest_property_date,
        MAX(cutoff_date) as lead_cutoff_date
    FROM lead_property_filtered
    GROUP BY ah4r_leadsid
)
SELECT DISTINCT l.*, ls.source_name
FROM {LEGACY_CRM_TABLE} l
LEFT JOIN leadsource_lookup ls ON l.ah4r_sourceid = ls.ah4r_leadsourceid
LEFT JOIN lead_property_dates lpd ON l.leadid = lpd.ah4r_leadsid
WHERE l.emailaddress1 IS NOT NULL 
    AND l.emailaddress1 != ''
    AND l.createdon >= '{start_date.strftime('%Y-%m-%d')}'
    AND (
        -- Has property: use property-based cutoff
        (lpd.ah4r_leadsid IS NOT NULL AND date(l.createdon) <= lpd.lead_cutoff_date)
        OR
        -- No property: use 11/6/2023 cutoff
        (lpd.ah4r_leadsid IS NULL AND date(l.createdon) <= '2023-08-06')
    )
""")
leads_filtered.createOrReplaceTempView("leads_filtered")

# Verify leads without properties
no_property_check = spark.sql("""
SELECT 
    COUNT(*) as leads_without_properties,
    MAX(createdon) as max_created_date
FROM leads_filtered l
WHERE NOT EXISTS (
    SELECT 1 FROM lead_property_filtered lp 
    WHERE lp.ah4r_leadsid = l.leadid
)
""").collect()[0]

print(f"Leads without properties: {no_property_check['leads_without_properties']:,}")
print(f"Max date for leads without properties: {no_property_check['max_created_date']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Consolidated Leads (One Per Email)

# COMMAND ----------

# Get first record per email
first_records = spark.sql("""
WITH ordered_records AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(emailaddress1))
            ORDER BY createdon ASC
        ) as rn
    FROM leads_filtered
)
SELECT * FROM ordered_records orr WHERE orr.rn = 1 
""")
first_records.createOrReplaceTempView("first_records")

# Get enrichment data
# enrichment_data = spark.sql("""
# SELECT 
#     LOWER(TRIM(emailaddress1)) as email_key,
#     get(COLLECT_LIST(CASE WHEN firstname IS NOT NULL AND firstname != '' THEN firstname END), 0) as best_firstname,
#     get(COLLECT_LIST(CASE WHEN lastname IS NOT NULL AND lastname != '' THEN lastname END), 0) as best_lastname,
#     get(COLLECT_LIST(CASE WHEN mobilephone IS NOT NULL AND mobilephone != '' THEN mobilephone END), 0) as best_mobilephone,
#     get(COLLECT_LIST(CASE WHEN telephone1 IS NOT NULL AND telephone1 != '' THEN telephone1 END), 0) as best_telephone1,
#     get(COLLECT_LIST(CASE WHEN source_name IS NOT NULL THEN source_name END), 0) as best_source_name,
#     COUNT(*) as total_records
# FROM leads_filtered
# GROUP BY LOWER(TRIM(emailaddress1))
# """)
# enrichment_data.createOrReplaceTempView("enrichment_data")

# Create final leads
final_leads = spark.sql("""
SELECT 
    ROW_NUMBER() OVER (ORDER BY f.createdon) as lead_key,
    capitalize_name(f.firstname) as lead_first_name,
    capitalize_name(f.lastname) as lead_last_name,
    format_email_lowercase(f.emailaddress1) as lead_email,
    format_phone_e164(f.telephone1) as lead_telephone1,
    format_phone_e164(f.telephone2) as lead_telephone2,
    format_phone_e164(f.mobilephone) as lead_mobilephone,
    IFNULL(f.source_name, '') as lead_initial_referral_source,
    f.createdon as lead_created_on,
    IFNULL(f.createdbyname, '') as lead_created_by,
    CASE 
        WHEN f.statecode = 0 THEN 'Open'
        WHEN f.statecode = 1 THEN 'Qualified'
        WHEN f.statecode = 2 THEN 'Disqualified'
        ELSE 'Unknown'
    END as lead_state,
    CASE 
        WHEN f.statecode = 0 AND f.statuscode = 1 THEN 'Lead'
        WHEN f.statecode = 1 THEN 'Qualified'
        WHEN f.statecode = 2 THEN 'Disqualified'
        ELSE 'Unknown'
    END as lead_status,
    IFNULL(f.ah4r_preferredlocation, '') as lead_preferred_location,
    IFNULL(f.ah4r_moveindate, CAST('1900-01-01' AS TIMESTAMP)) as lead_move_in_date,
    f.leadid as lead_source_system_id,
    'Legacy CRM' as lead_source_system,
    f.modifiedon as lead_last_activity_date,
    CURRENT_TIMESTAMP() as created_timestamp,
    CURRENT_TIMESTAMP() as modified_timestamp,
    0 as exclusion_flag
FROM first_records f
--INNER JOIN enrichment_data e 
    --ON LOWER(TRIM(f.emailaddress1)) = e.email_key
WHERE NOT EXISTS (
    SELECT 1
    FROM dsvc_uat_gov_fin.stg_d365.stg_d365_contact dc
    WHERE lower(trim(dc.emailaddress1)) = lower(trim(f.emailaddress1))
      AND date(dc.createdon) < date(f.createdon)
)
""")
final_leads.createOrReplaceTempView("final_leads")

lead_count = final_leads.count()
print(f"✅ Created {lead_count:,} consolidated leads")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Lead Mapping and Property Events

# COMMAND ----------

# Create lead ID mapping
lead_id_mapping = spark.sql("""
SELECT DISTINCT
    l.leadid,
    f.lead_key,
    f.lead_email
FROM leads_filtered l
INNER JOIN final_leads f
    ON LOWER(TRIM(l.emailaddress1)) = f.lead_email
""")
lead_id_mapping.createOrReplaceTempView("lead_id_mapping")

# Create property events - already deduplicated
property_events = spark.sql(f"""
SELECT 
    ROW_NUMBER() OVER (ORDER BY lp.createdon, lp.ah4r_leadpropertyid) as lead_event_key,
    m.lead_key,
    CAST(DATE_FORMAT(lp.createdon, 'yyyyMMdd') AS BIGINT) as created_on_dt_key,
    COALESCE(lp.property_key, -1) as property_key,
    'Property Interest' as event_type,
    'Legacy Lead Property' as referral_source,
    COALESCE(lp.new_favoritesdatetime, CAST('1900-01-01' AS TIMESTAMP)) as favorite_date,
    lp.createdon as created_on_time,
    CURRENT_TIMESTAMP() as created_timestamp,
    CURRENT_TIMESTAMP() as modified_timestamp
FROM lead_property_filtered lp
INNER JOIN lead_id_mapping m ON lp.ah4r_leadsid = m.leadid
""")

event_count = property_events.count()
property_mapped = property_events.filter("property_key != -1").count()
print(f"✅ Created {event_count:,} property events")
print(f"   With property keys: {property_mapped:,} ({property_mapped/event_count*100:.1f}%)")

access_code_events = spark.sql("""
SELECT 
    --ROW_NUMBER() OVER (ORDER BY lp.access_code_generated_at, lp.ah4r_leadpropertyid) AS lead_event_key,
    m.lead_key,
    CAST(DATE_FORMAT(lp.ah4r_chekedindatetime, 'yyyyMMdd') AS BIGINT)             AS created_on_dt_key,
    COALESCE(lp.property_key, -1)                                                    AS property_key,
    'Access Code Generated'                                                          AS event_type,
    'Legacy Lead Property'                                                           AS referral_source,
    CAST('1900-01-01' AS TIMESTAMP)                                                  AS favorite_date,
    lp.ah4r_chekedindatetime                                                         AS created_on_time,
    CURRENT_TIMESTAMP()                                                              AS created_timestamp,
    CURRENT_TIMESTAMP()                                                              AS modified_timestamp
FROM lead_property_filtered lp
INNER JOIN lead_id_mapping m
    ON lp.ah4r_leadsid = m.leadid
WHERE lp.ah4r_chekedindatetime IS NOT NULL
  --AND DATE(lp.ah4r_chekedindatetime) <= lp.cutoff_date   -- keep within legacy window
""")

access_count = access_code_events.count()
print(f"✅ Created {access_count:,} access-code events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Insert Data

# COMMAND ----------

# Insert leads
final_leads.select(
    "lead_key",
    "lead_first_name",
    "lead_last_name",
    "lead_email",
    "lead_telephone1",
    "lead_telephone2",
    "lead_mobilephone",
    "lead_initial_referral_source",
    "lead_created_on",
    "lead_created_by",
    "lead_state",
    "lead_status",
    "lead_preferred_location",
    "lead_move_in_date",
    "lead_source_system_id",
    "lead_source_system",
    "lead_last_activity_date",
    "created_timestamp",
    "modified_timestamp",
    "exclusion_flag"
).write.mode("append").insertInto(f"{UAT_SCHEMA}.dim_lead")

print(f"✅ Inserted {lead_count:,} leads")

# Insert events
property_events.write.mode("append").insertInto(f"{UAT_SCHEMA}.ft_lead_event")
print(f"✅ Inserted {event_count:,} events")

# access_code_events.write.mode("append").insertInto(f"{UAT_SCHEMA}.ft_lead_event")
# print(f"✅ Inserted {access_count:,} access-code events")

# COMMAND ----------

access_code_events.createOrReplaceTempView("access_code_events")

# 2) Insert, naming ONLY the non-identity columns in the target
spark.sql(f"""
INSERT INTO {UAT_SCHEMA}.ft_lead_event (
  lead_key,
  created_on_dt_key,
  property_key,
  event_type,
  referral_source,
  favorite_date,
  created_on_time,
  created_timestamp,
  modified_timestamp
)
SELECT
  lead_key,
  created_on_dt_key,
  property_key,
  event_type,
  referral_source,
  favorite_date,
  created_on_time,
  created_timestamp,
  modified_timestamp
FROM access_code_events
""")