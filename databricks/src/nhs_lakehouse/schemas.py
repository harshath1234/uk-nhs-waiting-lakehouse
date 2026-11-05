"""Source schemas, checked against the real files on 5 Nov 2026 (see docs/source_quirks.md)."""

from __future__ import annotations

# ---------------------------------------------------------------- RTT full extract
RTT_ID_COLUMNS: list[tuple[str, str]] = [
    ("Period", "period_label"),
    ("Provider Parent Org Code", "provider_parent_code"),
    ("Provider Parent Name", "provider_parent_name"),
    ("Provider Org Code", "provider_code"),
    ("Provider Org Name", "provider_name"),
    ("Commissioner Parent Org Code", "commissioner_parent_code"),
    ("Commissioner Parent Name", "commissioner_parent_name"),
    ("Commissioner Org Code", "commissioner_code"),
    ("Commissioner Org Name", "commissioner_name"),
    ("RTT Part Type", "part_type"),
    ("RTT Part Description", "part_description"),
    ("Treatment Function Code", "treatment_function_code"),
    ("Treatment Function Name", "treatment_function_name"),
]
N_BANDS = 105  # >0-1 weeks ... >103-104 weeks, then >104 weeks


def band_source_name(i: int) -> str:
    return "Gt 104 Weeks SUM 1" if i == 104 else f"Gt {i:02d} To {i + 1:02d} Weeks SUM 1"


def band_column(i: int) -> str:
    return f"wb_{i:03d}"


RTT_BAND_COLUMNS: list[tuple[str, str]] = [(band_source_name(i), band_column(i)) for i in range(N_BANDS)]
RTT_TOTAL_COLUMNS: list[tuple[str, str]] = [
    ("Total", "total_known_start"),
    ("Patients with unknown clock start date", "unknown_clock_start"),
    ("Total All", "total_all"),
]
RTT_COLUMNS = RTT_ID_COLUMNS + RTT_BAND_COLUMNS + RTT_TOTAL_COLUMNS  # 121 columns
RTT_SUBTOTAL_TF = "C_999"  # "Total" treatment function: a subtotal row inside the file
RTT_PARTS_WITH_BANDS = ("Part_1A", "Part_1B", "Part_2", "Part_2A")  # Part_3 has only Total All
RTT_PARTS = {
    "Part_1A": "Completed pathways, admitted",
    "Part_1B": "Completed pathways, non-admitted",
    "Part_2": "Incomplete pathways",
    "Part_2A": "Incomplete pathways with decision to admit",
    "Part_3": "New RTT periods",
}

# ---------------------------------------------------------------- A&E monthly CSV
AE_COLUMNS: list[tuple[str, str]] = [
    ("Period", "period_label"),
    ("Org Code", "org_code"),
    ("Parent Org", "parent_org"),
    ("Org name", "org_name"),
    ("A&E attendances Type 1", "att_type1"),
    ("A&E attendances Type 2", "att_type2"),
    ("A&E attendances Other A&E Department", "att_other"),
    ("A&E attendances Booked Appointments Type 1", "att_booked_type1"),
    ("A&E attendances Booked Appointments Type 2", "att_booked_type2"),
    ("A&E attendances Booked Appointments Other Department", "att_booked_other"),
    ("Attendances over 4hrs Type 1", "over4h_type1"),
    ("Attendances over 4hrs Type 2", "over4h_type2"),
    ("Attendances over 4hrs Other Department", "over4h_other"),
    ("Attendances over 4hrs Booked Appointments Type 1", "over4h_booked_type1"),
    ("Attendances over 4hrs Booked Appointments Type 2", "over4h_booked_type2"),
    ("Attendances over 4hrs Booked Appointments Other Department", "over4h_booked_other"),
    ("Patients who have waited 4-12 hs from DTA to admission", "dta_4_to_12h"),  # "hs" typo is real
    ("Patients who have waited 12+ hrs from DTA to admission", "dta_12h_plus"),
    ("Emergency admissions via A&E - Type 1", "emerg_adm_type1"),
    ("Emergency admissions via A&E - Type 2", "emerg_adm_type2"),
    ("Emergency admissions via A&E - Other A&E department", "emerg_adm_other"),
    ("Other emergency admissions", "emerg_adm_not_via_ae"),
]
AE_ATTENDANCE_COLS = [
    "att_type1",
    "att_type2",
    "att_other",
    "att_booked_type1",
    "att_booked_type2",
    "att_booked_other",
]
AE_OVER4H_COLS = [
    "over4h_type1",
    "over4h_type2",
    "over4h_other",
    "over4h_booked_type1",
    "over4h_booked_type2",
    "over4h_booked_other",
]
AE_TOTAL_MARKER = "TOTAL"

# ---------------------------------------------------------------- ODS etr (no header row, 27 columns)
ODS_ETR_COLUMNS = [
    "org_code",
    "name",
    "national_grouping",
    "high_level_health_geography",
    "address_line_1",
    "address_line_2",
    "address_line_3",
    "address_line_4",
    "address_line_5",
    "postcode",
    "open_date",
    "close_date",
    "null_12",
    "null_13",
    "null_14",
    "null_15",
    "null_16",
    "contact_telephone",
    "null_18",
    "null_19",
    "null_20",
    "amended_record_indicator",
    "null_22",
    "gor_code",
    "null_24",
    "null_25",
    "null_26",
]
NHS_REGIONS = {
    "Y56": "London",
    "Y58": "South West",
    "Y59": "South East",
    "Y60": "Midlands",
    "Y61": "East of England",
    "Y62": "North West",
    "Y63": "North East and Yorkshire",
}

MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "JANUARY",
            "FEBRUARY",
            "MARCH",
            "APRIL",
            "MAY",
            "JUNE",
            "JULY",
            "AUGUST",
            "SEPTEMBER",
            "OCTOBER",
            "NOVEMBER",
            "DECEMBER",
        ],
        start=1,
    )
}
