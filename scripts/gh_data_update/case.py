from enum import Enum
import json
import os


GH_CASE_TEMPLATE_JSON = os.environ.get("GH_CASE_TEMPLATE_JSON", "gh_data_template.json")


def read_case_template_json():
    with open(GH_CASE_TEMPLATE_JSON) as fh:
        return json.load(fh)


EMPTY_CASE_AS_DICT = read_case_template_json()


class PersonAgeSexGender():

    def __init__(self, age="", sb="", sbo="", gn="", go=""):
        self.age = age
        self.sex_at_birth = sb
        self.sex_at_birth_other = sbo
        self.gender = gn
        self.gender_other = go


class PersonRaceEthnicityNationality():

    def __init__(self, rc="", ro="", et="", eo="", ny="", no=""):
        self.race = rc
        self.race_other = ro
        self.ethnicity = et
        self.ethnicity_other = eo
        self.nationality = ny
        self.nationality_other = no


class PersonDemographics():

    def __init__(self, asg=PersonAgeSexGender(), ren=PersonRaceEthnicityNationality(), li="", oc="", hw=""):
        self.age_sex_gender = asg
        self.race_ethnicity_nationality = ren
        self.location_information = li
        self.occupation = oc
        self.healthcare_worker = hw


class CaseDemographics():

    def __init__(self, pd=PersonDemographics(), cs="", pa="", ps=""):
        self.person_demographics = pd
        self.case_status = cs
        self.pathogen = pa
        self.pathogen_status = ps


class InfectionHistory():

    def __init__(self, pi="", ci=""):
        self.previous_infection = pi
        self.co_infection = ci


class VaccinationHistory():

    def __init__(self, vx="", vn="", vd="", vs=""):
        self.vaccination = vx
        self.vaccine_name = vn
        self.vaccination_date = vd
        self.vaccine_side_effects = vs


class MedicalHistory():

    def __init__(self, ih=InfectionHistory(), vh=VaccinationHistory(), pc="", ps=""):
        self.infection_history = ih
        self.vaccination_history = vh
        self.preexisting_conditions = pc
        self.pregnancy_status = ps


class SymptomsHistory():

    def __init__(self, sm="", do=""):
        self.symptoms = sm
        self.date_onset = do


class ConfirmationHistory():

    def __init__(self, dc="", cm=""):
        self.date_confirmation = dc
        self.confirmation_method = cm


class ICUHistory():

    def __init__(self, ic="", da="", dd=""):
        self.intensive_care = ic
        self.date_admission_icu = da
        self.date_discharge_icu = dd


class HospitalizationHistory():

    def __init__(self, ih=ICUHistory(), hp="", rh="", hd="", dd=""):
        self.icu_history = ih
        self.hospitalized = hp
        self.reason_for_hospitalization = rh
        self.date_hospitalization = hd
        self.date_discharge_hospital = dd


class IsolationHistory():

    def __init__(self, ie="", di=""):
        self.isolated = ie
        self.date_isolation = di


class Outcome():

    def __init__(self, oc="", dd="", dr=""):
        self.outcome = oc
        self.date_death = dd
        self.date_recovered = dr


class ClinicalPresentation():

    def __init__(self, sh=SymptomsHistory(), ch=ConfirmationHistory(), hh=HospitalizationHistory(), ih=IsolationHistory(), oc=Outcome(), hm=""):
        self.symptoms_history = sh
        self.confirmation_history = ch
        self.hospitalization_history = hh
        self.isolation_history = ih
        self.outcome = oc
        self.home_monitoring = hm


class ContactHistory():

    def __init__(self, cw="", cd="", cs="", so="", ca="", cc=""):
        self.contact_with_case = cw
        self.contact_id = cd
        self.contact_setting = cs
        self.contact_setting_other = so
        self.contact_animal = ca
        self.contact_comment = cc


class TravelHistory():

    def __init__(self, th="", te="", ts="", tl=""):
        self.travel_history = th
        self.travel_history_entry = te
        self.travel_history_start = ts
        self.travel_history_location = tl


class Exposure():

    def __init__(self, ch=ContactHistory(), th=TravelHistory(), tr=""):
        self.contact_history = ch
        self.travel_history = th
        self.transmission = tr


class LaboratoryInformation():

    def __init__(self, gm="", an=""):
        self.genomics_metadata = gm
        self.accession_number = an


class Sources():

    def __init__(self, sa="", sb="", sc="", sd=""):
        self.source_i = sa
        self.source_ii = sb
        self.source_iii = sc
        self.source_iv = sd


class SourceInformation():

    def __init__(self, ss=Sources(), de="", dm=""):
        self.sources = ss
        self.date_entry = de
        self.date_last_modified = dm


class Case():

    def __init__(self, cd=CaseDemographics(), mh=MedicalHistory(), cp=ClinicalPresentation(), ex=Exposure(), li=LaboratoryInformation(), si=SourceInformation(), cid=""):
        self.case_demographics = cd
        self.medical_history = mh
        self.clinical_presentation = cp
        self.exposure = ex
        self.laboratory_information = li
        self.source_information = si
        self.case_id = cid


# FIXME: keys -> constants
def dict_to_case(case: dict) -> Case:
    # input: flat dict
    # output: Case instance
    asg = PersonAgeSexGender(case["Age"], case["Sex_at_birth"], case["Sex_at_birth_other"], case["Gender"], case["Gender_other"])
    ren = PersonRaceEthnicityNationality(case["Race"], case["Race_other"], case["Ethnicity"], case["Ethnicity_other"], case["Nationality"], case["Nationality_other"])
    pd = PersonDemographics(asg, ren, case["Location_information"], case["Occupation"], case["Healthcare_worker"])
    cd = CaseDemographics(pd, case["Case_status"], case["Pathogen"], case["Pathogen_status"])
    ih = InfectionHistory(case["Previous_infection"], case["Co_infection"])
    vh = VaccinationHistory(case["Vaccination"], case["Vaccine_name"], case["Vaccination_date"], case["Vaccine_side_effects"])
    mh = MedicalHistory(ih, vh, case["Pre_existing_condition"], case["Pregnancy_Status"])
    sh = SymptomsHistory(case["Symptoms"], case["Date_onset"])
    ch = ConfirmationHistory(case["Date_confirmation"], case["Confirmation_method"])
    uh = ICUHistory(case["Intensive_care"], case["Date_admission_ICU"], case["Date_discharge_ICU"])
    hh = HospitalizationHistory(uh, case["Hospitalized"], case["Reason_for_hospitalization"], case["Date_hospitalization"], case["Date_discharge_hospital"])
    oh = IsolationHistory(case["Isolated"], case["Date_isolation"])
    oc = Outcome(case["Outcome"], case["Date_death"], case["Date_recovered"])
    cp = ClinicalPresentation(sh, ch, hh, oh, oc, case["Home_monitoring"])
    ah = ContactHistory(case["Contact_with_case"], case["Contact_ID"], case["Contact_setting"], case["Contact_setting_other"], case["Contact_animal"], case["Contact_comment"])
    th = TravelHistory(case["Travel_history"], case["Travel_history_entry"], case["Travel_history_start"], case["Travel_history_location"])
    ex = Exposure(ah, th, case["Transmission"])
    li = LaboratoryInformation(case["Genomics_Metadata"], case["Accession_Number"])
    ss = Sources(case["Source"], case["Source_II"], case["Source_III"], case["Source_IV"])
    si = SourceInformation(ss, case["Date_entry"], case["Date_last_modified"])
    return Case(cd, mh, cp, ex, li, si, case["ID"])


def case_to_dict(case: Case) -> dict:
    c = EMPTY_CASE_AS_DICT.copy()
    c["Age"] = case.case_demographics.person_demographics.age_sex_gender.age
    c["Sex_at_birth"] = case.case_demographics.person_demographics.age_sex_gender.sex_at_birth
    c["Sex_at_birth_other"] = case.case_demographics.person_demographics.age_sex_gender.sex_at_birth_other
    c["Gender"] = case.case_demographics.person_demographics.age_sex_gender.gender
    c["Gender_other"] = case.case_demographics.person_demographics.age_sex_gender.gender_other
    c["Race"] = case.case_demographics.person_demographics.race_ethnicity_nationality.race
    c["Race_other"] = case.case_demographics.person_demographics.race_ethnicity_nationality.race_other
    c["Ethnicity"] = case.case_demographics.person_demographics.race_ethnicity_nationality.ethnicity
    c["Ethnicity_other"] = case.case_demographics.person_demographics.race_ethnicity_nationality.ethnicity_other
    c["Nationality"] = case.case_demographics.person_demographics.race_ethnicity_nationality.nationality
    c["Nationality_other"] = case.case_demographics.person_demographics.race_ethnicity_nationality.nationality_other
    c["Location_information"] = case.case_demographics.person_demographics.location_information
    c["Occupation"] = case.case_demographics.person_demographics.occupation
    c["Healthcare_worker"] = case.case_demographics.person_demographics.healthcare_worker
    c["Case_status"] = case.case_demographics.case_status
    c["Pathogen"] = case.case_demographics.pathogen
    c["Pathogen_status"] = case.case_demographics.pathogen_status
    c["Previous_infection"] = case.medical_history.infection_history.previous_infection
    c["Co_infection"] = case.medical_history.infection_history.co_infection
    c["Vaccination"] = case.medical_history.vaccination_history.vaccination
    c["Vaccine_name"] = case.medical_history.vaccination_history.vaccine_name
    c["Vaccination_date"] = case.medical_history.vaccination_history.vaccination_date
    c["Vaccine_side_effects"] = case.medical_history.vaccination_history.vaccine_side_effects
    c["Pre_existing_condition"] = case.medical_history.preexisting_conditions
    c["Pregnancy_Status"] = case.medical_history.pregnancy_status
    c["Symptoms"] = case.clinical_presentation.symptoms_history.symptoms
    c["Date_onset"] = case.clinical_presentation.symptoms_history.date_onset
    c["Date_confirmation"] = case.clinical_presentation.confirmation_history.date_confirmation
    c["Confirmation_method"] = case.clinical_presentation.confirmation_history.confirmation_method
    c["Intensive_care"] = case.clinical_presentation.hospitalization_history.icu_history.intensive_care
    c["Date_admission_ICU"] = case.clinical_presentation.hospitalization_history.icu_history.date_admission_icu
    c["Date_discharge_ICU"] = case.clinical_presentation.hospitalization_history.icu_history.date_discharge_icu
    c["Hospitalized"] = case.clinical_presentation.hospitalization_history.hospitalized
    c["Reason_for_hospitalization"] = case.clinical_presentation.hospitalization_history.reason_for_hospitalization
    c["Date_hospitalization"] = case.clinical_presentation.hospitalization_history.date_hospitalization
    c["Date_discharge_hospital"] = case.clinical_presentation.hospitalization_history.date_discharge_hospital
    c["Isolated"] = case.clinical_presentation.isolation_history.isolated
    c["Date_isolation"] = case.clinical_presentation.isolation_history.date_isolation
    c["Outcome"] = case.clinical_presentation.outcome.outcome
    c["Date_death"] = case.clinical_presentation.outcome.date_death
    c["Date_recovered"] = case.clinical_presentation.outcome.date_recovered
    c["Home_monitoring"] = case.clinical_presentation.home_monitoring
    c["Contact_with_case"] = case.exposure.contact_history.contact_with_case
    c["Contact_ID"] = case.exposure.contact_history.contact_id
    c["Contact_setting"] = case.exposure.contact_history.contact_setting
    c["Contact_setting_other"] = case.exposure.contact_history.contact_setting_other
    c["Contact_animal"] = case.exposure.contact_history.contact_animal
    c["Contact_comment"] = case.exposure.contact_history.contact_comment
    c["Travel_history"] = case.exposure.travel_history.travel_history
    c["Travel_history_entry"] = case.exposure.travel_history.travel_history_entry
    c["Travel_history_start"] = case.exposure.travel_history.travel_history_start
    c["Travel_history_location"] = case.exposure.travel_history.travel_history_location
    c["Transmission"] = case.exposure.transmission
    c["Genomics_Metadata"] = case.laboratory_information.genomics_metadata
    c["Accession_Number"] = case.laboratory_information.accession_number
    c["Source"] = case.source_information.sources.source_i
    c["Source_II"] = case.source_information.sources.source_ii
    c["Source_III"] = case.source_information.sources.source_iii
    c["Source_IV"] = case.source_information.sources.source_iv
    c["Date_entry"] = case.source_information.date_entry
    c["Date_last_modified"] = case.source_information.date_last_modified

    return c
