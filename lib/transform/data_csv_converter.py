import os

import pandas as pd

from lib.tracking_decorator import TrackingDecorator


@TrackingDecorator.track_time
def convert_data_to_csv(source_path, results_path, clean=False, quiet=False):
    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):
        # Make results path
        subdir = subdir.replace(f"{source_path}/", "")
        os.makedirs(os.path.join(results_path, subdir), exist_ok=True)

        for file_name in [file_name for file_name in sorted(files)
                          if not file_name.startswith(f"~") and
                             (file_name.endswith(".xlsx") or file_name.endswith(".xls"))]:
            source_file_path = os.path.join(source_path, subdir, file_name)

            convert_file_to_csv_permits_new_buildings_including_measures_on_existing_buildings(
                source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_new_residential_buildings(source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_type_and_contractor_including_measures_on_existing_buildings(
                source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_type_and_contractor_measures_on_existing_buildings(
                source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_type_and_contractor_new_buildings(source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_district_including_measures_on_existing_buildings(
                source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_district_measures_on_existing_buildings(
                source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_district_new_buildings(source_file_path, clean=clean, quiet=quiet)
            convert_file_to_csv_permits_by_district_new_buildings_with_1_or_2_apartments(
                source_file_path, clean=clean, quiet=quiet)


def convert_file_to_csv_permits_new_buildings_including_measures_on_existing_buildings(source_file_path, clean=False,
                                                                                       quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-1-permits-including-measures-on-existing-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        # Iterate over sheets
        sheet = "Baugen. Tab. 1"
        skiprows = 10
        names = ["year", "building_permits_total", "building_permits_residential_buildings",
                 "building_permits_non_residential_buildings",
                 "building_measure_on_existing_buildings", "usage_area", "living_area", "apartments",
                 "apartment_rooms", "estimated_costs"]
        drop_columns = ["year"]

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .replace("…", None) \
            .dropna() \
            .tail(1)

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_new_residential_buildings(source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-2-permits-new-residential-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        # Iterate over sheets
        sheet = "Baugen. Tab. 2 "
        skiprows = 10
        names = ["year", "residential_buildings", "residential_buildings_with_1_apartment",
                 "residential_buildings_with_2_apartments", "residential_buildings_with_3_or_more_apartments",
                 "apartments", "volume", "living_area", "estimated_costs",
                 "apartments_in_new_non_residential_buildings"]
        drop_columns = ["year"]

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .replace("-", 0) \
            .replace("…", None) \
            .dropna() \
            .tail(1)

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_type_and_contractor_including_measures_on_existing_buildings(source_file_path,
                                                                                                clean=False,
                                                                                                quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-3-permits-by-type-and-constructor-including-measures-on-existing-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 3"
        skiprows = 7
        names = ["type", "measures", "usage_area", "apartments", "living_area", "living_rooms", "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna() \
            .replace("–", 0) \
            .assign(type=lambda df: df["type"].apply(lambda row: build_type_name(row)))

        dataframe.reset_index(drop=True, inplace=True)
        dataframe = dataframe.assign(type_index=lambda df: df.index) \
            .assign(type_parent_index=lambda df: df.apply(lambda row: build_type_parent_index_3(row), axis=1)) \
            .fillna(-1) \
            .assign(type_parent_index=lambda df: df["type_parent_index"].astype(int))
        dataframe.insert(0, "type_index", dataframe.pop("type_index"))
        dataframe.insert(1, "type_parent_index", dataframe.pop("type_parent_index"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_type_and_contractor_measures_on_existing_buildings(source_file_path, clean=False,
                                                                                      quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-4-permits-by-type-and-constructor-measures-on-existing-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 4"
        skiprows = 7
        names = ["type", "buildings", "usage_area", "apartments", "living_area", "living_rooms", "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna() \
            .replace("–", 0) \
            .assign(type=lambda df: df["type"].apply(lambda row: build_type_name(row)))

        dataframe.reset_index(drop=True, inplace=True)
        dataframe = dataframe.assign(type_index=lambda df: df.index) \
            .assign(type_parent_index=lambda df: df.apply(lambda row: build_type_parent_index_4(row), axis=1)) \
            .fillna(-1) \
            .assign(type_parent_index=lambda df: df["type_parent_index"].astype(int))
        dataframe.insert(0, "type_index", dataframe.pop("type_index"))
        dataframe.insert(1, "type_parent_index", dataframe.pop("type_parent_index"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_type_and_contractor_new_buildings(source_file_path, clean=False, quiet=False):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-5-permits-by-type-and-constructor-new-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 5 "
        skiprows = 7
        names = ["type", "buildings", "volume", "usage_area", "apartments", "living_area", "living_rooms",
                 "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .dropna() \
            .replace("–", 0) \
            .assign(type=lambda df: df["type"].apply(lambda row: build_type_name(row)))

        dataframe.reset_index(drop=True, inplace=True)
        dataframe = dataframe.assign(type_index=lambda df: df.index) \
            .assign(type_parent_index=lambda df: df.apply(lambda row: build_type_parent_index_5(row), axis=1)) \
            .fillna(-1) \
            .assign(type_parent_index=lambda df: df["type_parent_index"].astype(int))
        dataframe.insert(0, "type_index", dataframe.pop("type_index"))
        dataframe.insert(1, "type_parent_index", dataframe.pop("type_parent_index"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_district_including_measures_on_existing_buildings(source_file_path, clean,
                                                                                     quiet):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-6-permits-by-districts-including-measures-on-existing-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 6"
        skiprows = 8
        names = ["district_name", "buildings", "usage_area", "apartments", "apartments_usage_area", "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .replace("–", 0) \
            .assign(district_id=lambda df: df["district_name"].apply(lambda row: build_district_id(row))) \
            .head(12) \
            .drop("district_name", axis=1)

        dataframe.reset_index(drop=True, inplace=True)
        dataframe.insert(0, "district_id", dataframe.pop("district_id"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_district_measures_on_existing_buildings(source_file_path, clean,
                                                                           quiet):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-7-permits-by-districts-measures-on-existing-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 7"
        skiprows = 8
        names = ["district_name", "buildings", "usage_area", "apartments", "apartments_usage_area", "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .replace("–", 0) \
            .assign(district_id=lambda df: df["district_name"].apply(lambda row: build_district_id(row))) \
            .head(12) \
            .drop("district_name", axis=1)

        dataframe.reset_index(drop=True, inplace=True)
        dataframe.insert(0, "district_id", dataframe.pop("district_id"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_district_new_buildings(source_file_path, clean, quiet):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-8-permits-by-districts-new-buildings.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 8"
        skiprows = 8
        names = ["district_name", "buildings", "volume", "usage_area", "apartments", "apartments_usage_area",
                 "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .replace("–", 0) \
            .assign(district_id=lambda df: df["district_name"].apply(lambda row: build_district_id(row))) \
            .head(12) \
            .drop("district_name", axis=1)

        dataframe.reset_index(drop=True, inplace=True)
        dataframe.insert(0, "district_id", dataframe.pop("district_id"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


def convert_file_to_csv_permits_by_district_new_buildings_with_1_or_2_apartments(source_file_path, clean, quiet):
    source_file_name, source_file_extension = os.path.splitext(source_file_path)
    file_path_csv = f"{source_file_name}-9-permits-by-district-new-buildings-with-1-or-2-apartments.csv"

    # Check if result needs to be generated
    if not clean and os.path.exists(file_path_csv):
        if not quiet:
            print(f"✓ Already exists {os.path.basename(file_path_csv)}")
        return

    # Determine engine
    engine = build_engine(source_file_extension)

    try:
        sheet = "Baugen. Tab. 9"
        skiprows = 7
        names = ["district_name", "buildings", "volume", "usage_area", "apartments", "apartments_usage_area",
                 "estimated_costs"]
        drop_columns = []

        dataframe = pd.read_excel(source_file_path, engine=engine, sheet_name=sheet, skiprows=skiprows, names=names,
                                  index_col=False) \
            .drop(columns=drop_columns, errors="ignore") \
            .replace("–", 0) \
            .assign(district_id=lambda df: df["district_name"].apply(lambda row: build_district_id(row))) \
            .head(12) \
            .drop("district_name", axis=1)

        dataframe.reset_index(drop=True, inplace=True)
        dataframe.insert(0, "district_id", dataframe.pop("district_id"))

        # Write csv file
        write_csv_file(dataframe, file_path_csv, quiet)
    except Exception as e:
        print(f"✗️ Exception: {str(e)}")


#
# Transformers
#

def build_type_name(value):
    value = str(value).lstrip().rstrip()

    if value == "Wohn- und Nichtwohngebäude":
        return "residential_and_non_residential_buildings"

    elif value == "Wohngebäude":
        return "residential_buildings"
    elif value == "Wohngebäude zusammen":
        return "residential_buildings"
    elif value == "Wohnheime":
        return "dormitories"
    elif value == "Wohngebäude mit Eigentumswohnungen" or value == "Wohngeb. m. Eigentumswohn." or value == "Wohngeb. mit Eigentumswohnungen":
        return "residential_buildings_with_condominium"
    elif value == "Wohngebäude mit 1 Wohnung":
        return "residential_buildings_with_1_apartment"
    elif value == "Wohngebäude mit 2 Wohnungen":
        return "residential_buildings_with_2_apartments"
    elif value == "Wohngebäude mit 3 o. m. Wohnungen" or value == "Wohngeb. mit 3 o. m. Wohnungen":
        return "residential_buildings_with_3_or_more_apartments"
    elif value == "darin: Wohnungen":
        return "apartments"
    elif value == "darin: Rauminhalt 1 000 m³" or value == "Rauminhalt" or value == "darin: Rauminhalt 1000 m³":
        return "volume_1000_m3"
    elif value == "landwirtschaftliche Betriebsgebäude":
        return "agricultural_buildings"
    elif value == "nichtlandwirtschaftliche Betriebsgebäude":
        return "non_agricultural_buildings"
    elif value == "sonstige Nichtwohngebäude":
        return "other_non_residential_buildings"
    elif value == "ausgewählte Infrastrukturgebäude":
        return "selected_infrastructure_buildings"

    elif value == "Nichtwohngebäude":
        return "non_residential_buildings"
    elif value == "Anstaltsgebäude":
        return "institution_buildings"
    elif value == "Büro- und Verwaltungsgebäude":
        return "office_and_administration_buildings"
    elif value == "Landwirtschaftliche Betriebsgebäude" or value == "landwirtschaftl. Betriebsgebäude":
        return "algricultural_buildings"
    elif value == "Nichtlandwirtschaftliche Betriebsgebäude" or value == "nichtlandwirtschaftl. Betriebsgeb.":
        return "non_algricultural_buildings"
    elif value == "Fabrik- und Werkstattgebäude":
        return "factory_and_workshop_buildings"
    elif value == "Handelsgebäude":
        return "commercial_buildings"
    elif value == "Warenlagergebäude":
        return "warehouse_buildings"
    elif value == "Hotels und Gaststätten":
        return "hotels_and_restaurants"
    elif value == "Sonstige Nichtwohngebäude":
        return "other_non_residential_buildings"
    elif value == "Ausgewählte Infrastrukturgebäude":
        return "selected_infrastructure_buildings"

    elif value == "Öffentliche Bauherren":
        return "public_builders"
    elif value == "Unternehmen":
        return "companies"
    elif value == "Wohnungsunternehmen":
        return "housing_companies"
    elif value == "Immobilienfonds":
        return "real_estate_funds"
    elif value == "Land- und Forstw., Tierh., Fischerei":
        return "agriculture_forestry_animal_husbandry_fishing"
    elif value == "Produzierendes Gewerbe":
        return "manufacturing_industry"
    elif value == "Handel, Kreditinst., Dienstleistung,\n Versicherung, Verkehr u. Nachr.überm.":
        return "trade_banking_services_insurance_transport_and_communications"
    elif value == "Private Haushalte":
        return "private_households"
    elif value == "Organisationen ohne Erwerbszweck":
        return "non_profit_organizations"
    elif value == "Organisationen o. Erwerbszweck":
        return "non_profit_organisations"

    elif value == "II. Halbjahr 2021":
        return "2021 half-year 2"
    elif value == "I.  Halbjahr 2021":
        return "2021 half-year 1"
    elif value == "II. Halbjahr 2020":
        return "2020 half-year 2"
    elif value == "I.  Halbjahr 2020":
        return "2020 half-year 1"
    elif value == "II. Halbjahr 2019":
        return "2019 half-year 2"
    elif value == "I.  Halbjahr 2019":
        return "2019 half-year 1"
    elif value == "Jahr 2019":
        return "2019"
    elif value == "Jahr 2018":
        return "2018"
    elif value == "2018 und früher":
        return "2018 and earlier"
    elif value == "2017 und früher":
        return "2017 and earlier"

    elif value == "vor  1919":
        return "before 1919"
    elif value == "von 1919 bis 1948":
        return "between 1919 and 1948"
    elif value == "von 1949 bis 1978":
        return "between 1949 and 1978"
    elif value == "von 1979 bis 1986":
        return "between 1979 and 1986"
    elif value == "von 1987 bis 1990":
        return "between 1987 and 1990"
    elif value == "von 1991 bis 1995":
        return "between 1991 and 1995"
    elif value == "von 1996 bis 2010":
        return "between 1996 and 2010"
    elif value == "2011 und später":
        return "2011 and later"

    elif value == "Schaffung öffentlicher\n Verkehrsflächen":
        return "creation_of_public_transport_areas"
    elif value == "Schaffung von\n Freiflächen":
        return "creation_of_open_spaces"
    elif value == "Errichtung eines\n neuen Wohngebäudes":
        return "creation_of_new_residential_building"
    elif value == "Errichtung eines\n neuen Nichtwohngebäudes":
        return "creation_of_new_non_residential_building"
    elif value == "Bauordnungsrechtliche\n Unzulässigkeit des Gebäudes":
        return "building_regulations_inadmissability"
    elif value == "Außergewöhnliches\n Ereignis (Brand etc.)":
        return "extraordinary_event"
    elif value == "Nutzungsänderung":
        return "change_of_use"
    elif value == "Nutzungsänderung\n ganzer Gebäude im \n Zuge von Baumaßnahmen":
        return "change_of_use_of_entire_buildings_in_the_course_of_construction_work"
    elif value == "Sonstige Gründe":
        return "other_reasons"

    else:
        return value


def build_type_parent_index_3(row):
    row_index = row.name

    if row_index == 0:
        return None
    elif row_index == 1:
        return 0
    elif row_index == 2:
        return 1
    elif row_index == 3:
        return 1
    elif row_index == 4:
        return 1
    elif row_index == 5:
        return 1
    elif row_index == 6:
        return 5
    elif row_index == 7:
        return 5
    elif row_index == 8:
        return 5
    elif row_index == 9:
        return 5
    elif row_index == 10:
        return 5
    elif row_index == 11:
        return 1
    elif row_index == 12:
        return 1
    elif row_index == 13:
        return 0
    elif row_index == 14:
        return 13
    elif row_index == 15:
        return 13
    elif row_index == 16:
        return 13
    elif row_index == 17:
        return 13
    elif row_index == 18:
        return 17
    elif row_index == 19:
        return 17
    elif row_index == 20:
        return 17
    elif row_index == 21:
        return 17
    elif row_index == 22:
        return 13
    elif row_index == 23:
        return 13
    elif row_index == 24:
        return 13
    elif row_index == 25:
        return 13
    elif row_index == 26:
        return 25
    elif row_index == 27:
        return 25
    elif row_index == 28:
        return 25
    elif row_index == 29:
        return 25
    elif row_index == 30:
        return 25
    elif row_index == 31:
        return 13
    elif row_index == 32:
        return 13
    else:
        return None


def build_type_parent_index_4(row):
    row_index = row.name

    if row_index == 0:
        return None
    elif row_index == 1:
        return 0
    elif row_index == 2:
        return 1
    elif row_index == 3:
        return 1
    elif row_index == 4:
        return 1
    elif row_index == 5:
        return 1
    elif row_index == 6:
        return 5
    elif row_index == 7:
        return 5
    elif row_index == 8:
        return 5
    elif row_index == 9:
        return 5
    elif row_index == 10:
        return 5
    elif row_index == 11:
        return 1
    elif row_index == 12:
        return 1
    elif row_index == 13:
        return 0
    elif row_index == 14:
        return 13
    elif row_index == 15:
        return 13
    elif row_index == 16:
        return 13
    elif row_index == 17:
        return 13
    elif row_index == 18:
        return 17
    elif row_index == 19:
        return 17
    elif row_index == 20:
        return 17
    elif row_index == 21:
        return 17
    elif row_index == 22:
        return 13
    elif row_index == 23:
        return 13
    elif row_index == 24:
        return 13
    elif row_index == 25:
        return 13
    elif row_index == 26:
        return 25
    elif row_index == 27:
        return 25
    elif row_index == 28:
        return 25
    elif row_index == 29:
        return 25
    elif row_index == 30:
        return 25
    elif row_index == 31:
        return 13
    elif row_index == 32:
        return 13
    else:
        return None


def build_type_parent_index_5(row):
    row_index = row.name

    if row_index == 0:
        return None
    if row_index == 1:
        return 0
    if row_index == 2:
        return 1
    if row_index == 3:
        return 1
    if row_index == 4:
        return 1
    if row_index == 5:
        return 1
    if row_index == 6:
        return 1
    if row_index == 7:
        return 1
    if row_index == 8:
        return 1
    if row_index == 9:
        return 8
    if row_index == 10:
        return 8
    if row_index == 11:
        return 8
    if row_index == 12:
        return 8
    if row_index == 13:
        return 8
    if row_index == 14:
        return 1
    if row_index == 15:
        return 1
    if row_index == 16:
        return 0
    if row_index == 17:
        return 16
    if row_index == 18:
        return 16
    if row_index == 19:
        return 16
    if row_index == 20:
        return 16
    if row_index == 21:
        return 20
    if row_index == 22:
        return 20
    if row_index == 23:
        return 20
    if row_index == 24:
        return 20
    if row_index == 25:
        return 16
    if row_index == 26:
        return 16
    if row_index == 27:
        return 16
    if row_index == 28:
        return 16
    if row_index == 29:
        return 28
    if row_index == 30:
        return 28
    if row_index == 31:
        return 28
    if row_index == 32:
        return 28
    if row_index == 33:
        return 28
    if row_index == 34:
        return 16
    if row_index == 35:
        return 16
    else:
        return None


def build_district_id(value):
    value = str(value).lstrip().rstrip().replace(" ", "")

    if value == "Mitte":
        return "01"
    elif value == "Friedrichshain-Kreuzberg":
        return "02"
    elif value == "Pankow":
        return "03"
    elif value == "Charlottenburg-Wilmersdorf":
        return "04"
    elif value == "Spandau":
        return "05"
    elif value == "Steglitz-Zehlendorf":
        return "06"
    elif value == "Tempelhof-Schöneberg":
        return "07"
    elif value == "Neukölln":
        return "08"
    elif value == "Treptow-Köpenick":
        return "09"
    elif value == "Marzahn-Hellersdorf":
        return "10"
    elif value == "Lichtenberg":
        return "11"
    elif value == "Reinickendorf":
        return "12"
    else:
        return None


#
# Helpers
#

def build_engine(source_file_extension):
    return "openpyxl" if source_file_extension == ".xlsx" else None


def write_csv_file(dataframe, file_path, quiet):
    if dataframe.shape[0] > 0:
        dataframe.to_csv(file_path, index=False)
        if not quiet:
            print(f"✓ Convert {os.path.basename(file_path)}")
    else:
        if not quiet:
            print(dataframe.head())
            print(f"✗️ Empty {os.path.basename(file_path)}")
