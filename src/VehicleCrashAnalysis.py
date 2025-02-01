from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from src.utils import load_data_from_csv, save_dataframe


class VehicleCrashAnalysis:
    def __init__(self, spark, config):
        file_paths = config.get("INPUT_FILENAME")
        self.df_charges = load_data_from_csv(spark, file_paths.get("Charges"))
        self.df_damages = load_data_from_csv(spark, file_paths.get("Damages"))
        self.df_endorse = load_data_from_csv(spark, file_paths.get("Endorse"))
        self.df_primary_person = load_data_from_csv(spark, file_paths.get("Primary_Person"))
        self.df_units = load_data_from_csv(spark, file_paths.get("Units"))
        self.df_restrict = load_data_from_csv(spark, file_paths.get("Restrict"))

    def count_male_accidents(self, output_path, output_format):
        df_filtered = self.df_primary_person.filter(
            (self.df_primary_person.PRSN_GNDR_ID == "MALE") &
            (self.df_primary_person.DEATH_CNT > 2)
        )
        save_dataframe(df_filtered, output_path, output_format)
        return df_filtered.count()

    def count_2_wheeler_accidents(self, output_path, output_format):
        df_filtered = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        save_dataframe(df_filtered, output_path, output_format)
        return df_filtered.count()

    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(self, output_path, output_format):
        df_filtered = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .filter(
                (col("PRSN_INJRY_SEV_ID") == "KILLED") &
                (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &
                (col("VEH_MAKE_ID") != "NA")
            )
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        save_dataframe(df_filtered, output_path, output_format)
        return [row[0] for row in df_filtered.collect()]

    def count_hit_and_run_with_valid_licenses(self, output_path, output_format):
        df_filtered = (
            self.df_units.select("CRASH_ID", "VEH_HNR_FL")
            .join(self.df_primary_person.select("CRASH_ID", "DRVR_LIC_TYPE_ID"), on=["CRASH_ID"], how="inner")
            .filter(
                (col("VEH_HNR_FL") == "Y") &
                col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])
            )
        )
        save_dataframe(df_filtered, output_path, output_format)
        return df_filtered.count()

    def get_state_with_no_female_accident(self, output_path, output_format):
        df_filtered = (
            self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID != "FEMALE")
            .groupby("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        save_dataframe(df_filtered, output_path, output_format)
        return df_filtered.first().DRVR_LIC_STATE_ID

    def get_top_vehicle_contributing_to_injuries(self, output_path, output_format):
        df_filtered = (
            self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA")
            .withColumn("TOT_CASUALTIES_CNT", self.df_units[35] + self.df_units[36])
            .groupby("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
            .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        )
        df_top_3_to_5 = df_filtered.limit(5).subtract(df_filtered.limit(2))
        save_dataframe(df_top_3_to_5, output_path, output_format)
        return [veh[0] for veh in df_top_3_to_5.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_ug_crash_for_each_body_style(self, output_path, output_format):
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df_filtered = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])
                & ~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])
            )
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )
        save_dataframe(df_filtered, output_path, output_format)
        return df_filtered

    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(self, output_path, output_format):
        df_filtered = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") |
                col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        save_dataframe(df_filtered, output_path, output_format)
        return [row[0] for row in df_filtered.collect()]

    def get_crash_ids_with_no_damage(self, output_path, output_format):
        df_filtered = (
            self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &
                    ~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])
                ) |
                (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                    ~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])
                )
            )
            .filter(self.df_damages.DAMAGED_PROPERTY == "NONE")
            .filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        )
        save_dataframe(df_filtered, output_path, output_format)
        return [row[0] for row in df_filtered.collect()]

    def get_top_5_vehicle_brand(self, output_path, output_format):
        top_25_state_list = [row[0] for row in self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull())
                             .groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_used_vehicle_colors = [row[0] for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
                                      .groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        df_filtered = (
            self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        save_dataframe(df_filtered, output_path, output_format)
        return [row[0] for row in df_filtered.collect()]
