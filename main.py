from pyspark.sql import SparkSession

from src.utils import read_yaml_config
from src.VehicleCrashAnalysis import VehicleCrashAnalysis

if __name__ == "__main__":
    # Initialize spark session
    spark = SparkSession.builder.appName("VehicleCrashAnalysis").getOrCreate()

    config_file_name = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    config = read_yaml_config(config_file_name)
    output_file_paths = config.get("OUTPUT_PATH")
    file_format = config.get("FILE_FORMAT")

    vehicle_crash = VehicleCrashAnalysis(spark, config)

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2
    print(
        "1. Number of males killed greater than 2: \t",
        vehicle_crash.count_male_accidents(output_file_paths.get(1), file_format.get("Output")),
    )

    # 2. How many two-wheelers are booked for crashes?
    print(
        "2. Number of two wheelers booked for crashes: \t",
        vehicle_crash.count_2_wheeler_accidents(
            output_file_paths.get(2), file_format.get("Output")
        ),
    )

    # 3. Top 5 Vehicle Makes in fatal crashes without airbag deployment
    print(
        "3. Top 5 Vehicle Makes in fatal crashes without airbag deployment: \t",
        vehicle_crash.top_5_vehicle_makes_for_fatal_crashes_without_airbags(
            output_file_paths.get(3), file_format.get("Output")
        ),
    )

    # 4. Number of Vehicles with a driver having valid licenses involved in hit-and-run
    print(
        "4. Number of Vehicles with a valid license involved in hit-and-run: \t",
        vehicle_crash.count_hit_and_run_with_valid_licenses(
            output_file_paths.get(4), file_format.get("Output")
        ),
    )

    # 5. State with the highest number of accidents without female involvement
    print(
        "5. State with no female accident involvement: \t",
        vehicle_crash.get_state_with_no_female_accident(
            output_file_paths.get(5), file_format.get("Output")
        ),
    )

    # 6. Top 3rd to 5th Vehicle Makes contributing to the most injuries
    print(
        "6. Top 3rd to 5th Vehicle Makes contributing to the most injuries: \t",
        vehicle_crash.get_top_vehicle_contributing_to_injuries(
            output_file_paths.get(6), file_format.get("Output")
        ),
    )

    # 7. Top ethnic user group for each unique body style involved in crashes
    print("7. Top ethnic user group for each body style involved in crashes: ")
    vehicle_crash.get_top_ethnic_ug_crash_for_each_body_style(
        output_file_paths.get(7), file_format.get("Output")
    ).show(truncate=False)

    # 8. Top 5 Zip Codes for alcohol-related crashes
    print(
        "8. Top 5 Zip Codes for alcohol-related crashes: \t",
        vehicle_crash.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
            output_file_paths.get(8), file_format.get("Output")
        ),
    )

    # 9. Distinct Crash IDs with no property damage, high damage level, and insurance
    print(
        "9. Distinct Crash IDs with no property damage and high damage level with insurance: \t",
        vehicle_crash.get_crash_ids_with_no_damage(
            output_file_paths.get(9), file_format.get("Output")
        ),
    )

    # 10. Top 5 Vehicle Makes for speeding related offences
    print(
        "10. Top 5 Vehicle Makes for speeding related offences: \n",
        vehicle_crash.get_top_5_vehicle_brand(
            output_file_paths.get(10), file_format.get("Output")
        ),
    )

    spark.stop()
