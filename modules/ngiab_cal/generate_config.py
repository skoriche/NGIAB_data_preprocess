from data_processing.file_paths import file_paths
from datetime import datetime, timedelta
from pathlib import Path
import json
import sqlite3
import logging
from hydrotools.nwis_client import IVDataService

logger = logging.getLogger(__name__)


def create_crosswalk_json(
    hydrofabric_path: Path, gage_id: str, output_file: Path
) -> None:
    if not hydrofabric_path.exists():
        raise FileNotFoundError(
            f"Hydrofabric path {hydrofabric_path} does not exist, have you run the subset command?"
        )

    with sqlite3.connect(hydrofabric_path) as con:
        sql_query = f"SELECT id FROM 'flowpath-attributes' WHERE gage = '{gage_id}'"
        result = con.execute(sql_query).fetchall()
        if len(result) == 0:
            logger.critical(f"Gage ID {gage_id} is not associated with any waterbodies")
            raise IndexError(f"Could not find a waterbody for gage {gage_id}")
        if len(result) > 1:
            logger.critical(
                f"Gage ID {gage_id} is associated with multiple waterbodies"
            )
            raise IndexError(f"Could not find a unique waterbody for gage {gage_id}")

        wb_id = result[0][0]
        cat_id = wb_id.replace("wb", "cat")

    data = {cat_id: {"Gage_no": gage_id}}
    with open(output_file, "w") as f:
        f.write(json.dumps(data))


def convert_paths_to_absolute(source_file: Path, dest_file: Path) -> None:
    # a bit dodgy but removeable once ngiab-cal is updated
    with open(source_file, "r") as f:
        with open(dest_file, "w") as out:
            for line in f:
                line = line.replace("./", "/ngen/ngen/data/")
                line = line.replace("/ngen/ngen/data/outputs/ngen/", ".")
                line = line.replace("outputs/troute/", ".")
                # ngiab-cal takes troute yaml as an input but doesn't replace this value
                line = line.replace("/ngen/ngen/data/config/troute.yaml", "/ngen/ngen/data/calibration/troute.yaml")
                if "lakeout_output" in line:
                    continue
                if "lite_restart" in line:
                    continue
                out.write(line)


def setup_ngen_troute_config(paths: file_paths) -> None:
    realization_path = paths.config_dir / "realization.json"
    troute_path = paths.config_dir / "troute.yaml"
    # copy over the realization.json file and troute.yaml file
    if not realization_path.exists():
        raise FileNotFoundError(
            f"Realization file {realization_path} does not exist, have you run realization generation?"
        )
    if not troute_path.exists():
        raise FileNotFoundError(
            f"Troute config file {troute_path} does not exist, have you run the subset command?"
        )
    convert_paths_to_absolute(troute_path, paths.calibration_dir / "troute.yaml")
    convert_paths_to_absolute(
        realization_path, paths.calibration_dir / "realization.json"
    )


def get_start_end_times(realization_path: Path) -> tuple[str, str]:
    with open(realization_path, "r") as f:
        realization = json.loads(f.read())
    start = realization["time"]["start_time"]
    end = realization["time"]["end_time"]
    return start, end


def write_usgs_data_to_csv(start:str, end:str, gageid:str, output_file: Path) -> None:
    service = IVDataService(cache_filename="~/.ngiab/hydrotools_cache.sqlite")
    data = service.get(sites=gageid, startDT=start, endDT=end)
    unit = data["measurement_unit"][0]
    data = data[["value_time", "value"]]
    data.columns = ["value_date", "obs_flow"]
    # usgs data is in ft3/s, ngen-cal converts to m3/s without checking so LEAVE IT AS ft3/s
    data.to_csv(output_file, index=False)


def create_ngen_cal_config(
    output_dir: Path, gage_id: str, start_str: str, end_str: str
) -> None:
    time_format = "%Y-%m-%d %H:%M:%S"
    start = datetime.strptime(start_str, time_format)
    end = datetime.strptime(end_str, time_format)
    total_range = end - start
    # 2 year minimum suggested, 12 month warm up, then 50/50 split between calibration and validation
    # if more than 2 years, 12 month warm up and the rest is split 50/50
    if total_range.days < 730:
        logger.warning(
            "Calibration period is less than 2 years, this may not be enough data for calibration"
        )
    # warm up is half the range, capped at 365 days
    warm_up = timedelta(days=(total_range.days / 2))
    if warm_up.days < 365:
        warm_up = timedelta(days=365)
    # evaluation starts at the end of the warm up period
    evaluation_start = start + warm_up
    # ends after half the remaining time
    evaluation_end = end - ((total_range - warm_up) / 2)
    # validation starts at the end of the evaluation period
    validation_start = evaluation_end
    validation_end = end

    with open(file_paths.template_calibration_config, "r") as f:
        template = f.read()

    paths = file_paths(output_dir=output_dir)


    with open(paths.calibration_dir / "ngen_cal_conf.yaml", "w") as file:
        file.write(
            template.format(
                subset_hydrofabric = paths.geopackage_path.name,
                evaluation_start=evaluation_start.strftime(time_format),
                evaluation_stop=evaluation_end.strftime(time_format),
                valid_start_time=start.strftime(time_format),
                valid_end_time=end.strftime(time_format),
                valid_eval_start_time=validation_start.strftime(time_format),
                valid_eval_end_time=validation_end.strftime(time_format),
                full_eval_start_time=start.strftime(time_format),
                full_eval_end_time=end.strftime(time_format),
                gage_id=gage_id,
            )
        )


def create_calibration_config(calibration_dir: Path, gage_id: str) -> None:
    # first pass at this so I'm probably not using ngen-cal properly
    # for now keep it simple and only allow single gage lumped calibration
    gage_id = gage_id.split("-")[-1]
    # get the output directory
    paths = file_paths(output_dir=calibration_dir)
    if not paths.calibration_dir.exists():
        paths.calibration_dir.mkdir(parents=True)
    # create crosswalk.json
    create_crosswalk_json(
        paths.geopackage_path, gage_id, paths.calibration_dir / "crosswalk.json"
    )
    # copy over the realization.json file and troute.yaml file
    # update the paths to be non relative
    setup_ngen_troute_config(paths)
    # get start and end times from realization.json
    start, end = get_start_end_times(paths.config_dir / "realization.json")
    # get the observed flow for the gage in for the time period
    write_usgs_data_to_csv(
        start, end, gage_id, paths.calibration_dir / "obs_hourly_discharge.csv"
    )
    # create the dates for the ngen-cal config
    create_ngen_cal_config(paths.output_dir, gage_id, start, end)
