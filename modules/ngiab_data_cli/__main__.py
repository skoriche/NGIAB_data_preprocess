from typing import Tuple
import rich.status
import sys
# add a status bar for these imports so the cli feels more responsive
with rich.status.Status("Initializing...") as status:
    from data_sources.source_validation import validate_all
    from ngiab_data_cli.custom_logging import setup_logging, set_logging_to_critical_only
    from ngiab_data_cli.arguments import parse_arguments
    from data_processing.file_paths import file_paths
    import argparse
    import logging
    import time
    from typing import List
    import subprocess
    import time
    from dask.distributed import Client
    from data_processing.gpkg_utils import get_catid_from_point, get_cat_from_gage_id
    from data_processing.graph_utils import get_upstream_cats
    from data_processing.subset import subset, subset_vpu
    from data_processing.forcings import create_forcings
    from data_processing.create_realization import create_realization, create_em_realization
    from ngiab_cal import create_calibration_config, generate_best_realization


def validate_input(args: argparse.Namespace) -> Tuple[str,str]:
    """Validate input arguments."""

    if args.vpu:
        if not args.output_name:
            args.output_name = f"vpu-{args.vpu}"
        return args.vpu, args.output_name

    input_feature = args.input_feature.replace("_", "-")

    # look at the prefix for autodetection, if -g or -l is used then there is no prefix
    if len(input_feature.split("-")) > 1:
        prefix = input_feature.split("-")[0]
        if prefix.lower() == "gage":
            args.gage = True
        elif prefix.lower() == "wb":
            logging.warning("Waterbody IDs are no longer supported!")
            logging.warning(f"Automatically converting {input_feature} to catid")
            time.sleep(2)

    # always add or replace the prefix with cat if it is not a lat lon or gage
    if not args.latlon and not args.gage:
        input_feature = "cat-" + input_feature.split("-")[-1]

    if args.latlon and args.gage:
        raise ValueError("Cannot use both --latlon and --gage options at the same time.")

    if args.latlon:
        feature_name = get_cat_id_from_lat_lon(input_feature)
        logging.info(f"Found {feature_name} from {input_feature}")
    elif args.gage:
        feature_name = get_cat_from_gage_id(input_feature)
        logging.info(f"Found {feature_name} from {input_feature}")
    else:
        feature_name = input_feature

    if args.output_name:
        output_folder = args.output_name
    elif args.gage:
        output_folder = input_feature
    else:
        output_folder = feature_name

    if args.cal and not args.gage:
        raise ValueError("Calibration is only available for gage IDs.")

    return feature_name, output_folder


def get_cat_id_from_lat_lon(input_feature: str) -> List[str]:
    """Read catchment IDs from input file or return single ID."""
    if "," in input_feature:
        coords = input_feature.split(",")
        return get_catid_from_point({"lat": float(coords[0]), "lng": float(coords[1])})
    else:
        raise ValueError("Lat Lon input must be comma separated e.g. -l 54.33,-69.4")


def set_dependent_flags(args, paths: file_paths):

    # if validate is set, run everything that is missing
    if args.validate:
        logging.info("Running all missing steps required to run ngiab.")
        args = validate_run_directory(args, paths)

    # realization and forcings require subset to have been run at least once
    if args.realization or args.forcings:
        if not paths.subset_dir.exists() and not args.subset:
            logging.warning(
                "Subset required for forcings and realization generation, enabling subset."
            )
            args.subset = True

    if (args.forcings or args.realization) and not (args.start_date and args.end_date):
        raise ValueError(
            "Both --start and --end are required for forcings generation or realization creation. YYYY-MM-DD"
        )

    return args


def validate_run_directory(args, paths: file_paths):
    # checks the folder that is going to be run, enables steps that are needed to populate the folder
    if not paths.subset_dir.exists():
        logging.info("Subset folder does not exist, enabling subset, forcings, and realization.")
        args.subset = True
        args.forcings = True
        args.realization = True
        return args
    if not paths.forcings_dir.exists():
        logging.info("Forcings folder does not exist, enabling forcings.")
        args.forcings = True
    # this folder only exists if realization generation has run
    cat_config_dir = paths.config_dir / "cat_config"
    if not cat_config_dir.exists():
        logging.info("Realization folder does not exist, enabling realization.")
        args.realization = True
    return args


def main() -> None:
    setup_logging()
    validate_all()
    try:
        args = parse_arguments()
        if args.debug:
            logging.getLogger("data_processing").setLevel(logging.DEBUG)
        feature_to_subset, output_folder = validate_input(args)
        paths = file_paths(output_folder)
        paths.append_cli_command(sys.argv)
        args = set_dependent_flags(args, paths)  # --validate
        if feature_to_subset:
            logging.info(f"Processing {feature_to_subset} in {paths.output_dir}")
            if not args.vpu:
                upstream_count = len(get_upstream_cats(feature_to_subset))
                logging.info(f"Upstream catchments: {upstream_count}")
                if upstream_count == 0:
                    # if there are no upstreams, exit
                    logging.error("No upstream catchments found.")
                    return

        if args.subset:
            if args.vpu:
                logging.info(f"Subsetting VPU {args.vpu}")
                subset_vpu(args.vpu, output_gpkg_path=paths.geopackage_path)
                logging.info("Subsetting complete.")
            else:
                logging.info(f"Subsetting hydrofabric")
                subset(feature_to_subset, output_gpkg_path=paths.geopackage_path)
                logging.info("Subsetting complete.")

        if args.forcings:
            logging.info(f"Generating forcings from {args.start_date} to {args.end_date}...")
            if args.empirical_model:
                create_forcings(
                    start_time=args.start_date,
                    end_time=args.end_date,
                    output_folder_name=output_folder,
                    forcing_vars = ["t2d", "precip"]
                )
            else:
                create_forcings(
                    start_time=args.start_date,
                    end_time=args.end_date,
                    output_folder_name=output_folder,
                )
            logging.info("Forcings generation complete.")

        if args.realization:
            logging.info(f"Creating realization from {args.start_date} to {args.end_date}...")
            if args.empirical_model:
                create_em_realization(
                    output_folder, start_time=args.start_date, end_time=args.end_date
                )
            else:
                create_realization(
                    output_folder, start_time=args.start_date, end_time=args.end_date, use_nwm_gw=args.nwm_gw
                )
            logging.info("Realization creation complete.")

        # check if the dask client is still running and close it
        try:
            client = Client.current()
            client.shutdown()
        except ValueError:
            # value error is raised if no client is running
            pass

        if args.run and not args.cal:
            logging.info("Running Next Gen using NGIAB...")
            # open the partitions.json file and get the number of partitions
            with open(paths.metadata_dir / "num_partitions", "r") as f:
                num_partitions = int(f.read())

            try:
                subprocess.run("docker pull joshcu/ngen", shell=True)
            except:
                logging.error("Docker is not running, please start Docker and try again.")
            try:
                command = f'docker run --rm -it -v "{str(paths.subset_dir)}:/ngen/ngen/data" joshcu/ngen /ngen/ngen/data/ auto {num_partitions}'
                # command = f'docker run --rm -it -v "{str(paths.subset_dir)}:/ngen/ngen/data" awiciroh/ciroh-ngen-image:latest-x86 /ngen/ngen/data/ auto {num_partitions}'
                subprocess.run(command, shell=True)
                logging.info("Next Gen run complete.")
            except:
                logging.error("Next Gen run failed.")

        if args.eval:
            plot = False
            try:
                import seaborn, matplotlib

                plot = True
            except ImportError:
                # silently fail as plotting isn't publicly supported
                pass

            try:
                from ngiab_eval import evaluate_folder

                if plot:
                    logging.info("Plotting enabled")
                logging.info("Evaluating model performance...")
                evaluate_folder(paths.subset_dir, plot=plot, debug=args.debug)
            except ImportError:
                logging.error(
                    "Evaluation module not found. Please install the ngiab_eval package to evaluate model performance."
                )

        if args.vis:
            try:
                command = f'docker run --rm -it -p 3000:3000 -v "{str(paths.subset_dir)}:/ngen/ngen/data/" joshcu/ngiab_grafana:v0.2.0'
                subprocess.run(command, shell=True)
            except:
                logging.error("Failed to launch docker container.")

        if args.cal:
            logging.info("Creating calibration config...")
            create_calibration_config(paths.output_dir,  args.input_feature.replace("_", "-"))
            logging.info("Calibration config created.")
            logging.info(f"This is still experimental, run the following command to start calibration:")
            logging.info(
                f'docker run -it -v "{paths.output_dir}:/ngen/ngen/data" joshcu/ngiab-cal'
            )

            if args.run:
                try:
                    subprocess.run("docker pull joshcu/ngiab-cal", shell=True)
                except:
                    logging.error("Docker is not running, please start Docker and try again.")
                logging.warning("Beginning calibration...")
                try:
                    command = f'docker run --rm -it -v "{str(paths.subset_dir)}:/ngen/ngen/data" joshcu/ngiab-cal /calibration/run.sh'
                    subprocess.run(command, shell=True)
                    logging.info("Calibration complete.")
                except:
                    logging.error("Calibration failed.")

            calibration_worker_dir = paths.calibration_dir / "Output"
            # if this folder exists then calibration has been run
            # but not necessarily completed
            if calibration_worker_dir.exists():
                logging.info("Calibration has been run.")
                logging.info(
                    f"Generating best realization to {paths.output_dir / "config" / "best_realization.json" }..."
                )
                generate_best_realization(paths.calibration_dir)

        logging.info("All operations completed successfully.")
        logging.info(f"Output folder: file:///{paths.output_dir}")
        # set logging to ERROR level only as dask distributed can clutter the terminal with INFO messages
        # that look like errors
        set_logging_to_critical_only()

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()
