import argparse
from ngiab_cal import create_calibration_config

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a calibration config for ngen-cal"
    )
    parser.add_argument(
        "-d",
        "--calibration_dir",
        type=Path,
        help="Directory to create the calibration config in",
    )
    parser.add_argument("-g", "--gage", type=str, help="Gage ID to use for calibration")
    args = parser.parse_args()
    create_calibration_config(args.calibration_dir, args.gage)
