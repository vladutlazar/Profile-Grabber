import os
import shutil
import pandas as pd
import sys
import threading
import logging

# Setup logging to output to both console and debug.txt file
log_file = 'debug.txt'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler(log_file),
    logging.StreamHandler(sys.stdout)
])

class TimeoutException(Exception):
    pass

class TimeoutFlag:
    def __init__(self):
        self.flag = False

    def set(self):
        self.flag = True

    def is_set(self):
        return self.flag

def copy_profiles_folder(source, destination, timeout=60):
    temp_file = 'last_accessed_drive.tmp'

    def update_temp_file():
        with open(temp_file, 'a') as f:  # Append mode
            f.write(f"{source}\n")

    def raise_timeout_exception():
        raise TimeoutException

    timeout_flag = TimeoutFlag()

    def check_timeout():
        if not timeout_flag.is_set():
            timeout_flag.set()

    timer = threading.Timer(timeout, check_timeout)
    timer.start()

    try:
        profiles_found = False
        config_files_copied = False
        for root, dirs, files in os.walk(source):
            if timeout_flag.is_set():
                logging.info(f"Skipping {source} due to timeout.")
                break

            if 'profiles' in root.split(os.sep) and 'test_profile' not in root.split(os.sep):
                if any(keyword in root.lower() for keyword in ['not_used', 'not used', 'old']):
                    logging.info(f"Ignoring directory due to path containing exclusion keyword: {root}")
                    continue

                # Filter out directories and files that should be ignored
                filtered_dirs = [d for d in dirs if all(keyword not in d.lower() for keyword in ['not_used', 'not used', 'old'])]
                filtered_files = [f for f in files if f.lower() != 'test_profile.xml' and all(keyword not in f.lower() for keyword in ['not_used', 'not used', 'old'])]

                # Skip if the directory is empty or only contains test_profile.xml
                if not filtered_files and not any(os.path.join(root, d) for d in filtered_dirs):
                    continue

                parent_directory = os.path.basename(os.path.dirname(root))
                profiles_destination = os.path.join(destination, f"{parent_directory}_profile")

                if not os.path.exists(profiles_destination):
                    os.makedirs(profiles_destination)

                # Copy the config files from the parent folder of the profiles folder
                parent_folder = os.path.dirname(root)
                config_files = [f for f in os.listdir(parent_folder) if f == 'FraMES.Client.Shell.exe.config']

                for config_file in config_files:
                    if timeout_flag.is_set():
                        logging.info(f"Skipping {source} due to timeout.")
                        break

                    source_config_file = os.path.join(parent_folder, config_file)
                    dest_config_file = os.path.join(profiles_destination, config_file)

                    shutil.copy(source_config_file, dest_config_file)
                    logging.info(f"Copied config file: {source_config_file} to {dest_config_file}")
                    config_files_copied = True

                for dir in filtered_dirs:
                    if timeout_flag.is_set():
                        logging.info(f"Skipping {source} due to timeout.")
                        break

                    source_dir = os.path.join(root, dir)
                    dest_dir = os.path.join(profiles_destination, dir)

                    shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
                    logging.info(f"Copied directory: {source_dir} to {dest_dir}")

                    profiles_found = True
                    update_temp_file()  # Update temp file
                    timer.cancel()  # Reset the timer
                    timer = threading.Timer(timeout, check_timeout)
                    timer.start()

                for file in filtered_files:
                    if timeout_flag.is_set():
                        logging.info(f"Skipping {source} due to timeout.")
                        break

                    source_file = os.path.join(root, file)
                    dest_file = os.path.join(profiles_destination, file)

                    shutil.copy(source_file, dest_file)
                    logging.info(f"Copied file: {source_file} to {dest_file}")

                    profiles_found = True
                    update_temp_file()  # Update temp file
                    timer.cancel()  # Reset the timer
                    timer = threading.Timer(timeout, check_timeout)
                    timer.start()

        if not profiles_found and not config_files_copied:
            logging.info(f"No profiles or config files found in {source} within the timeout period.")

    except TimeoutException:
        logging.info(f"Skipping {source} due to timeout.")
    finally:
        timer.cancel()

def main():
    try:
        excel_file_path = r'\\vt1.vitesco.com\SMT\didt1083\01_MES_PUBLIC\1.6.Production Errors\production_pc.xlsx'
        df = pd.read_excel(excel_file_path)

        destination_folder = r'\\vt1.vitesco.com\SMT\didt1083\04_MES_BACKUP\Profiles'

        for index, row in df.iterrows():
            drive_name = row['drive_name']
            drive_path = row['drive_path']

            # Ensure drive_path is a string and handle missing values
            try:
                drive_path = str(drive_path)
            except ValueError:
                logging.info(f"Skipping row with invalid drive_path: {row}")
                continue

            # Skip rows with empty drive_path
            if not drive_path or drive_path.lower() == 'nan':
                logging.info(f"Skipping row with empty drive_path: {row}")
                continue

            drive_destination_folder = os.path.join(destination_folder, drive_name)
            if not os.path.exists(drive_destination_folder):
                os.makedirs(drive_destination_folder)

            try:
                copy_profiles_folder(drive_path, drive_destination_folder)
            except TimeoutException:
                logging.info(f"Skipping {drive_path} due to timeout.")
                continue

        logging.info("Profiles and config files copied and organized successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
