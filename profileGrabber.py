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

def copy_xml_files(source, destination, timeout=60):
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
        xml_found = False
        for root, dirs, files in os.walk(source):
            if timeout_flag.is_set():
                logging.info(f"Skipping {source} due to timeout.")
                break

            if 'profiles' in root.split(os.sep) and 'test_profile' not in root.split(os.sep):
                parent_directory = os.path.basename(os.path.dirname(root))
                xml_files = [f for f in files if f.endswith('.xml')]

                for xml_file in xml_files:
                    if timeout_flag.is_set():
                        logging.info(f"Skipping {source} due to timeout.")
                        break

                    if xml_file.lower() == "test_profile.xml":
                        logging.info(f"Ignoring test profile XML file: {os.path.join(root, xml_file)}")
                        continue

                    source_path = os.path.join(root, xml_file)

                    if "not_used" in source_path.lower() or "not used" in source_path.lower() or "old" in source_path.lower():
                        logging.info(f"Ignoring XML file: {source_path}")
                        continue

                    destination_file = os.path.join(destination, f"{parent_directory}_{xml_file}_profile.xml")
                    shutil.copy(source_path, destination_file)
                    logging.info(f"Copied XML file: {source_path} to {destination_file}")

                    xml_found = True
                    update_temp_file()  # Update temp file
                    timer.cancel()  # Reset the timer
                    timer = threading.Timer(timeout, check_timeout)
                    timer.start()

        if not xml_found:
            logging.info(f"No XML files found in {source} within the timeout period.")

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
                copy_xml_files(drive_path, drive_destination_folder)
            except TimeoutException:
                logging.info(f"Skipping {drive_path} due to timeout.")
                continue

        logging.info("XML files copied and organized successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
