import os
import shutil

from lib.tracking_decorator import TrackingDecorator
import re

@TrackingDecorator.track_time
def copy_data(source_path, results_path, clean=False, quiet=False):
    # Iterate over files
    for subdir, dirs, files in sorted(os.walk(source_path)):

        # Make results path
        subdir = subdir.replace(f"{source_path}/", "")
        os.makedirs(os.path.join(results_path, subdir), exist_ok=True)

        for source_file_name in sorted(files):
            results_file_name = get_results_file_name(source_file_name)

            source_file_path = os.path.join(source_path, subdir, source_file_name)
            results_file_path = os.path.join(results_path, subdir, results_file_name)

            # Check if file needs to be copied
            if clean or not os.path.exists(results_file_path):
                shutil.copyfile(source_file_path, results_file_path)

                if not quiet:
                    print(f"✓ Copy {results_file_name}")
            else:
                print(f"✓ Already exists {results_file_name}")


def get_results_file_name(source_file_name):
    year = re.findall(r'\d{4}', source_file_name)[-1]
    month = re.findall(r'\d{2}', source_file_name)[-1]

    return f"berlin-building-permits-{year}-{month}.xlsx"
