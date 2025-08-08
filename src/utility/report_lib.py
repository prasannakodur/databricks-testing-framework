import datetime
import os

# Get the project root directory dynamically
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dir_path = os.path.join(project_root, 'report')

print(dir_path)
print('hi')
os.makedirs(dir_path,exist_ok=True)
# Ensure the 'report' directory exists
# report_dir = "/Users/admin/PycharmProjects/taf/report"
# os.makedirs(report_dir, exist_ok=True)

# Create a single report filename for the session
timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")
# timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")
report_file_name = os.path.join(dir_path,f"report_{timestamp}.txt")
# report_filename = os.path.join(report_dir, f"report_{timestamp}.txt")
# print(report_filename)

def write_output(validation_type, status, details):
    # Write the output to the report file
    with open(report_file_name, "a") as report:
        report.write(f"{validation_type}: {status}\nDetails: {details}\n\n")