import os

# The folder containing the files. '.' means the current folder.
folder_path = '.'

# The strings to find and replace
old_string = 'XLF'
new_string = 'STT'

# Loop through all the files in the folder
for filename in os.listdir(folder_path):
    # Check if the old string is in the filename
    if old_string in filename:
        # Create the new filename by replacing the old string
        new_filename = filename.replace(old_string, new_string)

        # Get the full old and new file paths
        old_filepath = os.path.join(folder_path, filename)
        new_filepath = os.path.join(folder_path, new_filename)

        # Rename the file
        os.rename(old_filepath, new_filepath)
        print(f'Renamed: {filename} -> {new_filename}')

print('\nDone!')