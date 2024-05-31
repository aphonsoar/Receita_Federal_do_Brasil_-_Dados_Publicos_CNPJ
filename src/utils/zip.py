from zipfile import ZipFile

def create_sample_zip(filename, content):
  """
  This function creates a sample ZIP file with the specified name and content.

  Args:
      filename (str): The name of the ZIP file to create.
      content (str): The content to add to the ZIP file (e.g., some text).
  """
  if not filename.endswith('.zip'):
    filename += '.zip'

  with ZipFile(filename, 'w') as zip_file:
    zip_file.writestr('data.txt', content)


def extract_zip_file(file_path, extracted_files_path):
    """
    Extracts a zip file to the specified directory.

    Args:
        file_path (str): The path to the zip file.
        extracted_files_path (str): The path to the directory where the files will be extracted.
    """
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_files_path)


def list_zip_contents(zip_file_path):
    """
    Lists the filenames and other information about files within a zip archive.

    Args:
        zip_file_path (str): Path to the zip archive.

    Returns:
        list: A list of ZipInfo objects containing information about each file in the zip.
    """
    with ZipFile(zip_file_path, 'r') as zip_ref:
        return zip_ref.infolist()