import re
from re import escape
import argparse
import subprocess
import os


def run_pip_compile(input_file: str, output_file: str) -> int:
    """
    Runs pip-compile command to generate a requirements file.

    Args:
        input_file (str): Path to the input requirements file.
        output_file (str): Path to the output requirements file.

    Returns:
        int: The number of packages in the output file.
    """
    # Command to run pip-compile
    command = [
        "pip-compile",
        input_file,
        f"--output-file={output_file}",
        "--quiet",
        "--strip-extras",
    ]

    # Run the command
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running pip-compile: {e}")
        raise

    # Get package count after operation
    with open(output_file, "r") as file:
        lines = file.readlines()
        package_count = len(lines)

    return package_count


def find_packages_with_comment(
        requirements_file: str,
        pip_compile_output_file: str,
        requirements_output_file: str) -> int:
    """
    Finds packages with a specific comment in a requirements file and writes them to a new file.

    Args:
        requirements_file (str): Path to the original requirements file.
        pip_compile_output_file (str): Path to the pip-compile output file.
        requirements_output_file (str): Path to the output file for the packages with comments.

    Returns:
        int: The number of packages with comments in the output file.
    """
    splitted_file = requirements_file.split('.')
    name = splitted_file[0]
    extension = splitted_file[1]
    pattern = (
        rf"^([^\s#][\w\-]+)==([\d\.]+)\n\s+# via -r {escape(name)}.{escape(extension)}$"
    )

    with open(pip_compile_output_file, "r") as file:
        with open(requirements_output_file, "w") as out_file:
            text = file.read()

            matches = re.finditer(pattern, text, re.MULTILINE)

            for match in matches:
                # Catched patterns
                package_name = match.group(1)
                version = match.group(2)

                # Write to file
                out_file.write(package_name + '==' + version + "\n")

    # Get package count after operation
    with open(requirements_output_file, "r") as file:
        lines = file.readlines()
        package_count = len(lines)

    return package_count


if __name__ == "__main__":
    description = "Find packages with a specific comment in a requirements file."

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "requirements_file", type=str, help="Path to the requirements file."
    )
    parser.add_argument(
        "output_file",
        type=str,
        help="Path to the output file.")

    args = parser.parse_args()

    # Get package count before operation
    with open(args.requirements_file, "r") as file:
        lines = file.readlines()
        package_count_before = len(lines)

    # Temporary file for pip-compile output
    tmp_file = 'tmp.in'
    run_pip_compile(args.requirements_file, tmp_file)

    # Get package count after operation
    package_count_after = find_packages_with_comment(
        args.requirements_file, tmp_file, args.output_file
    )

    os.remove(tmp_file)

    print(f"We removed {package_count_after-package_count_before} packages.")