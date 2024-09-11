from pathlib import Path
from setuptools import find_namespace_packages, setup

# get the dependencies and installs
requirements = [
    "numpy",
    "pandas",
    "pyspark",
    "openpyxl",
    "xlrd",
    "xmltodict",
    "holidays"
]

# get version number
with open(Path(__file__).parent / "elexon" / "__init__.py") as f:
    version_line = f.readlines()[-1]
    version_number = version_line.split()[-1].strip("\"'")

setup(
    name="elexon_gb_demand_build",
    version=version_number,
    author="Pablo Paredes",
    description="Package to Compile Elexon Dataflows and create a gross demand table with half-hourly time-granularity, by GSP (Grid Supply Point), and by sector (Domestic, Non-domestic)",
    packages=find_namespace_packages(
        where=str(Path(__file__).parent), include=["elexon", "elexon.*"]
    ),
    install_requires=requirements,
    include_package_data=True,
    python_requires=">=3.7",
)
