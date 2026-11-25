from setuptools import setup, find_packages

setup(
    name="rws-sptrans-databricks-mec",
    version="0.1.0",
    description="SPTrans Databricks ETL package",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    zip_safe=False,
)
