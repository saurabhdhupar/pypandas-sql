import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypandas_sql",
    version="0.0.1",
    scripts=['pypandasql'],
    packages=setuptools.find_packages(),
    author="Saurabh Dhupar",
    author_email="saurabh.dhupar@gmail.com",
    description="Utility to read/write data from various data sources",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/saurabhdhupar/pypandas-sql",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3',
    install_requires=['psycopg2-binary', 'pandas', 'click', 'configparser'],
    include_package_data=True
)
