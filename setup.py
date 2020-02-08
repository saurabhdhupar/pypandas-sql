import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypandas_sql",
    version="0.0.2",
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
    setup_requires=["pytest-runner<4"],
    tests_require=["pytest"],
    install_requires=[
        'sqlalchemy~=1.3',
        'psycopg2-binary>=2.7.4',
        'numpy==1.18.1',
        'python-dateutil==2.8.1',
        'pytz==2019.3',
        'six==1.14.0',
        'zipp==2.2.0',
        'SecretStorage==3.1.2',
        'pandas==0.25.3',
        'Click==7.0',
        'importlib-metadata==1.5.0',
        'keyring==21.0.0',
        'appdirs==1.4.3',
        'configparser==4.0.2',
    ]
)
