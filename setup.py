from setuptools import setup, find_packages

setup(
    name="tx-training",
    version="0.6.0",
    description="A package to run G2I tasks in Databricks via JoinCustomersIntoOrders",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "tx_training": ["jobs/g2i/*.json"],
    },
    install_requires=["pyspark>=2.4.0", "delta-spark>=3.2.0"],
    entry_points={
        "console_scripts": [
            "tx-training=tx_training.jobs.g2i.join_cus_and_ord:run_execute",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
