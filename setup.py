from setuptools import setup, find_packages

setup(
    name="tx-training",
    version="1.1.0",
    description="A package to run G2I tasks in Databricks via JoinCustomersIntoOrders",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "tx_training": ["jobs/g2i/*.json"],
    },
    install_requires=["pyspark>=2.4.0", "delta-spark>=3.2.0"],
    entry_points={
        "console_scripts": [
            "list_customers_from_3_cities=tx_training.jobs.g2i.list_customers_from_3_cities:main",
            "list_order_in_range=tx_training.jobs.g2i.list_order_in_range:main",
            "list_products_above_average=tx_training.jobs.g2i.list_products_above_average:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
