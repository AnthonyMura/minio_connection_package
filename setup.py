from setuptools import setup, find_packages

setup(
    name="minio_connection",
    version="1.0.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "minio_utils": ["minio_configs.json"],
    },
    install_requires=[
        "boto3",
    ],
    author="minkota",
    description="A reusable package for MinIO connection utilities",
    license="MIT",
)