from setuptools import setup, find_packages


setup(
   name="sql_helper",
   version="0.1.0",
   description="A helper for PostgreSQL ",
   author='Blue',
   url="https://nqn.blue/",
   packages=find_packages(),
   install_requires=["aiopg", "discord.py"]
)
