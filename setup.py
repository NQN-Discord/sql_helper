from setuptools import setup


setup(
   name="sql_helper",
   version="0.1.0",
   description="A helper for PostgreSQL ",
   author='Blue',
   url="https://nqn.blue/",
   packages=["sql_helper"],
   install_requires=["aiopg", "discord.py"]
)
