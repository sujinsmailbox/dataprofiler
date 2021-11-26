import setuptools
import glob
import os.path

with open(os.path.dirname(__file__) + "README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.dirname(__file__) + "version", "r") as fh:
    version = fh.read()

requirements = []
with open(os.path.dirname(__file__) + "requirements.txt", "r") as fh:
    for line in fh:
        requirements.append(line)

py_files = glob.glob("*.py")

setuptools.setup(
    name="DataProfiler",
    version=version,
    description="data profiler for csv files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),    
    py_modules=[p.replace(".py", "") for p in py_files],
    install_requires=requirements,
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: Copyright YARA"
        "Operating System :: OS Independent",
    ],
)
