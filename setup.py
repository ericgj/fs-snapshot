from setuptools import setup, find_packages

tests_require = ["pytest", "hypothesis"]

setup(
    name="fs-snapshot",
    version="0.2",
    description="A file system snapshot + diff tool and library",
    license="MIT",
    author="Eric Gjertsen",
    email="ericgj72@gmail.com",
    packages=find_packages(),
    entry_points={"console_scripts": ["fs-snapshot = fs_snapshot.__main__:main"]},
    tests_require=tests_require,
    extras_require={"test": tests_require},  # to make pip happy
    zip_safe=False,  # to make mypy happy
)
