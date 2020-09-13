from setuptools import setup


setup(
    name="fs-snapshot",
    version="0.1",
    description="A file system snapshot + diff tool and library",
    license="MIT",
    author="Eric Gjertsen",
    email="ericgj72@gmail.com",
    packages=["fs_snapshot"],
    entry_points={"console_scripts": ["fs-snapshot = fs_snapshot.__main__:main"]},
    tests_require=["pytest", "hypothesis"],
    zip_safe=False,  # to make mypy happy
)
