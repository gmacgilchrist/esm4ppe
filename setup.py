from setuptools import find_packages, setup

exec(open("esm4ppe/version.py").read())

setup(
    name='esm4ppe',
    url='https://github.com/gmacgilchrist/esm4ppe.git',
    version=__version__,
    packages=find_packages(),
    license='MIT',
)
