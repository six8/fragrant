from setuptools import setup, find_packages

def main():
    setup(
        name = 'fragrant',
        version = '0.7',
        packages=find_packages('src'),
        zip_safe=False,
        install_requires = [
            'fabric>=1.2',
        ],
        extras_require = {
            'httpcache': ['eventlet'],
        },
        tests_require = ['pytest'],
        package_dir = {'':'src'},
    )

if __name__ == '__main__':
    main()