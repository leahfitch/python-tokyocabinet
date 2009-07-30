from setuptools import setup, Extension


setup(
    name = "tokyocabinet",
    version = "0.1",
    packages = ['tokyocabinet'],
    ext_modules = [
        Extension(
            "tokyocabinet.btree", ['tokyocabinet/btree.c'],
            libraries=["tokyocabinet"]
        ),
        Extension(
            "tokyocabinet.hash", ['tokyocabinet/hash.c'],
            libraries=["tokyocabinet"]
        )
    ],
    description = """Python wrapper for the inestimably rad Tokyo Cabinet"""
)
