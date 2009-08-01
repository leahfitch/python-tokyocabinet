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
    description = """Another python wrapper for Tokyo Cabinet""",
    author = "Elisha Cook",
    author_email = "ecook@justastudio.com",
    url = "http://bitbucket.org/elisha/tokyocabinet/"
)
