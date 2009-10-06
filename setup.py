from setuptools import setup, Extension


setup(
    name = "tokyocabinet",
    version = "0.5",
    packages = ['tokyocabinet'],
    ext_modules = [
        Extension(
            "tokyocabinet.btree", ['tokyocabinet/btree.c'],
            libraries=["tokyocabinet"]
        ),
        Extension(
            "tokyocabinet.hash", ['tokyocabinet/hash.c'],
            libraries=["tokyocabinet"]
        ),
        Extension(
            "tokyocabinet.table", ['tokyocabinet/table.c'],
            libraries=["tokyocabinet"]
        )
    ],
    description = """tokyocabinet aims to be a complete python wrapper for the 
        Tokyo Cabinet database library by Mikio Hirabayashi (http://1978th.net/).
        
        So far the btree, hash and table APIs have been fully wrapped.""",
    author = "Elisha Cook",
    author_email = "ecook@justastudio.com",
    url = "http://bitbucket.org/elisha/tokyocabinet/"
)
