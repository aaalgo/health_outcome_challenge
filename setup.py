#!/usr/bin/env python3
from distutils.core import setup, Extension

cms_core = Extension('cms_core',
        language = 'c++',
        extra_compile_args = ['-O3', '-std=c++17', '-I.', '-g', '-Wno-sign-compare', '-Wno-parentheses', '-fopenmp', '-DDEBUG'], 
        #extra_compile_args = ['-O0', '-std=c++17', '-I.', '-g', '-Wno-sign-compare', '-Wno-parentheses', '-fopenmp', '-DDEBUG'], 
        include_dirs = ['/usr/include/eigen3', '/usr/local/include', '3rd/pybind11/include', '3rd/fmt/include', '3rd/readerwriterqueue', '3rd/bitsery/include'],
        libraries = ['boost_timer', 'boost_iostreams', 'glog', 'gomp'],
        library_dirs = ['/usr/local/lib', 'src'],
        sources = ['cms.cpp']
        )

setup (name = 'cms_core',
       version = '0.0.1',
       author = 'Yuanfang Guan',
       author_email = 'yuanfang.guan.1.0@gmail.com',
       license = '',
       description = '',
       ext_modules = [cms_core],
       )

