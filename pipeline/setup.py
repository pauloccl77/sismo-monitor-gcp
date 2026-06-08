"""
setup.py — empaqueta los módulos locales para Dataflow.
Dataflow corre en workers remotos que no tienen acceso a los archivos locales.
Este archivo le dice a Beam qué archivos subir junto con el pipeline.
"""
import setuptools

setuptools.setup(
    name="sismo-monitor-pipeline",
    version="1.0.0",
    packages=setuptools.find_packages(),
    py_modules=["transforms", "config"],
)
