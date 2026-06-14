import os


def test_spark_readme_and_notebook_exist():
    assert os.path.exists("spark/README.md")
    assert os.path.exists("spark/notebooks/spark_exploration.ipynb")
