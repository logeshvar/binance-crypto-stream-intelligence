from __future__ import annotations

import pytest

from streaming.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark_session():
    spark = create_spark_session("pytest-silver-transformations")
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
