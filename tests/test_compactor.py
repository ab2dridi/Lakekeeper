from unittest.mock import MagicMock

import pytest


@pytest.fixture
def spark_mock():
    spark = MagicMock()
    # minimal catalog.listTables
    t = MagicMock()
    t.name = "mytable"
    t.isTemporary = False
    spark.catalog.listTables.return_value = [t]
    return spark


def test_compute_target_files():
    from compactor.compactor import Compactor

    comp = Compactor(MagicMock(), block_size=128 * 1024 * 1024)
    # 3GB -> about 24 blocks
    total = 3 * 1024 * 1024 * 1024
    assert comp.compute_target_files(total) == pytest.approx(24, rel=0.2, abs=0)


def test_needs_compaction():
    from compactor.compactor import Compactor

    comp = Compactor(MagicMock(), block_size=128 * 1024 * 1024)
    # many tiny files
    assert comp.needs_compaction(1024 * 1024 * 1024, 2000)
    # few large files
    assert not comp.needs_compaction(1024 * 1024 * 1024, 2)
