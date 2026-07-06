"""Phase 0 smoke tests: proves the package imports and seeds validate."""

from generator.seed_loader import load_skus, load_stores


def test_seeds_load_and_validate():
    skus = load_skus()
    stores = load_stores()
    assert len(skus) > 0
    assert len(stores) > 0
    assert all(s.unit_price >= 0 and s.category for s in skus)


def test_store_ids_unique():
    stores = load_stores()
    ids = [s.store_id for s in stores]
    assert len(ids) == len(set(ids))
    