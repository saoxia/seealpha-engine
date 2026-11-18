"""Offline depth research export never alters the production source catalog."""

import pytest

from engine.pipeline.options_data import export_depth_replay


def test_depth_export_requires_an_existing_source_and_new_target(tmp_path):
    destination = tmp_path / "depth-research"
    with pytest.raises(ValueError, match="盘口研究"):
        export_depth_replay([], destination)
    assert not destination.exists()
    destination.mkdir()
    with pytest.raises(ValueError, match="盘口研究"):
        export_depth_replay(["not-a-real-source"], destination)
