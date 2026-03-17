from app.notebooks.templates.aiid_template import AIID_TEMPLATE


def test_aiid_template_contains_extended_visual_analyses():
    cells = AIID_TEMPLATE.get("cells") or []
    assert len(cells) >= 15

    ids = [str(cell.get("id")) for cell in cells]
    assert len(ids) == len(set(ids))

    analysis_types = [str(cell.get("analysis_type", "")) for cell in cells]
    assert "detailed_summary" in analysis_types
    assert analysis_types.count("heatmap") >= 2

    developer_cells = [
        cell
        for cell in cells
        if (cell.get("config") or {}).get("field") == "allegeddeveloperofaisystem_primary"
    ]
    assert developer_cells

