import pandas as pd

from src.indicator_domain.infrastructure.qtime.decoration_repository import (
    QTimeDecorationRepository,
)


def test_qtime_decoration_repository_round_trips_the_decision_ledger(tmp_path) -> None:
    repository = QTimeDecorationRepository(tmp_path / "qtime_oos_decoration.xlsx")
    decisions = pd.DataFrame(
        [
            {
                "prodcode": "M626",
                "step_desc": "Half Cutting->EVA&TFE",
                "lot_id": "L001",
                "timekey": "20260902010000",
                "flag": False,
            }
        ]
    )

    repository.save_decisions(decisions)

    result = repository.load_decisions()
    assert result.to_dict("records") == decisions.to_dict("records")
