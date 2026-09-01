"""Factory classification rules from the FineReport contract."""


def classify_shop(f_step: str) -> str:
    if f_step.startswith("1"):
        return "ARRAY"
    if f_step.startswith("2"):
        return "OLED"
    return "TP"
