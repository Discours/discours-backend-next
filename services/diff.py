import re
from difflib import ndiff


def get_diff(original, modified):
    """
    Get the difference between two strings using difflib.

    Parameters:
    - original: The original string.
    - modified: The modified string.

    Returns:
    A list of differences.
    """
    diff = list(ndiff(original.split(), modified.split()))
    return diff


def apply_diff(original, diff):
    """
    Apply the difference to the original string.

    Parameters:
    - original: The original string.
    - diff: The difference obtained from get_diff function.

    Returns:
    The modified string.
    """
    result = []
    pattern = re.compile(r"^(\+|-) ")

    for line in diff:
        match = pattern.match(line)
        if match:
            op = match.group(1)
            content = line[2:]
            if op == "+":
                result.append(content)
            elif op == "-":
                # Ignore deleted lines
                pass
        else:
            result.append(line)

    return " ".join(result)
