import secrets
import string

B36_ALPHABET = string.ascii_lowercase + string.digits


def random_id(length: int = 10) -> str:
    return "".join(secrets.choice(B36_ALPHABET) for _ in range(length))
