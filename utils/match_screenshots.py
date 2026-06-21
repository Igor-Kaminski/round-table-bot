import os
from pathlib import Path


IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg")
MATCH_SCREENSHOT_DIR = "match_screenshots"
MAX_SCREENSHOT_BYTES = 10 * 1024 * 1024


def screenshot_extension(filename):
    extension = os.path.splitext(str(filename or ""))[1].lower()
    return extension if extension in IMAGE_EXTENSIONS else None


def attachment_is_supported(attachment):
    extension = screenshot_extension(getattr(attachment, "filename", None))
    size = int(getattr(attachment, "size", 0) or 0)
    return extension is not None and size <= MAX_SCREENSHOT_BYTES


def resolve_screenshot_path(file_path):
    if not file_path:
        return None

    normalized = str(file_path).replace("\\", os.sep).replace("/", os.sep)
    candidate = Path(normalized)
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    resolved = candidate.resolve()
    screenshot_root = (Path.cwd() / MATCH_SCREENSHOT_DIR).resolve()
    if resolved != screenshot_root and screenshot_root not in resolved.parents:
        return None
    return resolved


def validate_image_file(file_path, extension):
    try:
        with open(file_path, "rb") as image_file:
            signature = image_file.read(12)
    except OSError:
        return False

    if extension == ".png":
        return signature.startswith(b"\x89PNG\r\n\x1a\n")
    if extension in {".jpg", ".jpeg"}:
        return signature.startswith(b"\xff\xd8\xff")
    return False


def move_screenshot_file(source_path, match_id, attachment_id, extension):
    extension = str(extension or "").lower()
    if extension not in IMAGE_EXTENSIONS or not validate_image_file(source_path, extension):
        raise ValueError("The attachment is not a valid PNG or JPEG image.")

    os.makedirs(MATCH_SCREENSHOT_DIR, exist_ok=True)
    relative_path = Path(MATCH_SCREENSHOT_DIR) / f"{int(match_id)}_{int(attachment_id)}{extension}"
    destination_path = resolve_screenshot_path(relative_path.as_posix())
    os.replace(source_path, destination_path)
    return relative_path.as_posix()


def remove_screenshot_file(file_path):
    resolved_path = resolve_screenshot_path(file_path)
    if not resolved_path:
        return False
    try:
        resolved_path.unlink(missing_ok=True)
        return True
    except OSError:
        return False
