import os
import json

_LOCALES = {}
_LOCALE_PATH = os.path.join(os.path.dirname(__file__), "..", "locales")

def load_locale(lang):
    if lang not in _LOCALES:
        file_path = os.path.join(_LOCALE_PATH, f"{lang}.json")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                _LOCALES[lang] = json.load(f)
        except FileNotFoundError:
            _LOCALES[lang] = {}
    return _LOCALES[lang]

def translate(key, lang="zh"):
    locale = load_locale(lang)
    return locale.get(key, key)