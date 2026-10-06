# ruff: ignore[implicit-namespace-package]  # conf.py - конфигурация Sphinx, а не модуль пакета
"""Конфигурация Sphinx для сайта документации tallyho.

Сборка: ``uv run poe docs`` (HTML в ``docs/_build/html``). Страницы написаны в MyST Markdown,
справочник API собирается из докстрингов пакета.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

from docutils import nodes
from sphinxcontrib.mermaid import mermaid

if TYPE_CHECKING:
    from sphinx.application import Sphinx

_ROOT = Path(__file__).resolve().parents[1]
# Справочник API импортирует пакет из исходников, а не из установленного wheel.
sys.path.insert(0, str(_ROOT / "src"))
# Собственные расширения сайта: таблицы схемы из кода.
sys.path.insert(0, str(_ROOT / "docs" / "_ext"))

_META = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
# Адрес репозитория задан в одном месте - в pyproject.toml.
_REPOSITORY = str(_META["urls"]["Repository"]).rstrip("/")
_OWNER, _REPO = _REPOSITORY.removeprefix("https://github.com/").split("/")

project = str(_META["name"])
author = "tallyho contributors"
copyright = f"2026, {author}"  # ruff: ignore[builtin-variable-shadowing]  # имя настройки задаёт Sphinx
version = release = str(_META["version"])
language = "ru"
# Русские подписи темы Shibuya: Sphinx сам собирает .mo из docs/_locale.
locale_dirs = ["_locale"]

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "sphinx_llm_friendly",
    "tallyho_schema",
]

# В docs/ лежат и внутренние документы проекта (ARCHITECTURE, ACCEPTANCE, plan/): на сайт
# попадает только то, что перечислено здесь.
include_patterns = ["index.md", "guide/**", "integrations/**", "architecture/**", "reference/**"]
exclude_patterns = ["_build", "_locale", "_ext"]
source_suffix = {".md": "markdown"}
master_doc = "index"

# -- MyST -----------------------------------------------------------------------------------
myst_enable_extensions = ["colon_fence", "deflist", "fieldlist", "attrs_inline"]
# Якоря заголовков как на GitHub: одни и те же ссылки работают на сайте и в репозитории.
myst_heading_anchors = 3
# Блок ```mermaid рисует диаграмму и на сайте, и в просмотре файла на GitHub.
myst_fence_as_directive = ["mermaid"]

# -- Справочник API -------------------------------------------------------------------------
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autodoc_class_signature = "separated"
autodoc_default_options = {"show-inheritance": True}
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/21/", None),
}

# -- HTML -----------------------------------------------------------------------------------
html_theme = "shibuya"
html_title = project
html_baseurl = f"https://{_OWNER.lower()}.github.io/{_REPO}/"
html_favicon = "_static/images/favicon.svg"
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
html_js_files = ["js/sidebar-scroll.js"]
html_show_sourcelink = False
html_theme_options = {
    "accent_color": "green",
    "github_url": _REPOSITORY,
    "globaltoc_expand_depth": 1,
    "light_logo": "_static/images/logo-light.svg",
    "dark_logo": "_static/images/logo-dark.svg",
    "nav_links": [
        {"title": "Руководство", "url": "guide/getting-started"},
        {"title": "Интеграции", "url": "integrations/index"},
        {"title": "Архитектура", "url": "architecture/index"},
        {"title": "API", "url": "reference/api"},
        {"title": "История изменений", "url": f"{_REPOSITORY}/blob/main/CHANGELOG.md"},
    ],
    # Кнопку «скопировать страницу как Markdown» добавляет sphinx_llm_friendly.
    "show_ai_links": False,
}
html_context = {
    "source_type": "github",
    "source_user": _OWNER,
    "source_repo": _REPO,
    "source_version": "main",
    "source_docs_path": "/docs/",
}

# -- Диаграммы ------------------------------------------------------------------------------
# Тему диаграммы расширение переключает само, вслед за темой сайта.
mermaid_light_theme = "neutral"
mermaid_dark_theme = "dark"
mermaid_height = "auto"
mermaid_fullscreen = False

# -- llms.txt -------------------------------------------------------------------------------
# sphinx_llm_friendly кладёт рядом с каждой HTML-страницей её Markdown-версию и собирает
# llms.txt и llms-full.txt в корне сайта.
llm_friendly_llms_txt_summary = (
    f"Документация tallyho {release}: групповой учёт задач поверх брокера на PostgreSQL. "
    "Вся документация одним файлом - llms-full.txt рядом с этим файлом."
)

copybutton_prompt_text = r"\$ "
copybutton_prompt_is_regexp = True


def _mermaid_as_markdown(translator: object, node: nodes.Element) -> NoReturn:
    """В Markdown-версии страницы диаграмма остаётся блоком ``mermaid``.

    Raises:
        SkipNode: всегда; так docutils узнаёт, что узел уже обработан.
    """
    add = translator.add  # type: ignore[attr-defined]  # MarkdownTranslator из sphinx_llm_friendly
    add("```mermaid", prefix_eol=2, suffix_eol=1)
    add(str(node["code"]))
    add("```", prefix_eol=1, suffix_eol=2)
    raise nodes.SkipNode


def setup(app: Sphinx) -> None:
    """Научить sphinx_llm_friendly узлу диаграммы: без обработчика он даёт предупреждение."""
    app.add_node(mermaid, override=True, llm_markdown=(_mermaid_as_markdown, None))
