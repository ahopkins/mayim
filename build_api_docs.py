import html
from operator import attrgetter
from typing import TextIO
from pydoc_markdown.interfaces import Context
from pydoc_markdown.contrib.loaders.python import PythonLoader
from pydoc_markdown.contrib.renderers.markdown import MarkdownRenderer
from pydoc_markdown.contrib.processors.google import GoogleProcessor
from pydoc_markdown.contrib.processors.filter import FilterProcessor
import docspec_python
import docspec


WHITELIST = ("__init__",)


class MayimRenderer(MarkdownRenderer):
    def _render_recursive(
        self, fp: TextIO, level: int, obj: docspec.ApiObject
    ):
        self._render_object(fp, level, obj)
        level += 1
        members = sorted(getattr(obj, "members", []), key=attrgetter("name"))
        for member in members:
            self._render_recursive(fp, level, member)

    def _render_object(self, fp: TextIO, level: int, obj: docspec.ApiObject):
        if isinstance(obj, docspec.Indirection):
            if obj.parent and obj.parent.name == "mayim":
                self._render_header(fp, level, obj)
                full_name = "mayim" + obj.target
                module_name, member_name = full_name.rsplit(".", 1)
                module = list(
                    docspec_python.load_python_modules([module_name])
                )[0]
                item = docspec.get_member(module, member_name)

                if item and item.docstring:
                    docstring = (
                        html.escape(item.docstring.content)
                        if self.escape_html_in_docstring
                        else item.docstring.content
                    )
                    lines = docstring.splitlines()
                    summary = lines[0]
                    fp.write(f"{summary}\n\n")

                fp.write("```{}\n".format("python" if self.code_lang else ""))
                fp.write(f"from mayim import {member_name}")
                fp.write("\n```\n\n")

                fp.write(
                    f"See [{full_name}](./{module_name}.html"
                    f"#{member_name})\n\n"
                )
        else:
            super()._render_object(fp, level, obj)


class MayimFilter(FilterProcessor):
    def _match(self, obj: docspec.ApiObject) -> bool:
        result = super()._match(obj)
        if result:
            result = self._additional(obj)
        return result

    def _additional(self, obj: docspec.ApiObject) -> bool:
        if isinstance(obj, docspec.Indirection) and not obj.target.startswith(
            "."
        ):
            return False
        elif isinstance(obj, docspec.Variable) and obj.name.isupper():
            return False
        elif obj.name.startswith("_") and obj.name not in WHITELIST:
            return False
        return True


context = Context(directory=".")
loader = PythonLoader(
    packages=["mayim"],
)
renderer = MayimRenderer(
    render_module_header=False,
    insert_header_anchors=False,
    render_page_title=True,
    descriptive_class_title=False,
)


loader.init(context)
renderer.init(context)

modules = list(sorted(loader.load(), key=attrgetter("name")))
resolver = renderer.get_resolver(modules)
GoogleProcessor().process(modules, resolver)
MayimFilter(
    documented_only=False,
    do_not_filter_modules=False,
    skip_empty_modules=True,
).process(modules, resolver)

index_content = """
## Index
"""
page_links = []
for module in modules:
    link = f"/api/{module.name}.md"
    file_path = f"docs/src{link}"
    with open(file_path, "w") as f:
        renderer.render_single_page(f, [module], page_title=module.name)

    if module.name != "mayim":
        index_content += f"- [{module.name}](./{module.name}.html)\n"
        page_links.append(link)

renderer.render_page_title = False
renderer.use_fixed_header_levels = True
renderer.header_level_by_type["Indirection"] = 3
main_modules = [module for module in modules if module.name == "mayim"]
file_path = "docs/src/api/index.md"
with open(file_path, "w") as f:
    f.write("# Mayim Package\n")
    f.write("\n## Root objects\n\n")
    renderer.render_single_page(f, main_modules)
    f.write(index_content)

links = ",\n        ".join([f"'{link}'" for link in page_links])
api_pages = f"""
module.exports = {{
    apiPages: [
        '/api/',
        {links}
    ]
}}
"""

file_path = "docs/src/.vuepress/apiPages.js"
with open(file_path, "w") as f:
    f.write(api_pages)
