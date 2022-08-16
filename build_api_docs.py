from dataclasses import dataclass
import html
from importlib import import_module
from operator import attrgetter
import re
from typing import List, Optional, TextIO, Type
from pydoc_markdown.interfaces import Context, SourceLinker
from pydoc_markdown.contrib.loaders.python import PythonLoader
from pydoc_markdown.contrib.renderers.markdown import (
    MarkdownRenderer,
    MarkdownReferenceResolver,
)
from pydoc_markdown.contrib.processors.google import GoogleProcessor
from pydoc_markdown.contrib.processors.filter import FilterProcessor
from pydoc_markdown.contrib.processors.crossref import CrossrefProcessor
from pydoc_markdown.util.docspec import ApiSuite
import docspec_python
import docspec

slugify = re.compile(r"[^a-zA-Z0-9_\-]")
dedup = re.compile(r"(-)\1+")
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
                self._render_root_level_object(fp, level, obj)
        else:
            super()._render_object(fp, level, obj)
        if isinstance(obj, docspec.Class):
            assert isinstance(obj.parent, docspec.Module)
            module = import_module(obj.parent.name)
            cls = getattr(module, obj.name)
            resolver = MayimMarkdownReferenceResolver(global_=True)

            parent_links = []
            for base in cls.__mro__:
                resolved = None
                fullname = f"{base.__module__}.{base.__qualname__}"
                if fullname.startswith("mayim") and base.__name__ != obj.name:
                    resolved = resolver.resolve_reference(
                        suite, obj, fullname, [docspec.Indirection]
                    )
                    if resolved:
                        module_name, member_name = self._resolve_name(
                            resolved
                        ).rsplit(".", 1)
                        parent_links.append(
                            f"[{resolved.name}](./{module_name}.html"
                            f"#{member_name})"
                        )

            if parent_links:
                parents = ", ".join(parent_links)
                fp.write(f"**Parents**: {parents}\n\n")

        if (
            obj.parent
            and isinstance(obj.parent, docspec.Class)
            and isinstance(obj, docspec.Variable)
        ):
            if obj.value:
                default = obj.value.replace("\n", "")
                fp.write(f"**Default**: `{default}`\n\n")

    def _render_root_level_object(
        self, fp: TextIO, level: int, obj: docspec.Indirection
    ):
        self._render_header(fp, level, obj)
        full_name = "mayim" + obj.target
        module_name, member_name = full_name.rsplit(".", 1)
        module = list(docspec_python.load_python_modules([module_name]))[0]
        item = docspec.get_member(module, member_name)

        if item and item.docstring:
            docstring = (
                html.escape(item.docstring.content)
                if self.escape_html_in_docstring
                else item.docstring.content
            )
            lines = docstring.splitlines()
            summary = ""
            for line in lines:
                if not line:
                    break
                summary += f" {line}"
            fp.write(f"{summary}\n\n")

        fp.write("```{}\n".format("python" if self.code_lang else ""))
        fp.write(f"from mayim import {member_name}")
        fp.write("\n```\n\n")

        fp.write(f"See [{full_name}](./{module_name}.html#{member_name})\n\n")

    def _resolve_name(self, obj: docspec.ApiObject) -> str:
        name = ""
        if obj.path:
            for part in obj.path:
                if part is not self and part.name != obj.name:
                    name += self._resolve_name(part)
        if not name:
            return obj.name
        name = f"{name}.{obj.name}"
        if isinstance(obj, docspec.Module) and name.startswith("."):
            name = f"mayim.{name}"
        return name

    def _render_toc(self, fp: TextIO, level: int, obj: docspec.ApiObject):
        if level > self.toc_maxdepth:
            return
        title = self._slugify(self._get_title(obj))
        display = self._escape(obj.name)
        if not self.add_module_prefix and isinstance(obj, docspec.Module):
            display = display.split(".")[-1]
        fp.write("  " * level + "* [{}](#{})\n".format(display, title))
        level += 1
        for child in sorted(
            getattr(obj, "members", []), key=attrgetter("name")
        ):
            self._render_toc(fp, level, child)

    @staticmethod
    def _slugify(text: str) -> str:
        slug = slugify.sub("-", text.lower())
        return dedup.sub("-", slug).strip("-")


@dataclass
class MayimMarkdownReferenceResolver(MarkdownReferenceResolver):
    def resolve_reference(
        self,
        suite: ApiSuite,
        scope: docspec.ApiObject,
        ref: str,
        exclusions: Optional[List[Type[docspec.ApiObject]]] = None,
    ) -> Optional[docspec.ApiObject]:
        ref_split = ref.split(".")

        resolved = self._resolve_local_reference(scope, ref_split)
        if resolved and not self._excluded(resolved, exclusions):
            return resolved

        if self.global_:

            def _recurse(
                obj: docspec.ApiObject,
            ) -> Optional[docspec.ApiObject]:
                resolved = self._resolve_reference_in_members(obj, ref_split)
                if resolved and not self._excluded(resolved, exclusions):
                    return resolved
                if isinstance(obj, docspec.HasMembers):
                    for member in obj.members:
                        resolved = _recurse(member)
                        if resolved and not self._excluded(
                            resolved, exclusions
                        ):
                            return resolved
                return None

            for module in suite:
                resolved = _recurse(module)
                if resolved and not self._excluded(resolved, exclusions):
                    return resolved

        return None

    def _resolve_reference_in_members(
        self, obj: Optional[docspec.ApiObject], ref: List[str]
    ) -> Optional[docspec.ApiObject]:
        if not obj:
            return None
        fullref = ".".join(ref)
        for part_name in ref:
            retrieved = docspec.get_member(
                obj, part_name
            ) or docspec.get_member(obj, fullref)
            if retrieved:
                return retrieved
        return None

    def _excluded(
        self,
        obj: docspec.ApiObject,
        exclusions: Optional[List[Type[docspec.ApiObject]]] = None,
    ) -> bool:
        if not exclusions:
            return False
        return (
            any(isinstance(obj, exc) for exc in exclusions)
            if exclusions
            else False
        )


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


@dataclass
class MayimSourceLinker(SourceLinker):
    def get_source_url(self, obj: docspec.ApiObject) -> Optional[str]:
        base = "https://github.com/ahopkins/mayim/tree/main/"
        return base + obj.location.filename + f"#L{obj.location.lineno}"


context = Context(directory=".")
loader = PythonLoader(
    packages=["mayim"],
)
source_linker = MayimSourceLinker()
renderer = MayimRenderer(
    render_module_header=False,
    insert_header_anchors=False,
    render_page_title=True,
    descriptive_class_title=False,
    signature_code_block=True,
    use_fixed_header_levels=False,
    source_linker=source_linker,
    code_headers=True,
    render_typehint_in_data_header=True,
    signature_with_decorators=True,
    render_toc=True,
)

source_linker.init(context)
loader.init(context)
renderer.init(context)

modules = list(sorted(loader.load(), key=attrgetter("name")))
suite = ApiSuite(modules)
resolver = renderer.get_resolver(modules)
CrossrefProcessor().process(modules, resolver)
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
renderer.render_toc = False
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
