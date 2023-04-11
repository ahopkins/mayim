(window.webpackJsonp=window.webpackJsonp||[]).push([[38],{474:function(t,a,s){"use strict";s.r(a);var n=s(35),e=Object(n.a)({},(function(){var t=this,a=t.$createElement,s=t._self._c||a;return s("ContentSlotsDistributor",{attrs:{"slot-key":t.$parent.slotKey}},[s("h1",{attrs:{id:"introduction"}},[s("a",{staticClass:"header-anchor",attrs:{href:"#introduction"}},[t._v("#")]),t._v(" Introduction")]),t._v(" "),s("h2",{attrs:{id:"what-is-mayim"}},[s("a",{staticClass:"header-anchor",attrs:{href:"#what-is-mayim"}},[t._v("#")]),t._v(" 💧 What is Mayim?")]),t._v(" "),s("p",[t._v("The simplest way to describe it would be to call it a "),s("strong",[t._v("one-way ORM")]),t._v(". That is to say that it does "),s("em",[t._v("not")]),t._v(" craft SQL statements for you. But it does take your executed query results and map them back to objects.")]),t._v(" "),s("p",[t._v("Think of it as "),s("strong",[t._v("BYOQ")]),t._v(" (Bring Your Own Query) mapping utility.")]),t._v(" "),s("p",[t._v("You supply the query, it handles the execution and model hydration.")]),t._v(" "),s("h2",{attrs:{id:"why"}},[s("a",{staticClass:"header-anchor",attrs:{href:"#why"}},[t._v("#")]),t._v(" 💧 Why?")]),t._v(" "),s("p",[t._v("I have nothing against ORMs, truthfully. They serve a great purpose and can be the right tool for the job in many situations. I just prefer not to use them where possible. Instead, I would rather "),s("strong",[t._v("have control of my SQL statements")]),t._v(".")]),t._v(" "),s("p",[t._v("The typical tradeoff though is that there is more work needed to hydrate from SQL queries to objects. Without an ORM, it is generally more difficult to maintain your code base as your schema changes.")]),t._v(" "),s("p",[t._v("Mayim aims to solve that by providing an "),s("strong",[t._v("elegant API")]),t._v(" with typed objects and methods. Mayim fully embraces "),s("strong",[t._v("type annotations")]),t._v(" and encourages their usage.")]),t._v(" "),s("h2",{attrs:{id:"how"}},[s("a",{staticClass:"header-anchor",attrs:{href:"#how"}},[t._v("#")]),t._v(" 💧 How?")]),t._v(" "),s("p",[t._v("There are two parts to it:")]),t._v(" "),s("ol",[s("li",[t._v("Write some SQL in a location that Mayim can access at startup ("),s("em",[t._v("this can be in a decorator as shown below, or "),s("code",[t._v(".sql")]),t._v(" files as seen later on")]),t._v(")")]),t._v(" "),s("li",[t._v("Create an "),s("code",[t._v("Executor")]),t._v(" that defines the query parameters that will be passed to your SQL")])]),t._v(" "),s("p",[t._v("Here is a real simple example:")]),t._v(" "),s("div",{staticClass:"language-python extra-class"},[s("pre",{pre:!0,attrs:{class:"language-python"}},[s("code",[s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("import")]),t._v(" asyncio\n"),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("from")]),t._v(" mayim "),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("import")]),t._v(" Mayim"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(",")]),t._v(" SQLiteExecutor"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(",")]),t._v(" query\n"),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("from")]),t._v(" dataclasses "),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("import")]),t._v(" dataclass\n\n\n"),s("span",{pre:!0,attrs:{class:"token decorator annotation punctuation"}},[t._v("@dataclass")]),t._v("\n"),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("class")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token class-name"}},[t._v("Person")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(":")]),t._v("\n    name"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(":")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token builtin"}},[t._v("str")]),t._v("\n\n\n"),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("class")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token class-name"}},[t._v("PersonExecutor")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),t._v("SQLiteExecutor"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(":")]),t._v("\n    "),s("span",{pre:!0,attrs:{class:"token decorator annotation punctuation"}},[t._v("@query")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),s("span",{pre:!0,attrs:{class:"token string"}},[t._v('"SELECT $name as name"')]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),t._v("\n    "),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("async")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("def")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token function"}},[t._v("select_person")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),t._v("self"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(",")]),t._v(" name"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(":")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token builtin"}},[t._v("str")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token operator"}},[t._v("-")]),s("span",{pre:!0,attrs:{class:"token operator"}},[t._v(">")]),t._v(" Person"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(":")]),t._v("\n        "),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(".")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(".")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(".")]),t._v("\n\n\n"),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("async")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("def")]),t._v(" "),s("span",{pre:!0,attrs:{class:"token function"}},[t._v("run")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(":")]),t._v("\n    executor "),s("span",{pre:!0,attrs:{class:"token operator"}},[t._v("=")]),t._v(" PersonExecutor"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),t._v("\n    Mayim"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),t._v("db_path"),s("span",{pre:!0,attrs:{class:"token operator"}},[t._v("=")]),s("span",{pre:!0,attrs:{class:"token string"}},[t._v('"./example.db"')]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),t._v("\n    "),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("print")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),s("span",{pre:!0,attrs:{class:"token keyword"}},[t._v("await")]),t._v(" executor"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(".")]),t._v("select_person"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),t._v("name"),s("span",{pre:!0,attrs:{class:"token operator"}},[t._v("=")]),s("span",{pre:!0,attrs:{class:"token string"}},[t._v('"Adam"')]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),t._v("\n\n\nasyncio"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(".")]),t._v("run"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),t._v("run"),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v("(")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),s("span",{pre:!0,attrs:{class:"token punctuation"}},[t._v(")")]),t._v("\n")])])]),s("p",[t._v("This example should be complete and run as is.")]),t._v(" "),s("p",[t._v("Let's continue on to see how we can install it. 😎")])])}),[],!1,null,null,null);a.default=e.exports}}]);