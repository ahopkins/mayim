const { apiPages } = require("./apiPages");
const { description } = require('../../package')
const guidePages = [
            '/guide/',
            '/guide/install',
            '/guide/basics',
            '/guide/sqlfiles',
            '/guide/simple',
            '/guide/executors',
            '/guide/hydrators',
            '/guide/pydantic',
            '/guide/extensions',
          ]
module.exports = {
  title: '💧 Mayim',
  description: description,
  head: [
    ['meta', { name: 'theme-color', content: '#5dadec' }],
    ['meta', { name: 'apple-mobile-web-app-capable', content: 'yes' }],
    ['meta', { name: 'apple-mobile-web-app-status-bar-style', content: 'black' }]
  ],
  base: '/mayim/',
  markdown: {
    anchor: {
      permalink: true,
    },
    extendMarkdown: (md) => {
      md.use(require("markdown-it-multicolumn").default);
    },
  },
  themeConfig: {
    repo: 'https://github.com/ahopkins/mayim',
    docsDir: "docs/src",
    docsBranch: "main",
    editLinks: true,
    lastUpdated: true,
    sidebarDepth: 2,
    nav: [
      {
        text: 'Documentation',
        items: [
          {
            text: "User Guide",
            items: [
              {"text": "Installation", link: "/guide/install"},
              {"text": "Basics", link: "/guide/basics"},
              {"text": "Writing SQL files", link: "/guide/sqlfiles"},
              {"text": "Full simple example", link: "/guide/simple"},
              {"text": "Creating Executors", link: "/guide/executors"},
              {"text": "Custom Hydrators", link: "/guide/hydrators"},
              {"text": "Working with Pydantic", link: "/guide/pydantic"},
              {"text": "Framework extensions", link: "/guide/extensions"},
            ]
          },
          {
            text: "API Docs",
            items: [
              {"text": "Mayim Package", link: "/api/index"},
            ]
          }
        ],
      },
      {
        text: 'Support/Discussion',
        link: 'https://github.com/ahopkins/mayim/discussions'
      }
    ],
    sidebar: {
      '/guide/': [
        {
          title: 'Guide',
          collapsable: false,
          children: guidePages
        },
        {
          title: 'API Docs',
          collapsable: true,
          children: apiPages
        }
      ],
      '/api/': [
        {
          title: 'Guide',
          collapsable: true,
          children: guidePages
        },
        {
          title: 'API Docs',
          collapsable: false,
          children: apiPages
        }
      ],
    }
  },

  /**
   * Apply plugins，ref：https://v1.vuepress.vuejs.org/zh/plugin/
   */
  plugins: [
    '@vuepress/plugin-back-to-top',
    '@vuepress/plugin-medium-zoom',
    "tabs",
    [
      "vuepress-plugin-code-copy",
      { color: "#5dadec", backgroundTransition: false },
    ],
  ]
}
