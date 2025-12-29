# ETLX docs (Hugo site)

This directory is a Hugo site. To preview locally:

1. Install Hugo Extended (https://gohugo.io/getting-started/installing/).
2. Run:

```bash
hugo server -s docs -D
```

To publish from CI the workflow uses .github/workflows/hugo-gh-pages.yml and deploys the generated site to the gh-pages branch.
