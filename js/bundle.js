// <stdin>
(() => {
  "use strict";
  const highlights = document.querySelectorAll(".highlight");
  if (!highlights) {
    return;
  }
  for (const highlight of highlights) {
    const copyBtn = document.querySelector(".highlight__copy");
    if (!copyBtn) {
      continue;
    }
    copyBtn.addEventListener("click", () => {
      const codeElement = highlight.querySelector("[data-lang]");
      const code = (codeElement?.innerText ?? "").replaceAll("\n\n", "\n");
      navigator.clipboard.writeText(code);
      copyBtn.classList.add("copied");
      setTimeout(() => {
        copyBtn.classList.remove("copied");
      }, 1e3);
    });
  }
})();
(() => {
  "use strict";
  const menu = document.querySelector(".menu");
  if (!menu) {
    return;
  }
  const menuToggles = document.querySelectorAll(
    '[data-action="toggle-menu"]'
  );
  for (const toggle of menuToggles) {
    toggle.addEventListener("click", (e) => {
      e.preventDefault();
      menu.classList.add("is-transitioning");
      menu.classList.toggle("open");
      setTimeout(() => {
        menu.classList.remove("is-transitioning");
      }, 200);
    });
  }
})();
(() => {
  "use strict";
  let searchData = [];
  const searchSections = document.querySelectorAll(".sidebar__section--search");
  if (!searchSections) {
    return;
  }
  document.addEventListener("keydown", (e) => {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement || e.target instanceof HTMLButtonElement || e.target instanceof HTMLSelectElement) {
      return;
    }
    if (e.key === "/" && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      const searchInput = searchSections[0].querySelector(
        'input[type="text"], input[type="search"]'
      );
      if (searchInput) {
        searchInput.focus();
      }
    }
  });
  for (const searchSection of searchSections) {
    let updateSidebarSearchResultsMaxHeight = function(sidebarContent2, searchContainer2, resultsContainer2) {
      if (!resultsContainer2.children.length) {
        return;
      }
      const maxHeight = sidebarContent2.clientHeight - searchContainer2.clientHeight;
      resultsContainer2.style.maxHeight = `${maxHeight}px`;
    }, formatSearchResult = function(result, query) {
      const titleElement = document.createElement("div");
      titleElement.classList.add("search-result__title");
      titleElement.innerHTML = extractSnippet(result.title, query);
      const snippetElement = document.createElement("p");
      snippetElement.classList.add("search-result__body");
      snippetElement.innerHTML = extractSnippet(result.content, query);
      return titleElement.outerHTML + snippetElement.outerHTML;
    }, extractSnippet = function(content, query) {
      if (!content || !query) {
        return "";
      }
      const queryLower = query.toLowerCase();
      const contentLower = content.toLowerCase();
      const queryIndex = contentLower.indexOf(queryLower);
      const snippetLength = 140;
      if (queryIndex === -1) {
        if (content.length < snippetLength) {
          return content;
        }
        return `${content.substring(0, snippetLength)}...`;
      }
      const start = Math.max(0, queryIndex - snippetLength / 2);
      const end = Math.min(content.length, start + snippetLength);
      let snippet = content.substring(start, end);
      if (start > 0 && end < content.length) {
        snippet = `...${snippet}...`;
      } else if (start > 0) {
        snippet = `...${snippet}`;
      } else if (end < content.length) {
        snippet = `${snippet}...`;
      }
      snippet = snippet.replace(
        new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi"),
        "<mark>$&</mark>"
      );
      return snippet;
    };
    const searchInput = searchSection.querySelector(
      'input[type="text"], input[type="search"]'
    );
    const resultsContainer = searchSection.querySelector(
      ".sidebar__section--search-results"
    );
    if (!searchInput || !resultsContainer) {
      continue;
    }
    const sidebarContent = document.querySelector(".sidebar__content");
    const searchContainer = searchInput.closest(".sidebar__section--search");
    if (!sidebarContent || !searchContainer) {
      continue;
    }
    updateSidebarSearchResultsMaxHeight(
      sidebarContent,
      searchContainer,
      resultsContainer
    );
    globalThis.addEventListener("resize", () => {
      updateSidebarSearchResultsMaxHeight(
        sidebarContent,
        searchContainer,
        resultsContainer
      );
    });
    let isSearching = false;
    const minChars = 3;
    searchInput.addEventListener("input", () => {
      search(
        searchInput.value,
        sidebarContent,
        searchContainer,
        searchInput,
        resultsContainer
      );
    });
    searchInput.addEventListener("focus", () => {
      search(
        searchInput.value,
        sidebarContent,
        searchContainer,
        searchInput,
        resultsContainer
      );
    });
    async function search(query, sidebarContent2, searchContainer2, searchInput2, resultsContainer2) {
      if (isSearching) {
        return;
      }
      searchInput2.classList.remove("has-results");
      resultsContainer2.innerHTML = "";
      if (query.length < minChars) {
        return;
      }
      isSearching = true;
      if (searchData.length === 0) {
        try {
          const response = await fetch("/index.json");
          searchData = await response.json();
        } catch (error) {
          console.error(error);
        }
      }
      const results = searchData.filter(
        (item) => item.title.toLowerCase().includes(query.toLowerCase()) || item.description.toLowerCase().includes(query.toLowerCase()) || item.content.toLowerCase().includes(query.toLowerCase())
      ).slice(0, 10);
      for (const result of results) {
        const li = document.createElement("li");
        const a = document.createElement("a");
        a.href = result.permalink;
        a.classList.add("search-result");
        a.innerHTML = formatSearchResult(result, query);
        li.appendChild(a);
        resultsContainer2.appendChild(li);
      }
      if (results.length > 0) {
        searchInput2.classList.add("has-results");
        updateSidebarSearchResultsMaxHeight(
          sidebarContent2,
          searchContainer2,
          resultsContainer2
        );
      }
      isSearching = false;
    }
  }
})();
(() => {
  "use strict";
  const sidebar = document.querySelector(".sidebar");
  if (!sidebar) {
    return;
  }
  const sidebarToggles = document.querySelectorAll(
    '[data-action="toggle-sidebar"]'
  );
  for (const toggle of sidebarToggles) {
    toggle.addEventListener("click", (e) => {
      e.preventDefault();
      sidebar.classList.add("is-transitioning");
      sidebar.classList.toggle("open");
      setTimeout(() => {
        sidebar.classList.remove("is-transitioning");
      }, 200);
    });
  }
})();
(() => {
  "use strict";
  const tocs = document.querySelectorAll(".toc");
  if (!tocs) {
    return;
  }
  for (const toc of tocs) {
    const tocHeader = toc.querySelector('[data-action="toggle-toc"]');
    if (!tocHeader) {
      continue;
    }
    tocHeader.addEventListener("click", (e) => {
      e.preventDefault();
      const tocToggle = tocHeader.querySelector(".toc__toggle");
      if (!tocToggle || getComputedStyle(tocToggle).display === "none") {
        return;
      }
      toc.classList.toggle("open");
    });
  }
})();
