(() => {
  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/dom/data.js
  var elementMap = /* @__PURE__ */ new Map();
  var data_default = {
    set(element, key, instance) {
      if (!elementMap.has(element)) {
        elementMap.set(element, /* @__PURE__ */ new Map());
      }
      const instanceMap = elementMap.get(element);
      if (!instanceMap.has(key) && instanceMap.size !== 0) {
        console.error(`Bootstrap doesn't allow more than one instance per element. Bound instance: ${Array.from(instanceMap.keys())[0]}.`);
        return;
      }
      instanceMap.set(key, instance);
    },
    get(element, key) {
      if (elementMap.has(element)) {
        return elementMap.get(element).get(key) || null;
      }
      return null;
    },
    remove(element, key) {
      if (!elementMap.has(element)) {
        return;
      }
      const instanceMap = elementMap.get(element);
      instanceMap.delete(key);
      if (instanceMap.size === 0) {
        elementMap.delete(element);
      }
    }
  };

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/util/index.js
  var MILLISECONDS_MULTIPLIER = 1e3;
  var TRANSITION_END = "transitionend";
  var parseSelector = (selector) => {
    if (selector && window.CSS && window.CSS.escape) {
      selector = selector.replace(/#([^\s"#']+)/g, (match, id) => `#${CSS.escape(id)}`);
    }
    return selector;
  };
  var toType = (object) => {
    if (object === null || object === void 0) {
      return `${object}`;
    }
    return Object.prototype.toString.call(object).match(/\s([a-z]+)/i)[1].toLowerCase();
  };
  var getTransitionDurationFromElement = (element) => {
    if (!element) {
      return 0;
    }
    let { transitionDuration, transitionDelay } = window.getComputedStyle(element);
    const floatTransitionDuration = Number.parseFloat(transitionDuration);
    const floatTransitionDelay = Number.parseFloat(transitionDelay);
    if (!floatTransitionDuration && !floatTransitionDelay) {
      return 0;
    }
    transitionDuration = transitionDuration.split(",")[0];
    transitionDelay = transitionDelay.split(",")[0];
    return (Number.parseFloat(transitionDuration) + Number.parseFloat(transitionDelay)) * MILLISECONDS_MULTIPLIER;
  };
  var triggerTransitionEnd = (element) => {
    element.dispatchEvent(new Event(TRANSITION_END));
  };
  var isElement = (object) => {
    if (!object || typeof object !== "object") {
      return false;
    }
    if (typeof object.jquery !== "undefined") {
      object = object[0];
    }
    return typeof object.nodeType !== "undefined";
  };
  var getElement = (object) => {
    if (isElement(object)) {
      return object.jquery ? object[0] : object;
    }
    if (typeof object === "string" && object.length > 0) {
      return document.querySelector(parseSelector(object));
    }
    return null;
  };
  var isVisible = (element) => {
    if (!isElement(element) || element.getClientRects().length === 0) {
      return false;
    }
    const elementIsVisible = getComputedStyle(element).getPropertyValue("visibility") === "visible";
    const closedDetails = element.closest("details:not([open])");
    if (!closedDetails) {
      return elementIsVisible;
    }
    if (closedDetails !== element) {
      const summary = element.closest("summary");
      if (summary && summary.parentNode !== closedDetails) {
        return false;
      }
      if (summary === null) {
        return false;
      }
    }
    return elementIsVisible;
  };
  var isDisabled = (element) => {
    if (!element || element.nodeType !== Node.ELEMENT_NODE) {
      return true;
    }
    if (element.classList.contains("disabled")) {
      return true;
    }
    if (typeof element.disabled !== "undefined") {
      return element.disabled;
    }
    return element.hasAttribute("disabled") && element.getAttribute("disabled") !== "false";
  };
  var getjQuery = () => {
    if (window.jQuery && !document.body.hasAttribute("data-bs-no-jquery")) {
      return window.jQuery;
    }
    return null;
  };
  var DOMContentLoadedCallbacks = [];
  var onDOMContentLoaded = (callback) => {
    if (document.readyState === "loading") {
      if (!DOMContentLoadedCallbacks.length) {
        document.addEventListener("DOMContentLoaded", () => {
          for (const callback2 of DOMContentLoadedCallbacks) {
            callback2();
          }
        });
      }
      DOMContentLoadedCallbacks.push(callback);
    } else {
      callback();
    }
  };
  var defineJQueryPlugin = (plugin) => {
    onDOMContentLoaded(() => {
      const $ = getjQuery();
      if ($) {
        const name = plugin.NAME;
        const JQUERY_NO_CONFLICT = $.fn[name];
        $.fn[name] = plugin.jQueryInterface;
        $.fn[name].Constructor = plugin;
        $.fn[name].noConflict = () => {
          $.fn[name] = JQUERY_NO_CONFLICT;
          return plugin.jQueryInterface;
        };
      }
    });
  };
  var execute = (possibleCallback, args = [], defaultValue = possibleCallback) => {
    return typeof possibleCallback === "function" ? possibleCallback(...args) : defaultValue;
  };
  var executeAfterTransition = (callback, transitionElement, waitForTransition = true) => {
    if (!waitForTransition) {
      execute(callback);
      return;
    }
    const durationPadding = 5;
    const emulatedDuration = getTransitionDurationFromElement(transitionElement) + durationPadding;
    let called = false;
    const handler = ({ target }) => {
      if (target !== transitionElement) {
        return;
      }
      called = true;
      transitionElement.removeEventListener(TRANSITION_END, handler);
      execute(callback);
    };
    transitionElement.addEventListener(TRANSITION_END, handler);
    setTimeout(() => {
      if (!called) {
        triggerTransitionEnd(transitionElement);
      }
    }, emulatedDuration);
  };
  var getNextActiveElement = (list, activeElement, shouldGetNext, isCycleAllowed) => {
    const listLength = list.length;
    let index = list.indexOf(activeElement);
    if (index === -1) {
      return !shouldGetNext && isCycleAllowed ? list[listLength - 1] : list[0];
    }
    index += shouldGetNext ? 1 : -1;
    if (isCycleAllowed) {
      index = (index + listLength) % listLength;
    }
    return list[Math.max(0, Math.min(index, listLength - 1))];
  };

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/dom/event-handler.js
  var namespaceRegex = /[^.]*(?=\..*)\.|.*/;
  var stripNameRegex = /\..*/;
  var stripUidRegex = /::\d+$/;
  var eventRegistry = {};
  var uidEvent = 1;
  var customEvents = {
    mouseenter: "mouseover",
    mouseleave: "mouseout"
  };
  var nativeEvents = /* @__PURE__ */ new Set([
    "click",
    "dblclick",
    "mouseup",
    "mousedown",
    "contextmenu",
    "mousewheel",
    "DOMMouseScroll",
    "mouseover",
    "mouseout",
    "mousemove",
    "selectstart",
    "selectend",
    "keydown",
    "keypress",
    "keyup",
    "orientationchange",
    "touchstart",
    "touchmove",
    "touchend",
    "touchcancel",
    "pointerdown",
    "pointermove",
    "pointerup",
    "pointerleave",
    "pointercancel",
    "gesturestart",
    "gesturechange",
    "gestureend",
    "focus",
    "blur",
    "change",
    "reset",
    "select",
    "submit",
    "focusin",
    "focusout",
    "load",
    "unload",
    "beforeunload",
    "resize",
    "move",
    "DOMContentLoaded",
    "readystatechange",
    "error",
    "abort",
    "scroll"
  ]);
  function makeEventUid(element, uid) {
    return uid && `${uid}::${uidEvent++}` || element.uidEvent || uidEvent++;
  }
  function getElementEvents(element) {
    const uid = makeEventUid(element);
    element.uidEvent = uid;
    eventRegistry[uid] = eventRegistry[uid] || {};
    return eventRegistry[uid];
  }
  function bootstrapHandler(element, fn) {
    return function handler(event) {
      hydrateObj(event, { delegateTarget: element });
      if (handler.oneOff) {
        EventHandler.off(element, event.type, fn);
      }
      return fn.apply(element, [event]);
    };
  }
  function bootstrapDelegationHandler(element, selector, fn) {
    return function handler(event) {
      const domElements = element.querySelectorAll(selector);
      for (let { target } = event; target && target !== this; target = target.parentNode) {
        for (const domElement of domElements) {
          if (domElement !== target) {
            continue;
          }
          hydrateObj(event, { delegateTarget: target });
          if (handler.oneOff) {
            EventHandler.off(element, event.type, selector, fn);
          }
          return fn.apply(target, [event]);
        }
      }
    };
  }
  function findHandler(events, callable, delegationSelector = null) {
    return Object.values(events).find((event) => event.callable === callable && event.delegationSelector === delegationSelector);
  }
  function normalizeParameters(originalTypeEvent, handler, delegationFunction) {
    const isDelegated = typeof handler === "string";
    const callable = isDelegated ? delegationFunction : handler || delegationFunction;
    let typeEvent = getTypeEvent(originalTypeEvent);
    if (!nativeEvents.has(typeEvent)) {
      typeEvent = originalTypeEvent;
    }
    return [isDelegated, callable, typeEvent];
  }
  function addHandler(element, originalTypeEvent, handler, delegationFunction, oneOff) {
    if (typeof originalTypeEvent !== "string" || !element) {
      return;
    }
    let [isDelegated, callable, typeEvent] = normalizeParameters(originalTypeEvent, handler, delegationFunction);
    if (originalTypeEvent in customEvents) {
      const wrapFunction = (fn2) => {
        return function(event) {
          if (!event.relatedTarget || event.relatedTarget !== event.delegateTarget && !event.delegateTarget.contains(event.relatedTarget)) {
            return fn2.call(this, event);
          }
        };
      };
      callable = wrapFunction(callable);
    }
    const events = getElementEvents(element);
    const handlers = events[typeEvent] || (events[typeEvent] = {});
    const previousFunction = findHandler(handlers, callable, isDelegated ? handler : null);
    if (previousFunction) {
      previousFunction.oneOff = previousFunction.oneOff && oneOff;
      return;
    }
    const uid = makeEventUid(callable, originalTypeEvent.replace(namespaceRegex, ""));
    const fn = isDelegated ? bootstrapDelegationHandler(element, handler, callable) : bootstrapHandler(element, callable);
    fn.delegationSelector = isDelegated ? handler : null;
    fn.callable = callable;
    fn.oneOff = oneOff;
    fn.uidEvent = uid;
    handlers[uid] = fn;
    element.addEventListener(typeEvent, fn, isDelegated);
  }
  function removeHandler(element, events, typeEvent, handler, delegationSelector) {
    const fn = findHandler(events[typeEvent], handler, delegationSelector);
    if (!fn) {
      return;
    }
    element.removeEventListener(typeEvent, fn, Boolean(delegationSelector));
    delete events[typeEvent][fn.uidEvent];
  }
  function removeNamespacedHandlers(element, events, typeEvent, namespace) {
    const storeElementEvent = events[typeEvent] || {};
    for (const [handlerKey, event] of Object.entries(storeElementEvent)) {
      if (handlerKey.includes(namespace)) {
        removeHandler(element, events, typeEvent, event.callable, event.delegationSelector);
      }
    }
  }
  function getTypeEvent(event) {
    event = event.replace(stripNameRegex, "");
    return customEvents[event] || event;
  }
  var EventHandler = {
    on(element, event, handler, delegationFunction) {
      addHandler(element, event, handler, delegationFunction, false);
    },
    one(element, event, handler, delegationFunction) {
      addHandler(element, event, handler, delegationFunction, true);
    },
    off(element, originalTypeEvent, handler, delegationFunction) {
      if (typeof originalTypeEvent !== "string" || !element) {
        return;
      }
      const [isDelegated, callable, typeEvent] = normalizeParameters(originalTypeEvent, handler, delegationFunction);
      const inNamespace = typeEvent !== originalTypeEvent;
      const events = getElementEvents(element);
      const storeElementEvent = events[typeEvent] || {};
      const isNamespace = originalTypeEvent.startsWith(".");
      if (typeof callable !== "undefined") {
        if (!Object.keys(storeElementEvent).length) {
          return;
        }
        removeHandler(element, events, typeEvent, callable, isDelegated ? handler : null);
        return;
      }
      if (isNamespace) {
        for (const elementEvent of Object.keys(events)) {
          removeNamespacedHandlers(element, events, elementEvent, originalTypeEvent.slice(1));
        }
      }
      for (const [keyHandlers, event] of Object.entries(storeElementEvent)) {
        const handlerKey = keyHandlers.replace(stripUidRegex, "");
        if (!inNamespace || originalTypeEvent.includes(handlerKey)) {
          removeHandler(element, events, typeEvent, event.callable, event.delegationSelector);
        }
      }
    },
    trigger(element, event, args) {
      if (typeof event !== "string" || !element) {
        return null;
      }
      const $ = getjQuery();
      const typeEvent = getTypeEvent(event);
      const inNamespace = event !== typeEvent;
      let jQueryEvent = null;
      let bubbles = true;
      let nativeDispatch = true;
      let defaultPrevented = false;
      if (inNamespace && $) {
        jQueryEvent = $.Event(event, args);
        $(element).trigger(jQueryEvent);
        bubbles = !jQueryEvent.isPropagationStopped();
        nativeDispatch = !jQueryEvent.isImmediatePropagationStopped();
        defaultPrevented = jQueryEvent.isDefaultPrevented();
      }
      const evt = hydrateObj(new Event(event, { bubbles, cancelable: true }), args);
      if (defaultPrevented) {
        evt.preventDefault();
      }
      if (nativeDispatch) {
        element.dispatchEvent(evt);
      }
      if (evt.defaultPrevented && jQueryEvent) {
        jQueryEvent.preventDefault();
      }
      return evt;
    }
  };
  function hydrateObj(obj, meta = {}) {
    for (const [key, value] of Object.entries(meta)) {
      try {
        obj[key] = value;
      } catch (e) {
        Object.defineProperty(obj, key, {
          configurable: true,
          get() {
            return value;
          }
        });
      }
    }
    return obj;
  }
  var event_handler_default = EventHandler;

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/dom/manipulator.js
  function normalizeData(value) {
    if (value === "true") {
      return true;
    }
    if (value === "false") {
      return false;
    }
    if (value === Number(value).toString()) {
      return Number(value);
    }
    if (value === "" || value === "null") {
      return null;
    }
    if (typeof value !== "string") {
      return value;
    }
    try {
      return JSON.parse(decodeURIComponent(value));
    } catch (e) {
      return value;
    }
  }
  function normalizeDataKey(key) {
    return key.replace(/[A-Z]/g, (chr) => `-${chr.toLowerCase()}`);
  }
  var Manipulator = {
    setDataAttribute(element, key, value) {
      element.setAttribute(`data-bs-${normalizeDataKey(key)}`, value);
    },
    removeDataAttribute(element, key) {
      element.removeAttribute(`data-bs-${normalizeDataKey(key)}`);
    },
    getDataAttributes(element) {
      if (!element) {
        return {};
      }
      const attributes = {};
      const bsKeys = Object.keys(element.dataset).filter((key) => key.startsWith("bs") && !key.startsWith("bsConfig"));
      for (const key of bsKeys) {
        let pureKey = key.replace(/^bs/, "");
        pureKey = pureKey.charAt(0).toLowerCase() + pureKey.slice(1, pureKey.length);
        attributes[pureKey] = normalizeData(element.dataset[key]);
      }
      return attributes;
    },
    getDataAttribute(element, key) {
      return normalizeData(element.getAttribute(`data-bs-${normalizeDataKey(key)}`));
    }
  };
  var manipulator_default = Manipulator;

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/util/config.js
  var Config = class {
    // Getters
    static get Default() {
      return {};
    }
    static get DefaultType() {
      return {};
    }
    static get NAME() {
      throw new Error('You have to implement the static method "NAME", for each component!');
    }
    _getConfig(config) {
      config = this._mergeConfigObj(config);
      config = this._configAfterMerge(config);
      this._typeCheckConfig(config);
      return config;
    }
    _configAfterMerge(config) {
      return config;
    }
    _mergeConfigObj(config, element) {
      const jsonConfig = isElement(element) ? manipulator_default.getDataAttribute(element, "config") : {};
      return {
        ...this.constructor.Default,
        ...typeof jsonConfig === "object" ? jsonConfig : {},
        ...isElement(element) ? manipulator_default.getDataAttributes(element) : {},
        ...typeof config === "object" ? config : {}
      };
    }
    _typeCheckConfig(config, configTypes = this.constructor.DefaultType) {
      for (const [property, expectedTypes] of Object.entries(configTypes)) {
        const value = config[property];
        const valueType = isElement(value) ? "element" : toType(value);
        if (!new RegExp(expectedTypes).test(valueType)) {
          throw new TypeError(
            `${this.constructor.NAME.toUpperCase()}: Option "${property}" provided type "${valueType}" but expected type "${expectedTypes}".`
          );
        }
      }
    }
  };
  var config_default = Config;

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/base-component.js
  var VERSION = "5.3.2";
  var BaseComponent = class extends config_default {
    constructor(element, config) {
      super();
      element = getElement(element);
      if (!element) {
        return;
      }
      this._element = element;
      this._config = this._getConfig(config);
      data_default.set(this._element, this.constructor.DATA_KEY, this);
    }
    // Public
    dispose() {
      data_default.remove(this._element, this.constructor.DATA_KEY);
      event_handler_default.off(this._element, this.constructor.EVENT_KEY);
      for (const propertyName of Object.getOwnPropertyNames(this)) {
        this[propertyName] = null;
      }
    }
    _queueCallback(callback, element, isAnimated = true) {
      executeAfterTransition(callback, element, isAnimated);
    }
    _getConfig(config) {
      config = this._mergeConfigObj(config, this._element);
      config = this._configAfterMerge(config);
      this._typeCheckConfig(config);
      return config;
    }
    // Static
    static getInstance(element) {
      return data_default.get(getElement(element), this.DATA_KEY);
    }
    static getOrCreateInstance(element, config = {}) {
      return this.getInstance(element) || new this(element, typeof config === "object" ? config : null);
    }
    static get VERSION() {
      return VERSION;
    }
    static get DATA_KEY() {
      return `bs.${this.NAME}`;
    }
    static get EVENT_KEY() {
      return `.${this.DATA_KEY}`;
    }
    static eventName(name) {
      return `${name}${this.EVENT_KEY}`;
    }
  };
  var base_component_default = BaseComponent;

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/dom/selector-engine.js
  var getSelector = (element) => {
    let selector = element.getAttribute("data-bs-target");
    if (!selector || selector === "#") {
      let hrefAttribute = element.getAttribute("href");
      if (!hrefAttribute || !hrefAttribute.includes("#") && !hrefAttribute.startsWith(".")) {
        return null;
      }
      if (hrefAttribute.includes("#") && !hrefAttribute.startsWith("#")) {
        hrefAttribute = `#${hrefAttribute.split("#")[1]}`;
      }
      selector = hrefAttribute && hrefAttribute !== "#" ? parseSelector(hrefAttribute.trim()) : null;
    }
    return selector;
  };
  var SelectorEngine = {
    find(selector, element = document.documentElement) {
      return [].concat(...Element.prototype.querySelectorAll.call(element, selector));
    },
    findOne(selector, element = document.documentElement) {
      return Element.prototype.querySelector.call(element, selector);
    },
    children(element, selector) {
      return [].concat(...element.children).filter((child) => child.matches(selector));
    },
    parents(element, selector) {
      const parents = [];
      let ancestor = element.parentNode.closest(selector);
      while (ancestor) {
        parents.push(ancestor);
        ancestor = ancestor.parentNode.closest(selector);
      }
      return parents;
    },
    prev(element, selector) {
      let previous = element.previousElementSibling;
      while (previous) {
        if (previous.matches(selector)) {
          return [previous];
        }
        previous = previous.previousElementSibling;
      }
      return [];
    },
    // TODO: this is now unused; remove later along with prev()
    next(element, selector) {
      let next = element.nextElementSibling;
      while (next) {
        if (next.matches(selector)) {
          return [next];
        }
        next = next.nextElementSibling;
      }
      return [];
    },
    focusableChildren(element) {
      const focusables = [
        "a",
        "button",
        "input",
        "textarea",
        "select",
        "details",
        "[tabindex]",
        '[contenteditable="true"]'
      ].map((selector) => `${selector}:not([tabindex^="-"])`).join(",");
      return this.find(focusables, element).filter((el) => !isDisabled(el) && isVisible(el));
    },
    getSelectorFromElement(element) {
      const selector = getSelector(element);
      if (selector) {
        return SelectorEngine.findOne(selector) ? selector : null;
      }
      return null;
    },
    getElementFromSelector(element) {
      const selector = getSelector(element);
      return selector ? SelectorEngine.findOne(selector) : null;
    },
    getMultipleElementsFromSelector(element) {
      const selector = getSelector(element);
      return selector ? SelectorEngine.find(selector) : [];
    }
  };
  var selector_engine_default = SelectorEngine;

  // ns-hugo-imp:/home/clovis/.cache/hugo_cache/modules/filecache/modules/pkg/mod/github.com/twbs/bootstrap@v5.3.2+incompatible/js/src/tab.js
  var NAME = "tab";
  var DATA_KEY = "bs.tab";
  var EVENT_KEY = `.${DATA_KEY}`;
  var EVENT_HIDE = `hide${EVENT_KEY}`;
  var EVENT_HIDDEN = `hidden${EVENT_KEY}`;
  var EVENT_SHOW = `show${EVENT_KEY}`;
  var EVENT_SHOWN = `shown${EVENT_KEY}`;
  var EVENT_CLICK_DATA_API = `click${EVENT_KEY}`;
  var EVENT_KEYDOWN = `keydown${EVENT_KEY}`;
  var EVENT_LOAD_DATA_API = `load${EVENT_KEY}`;
  var ARROW_LEFT_KEY = "ArrowLeft";
  var ARROW_RIGHT_KEY = "ArrowRight";
  var ARROW_UP_KEY = "ArrowUp";
  var ARROW_DOWN_KEY = "ArrowDown";
  var HOME_KEY = "Home";
  var END_KEY = "End";
  var CLASS_NAME_ACTIVE = "active";
  var CLASS_NAME_FADE = "fade";
  var CLASS_NAME_SHOW = "show";
  var CLASS_DROPDOWN = "dropdown";
  var SELECTOR_DROPDOWN_TOGGLE = ".dropdown-toggle";
  var SELECTOR_DROPDOWN_MENU = ".dropdown-menu";
  var NOT_SELECTOR_DROPDOWN_TOGGLE = `:not(${SELECTOR_DROPDOWN_TOGGLE})`;
  var SELECTOR_TAB_PANEL = '.list-group, .nav, [role="tablist"]';
  var SELECTOR_OUTER = ".nav-item, .list-group-item";
  var SELECTOR_INNER = `.nav-link${NOT_SELECTOR_DROPDOWN_TOGGLE}, .list-group-item${NOT_SELECTOR_DROPDOWN_TOGGLE}, [role="tab"]${NOT_SELECTOR_DROPDOWN_TOGGLE}`;
  var SELECTOR_DATA_TOGGLE = '[data-bs-toggle="tab"], [data-bs-toggle="pill"], [data-bs-toggle="list"]';
  var SELECTOR_INNER_ELEM = `${SELECTOR_INNER}, ${SELECTOR_DATA_TOGGLE}`;
  var SELECTOR_DATA_TOGGLE_ACTIVE = `.${CLASS_NAME_ACTIVE}[data-bs-toggle="tab"], .${CLASS_NAME_ACTIVE}[data-bs-toggle="pill"], .${CLASS_NAME_ACTIVE}[data-bs-toggle="list"]`;
  var Tab = class _Tab extends base_component_default {
    constructor(element) {
      super(element);
      this._parent = this._element.closest(SELECTOR_TAB_PANEL);
      if (!this._parent) {
        return;
      }
      this._setInitialAttributes(this._parent, this._getChildren());
      event_handler_default.on(this._element, EVENT_KEYDOWN, (event) => this._keydown(event));
    }
    // Getters
    static get NAME() {
      return NAME;
    }
    // Public
    show() {
      const innerElem = this._element;
      if (this._elemIsActive(innerElem)) {
        return;
      }
      const active = this._getActiveElem();
      const hideEvent = active ? event_handler_default.trigger(active, EVENT_HIDE, { relatedTarget: innerElem }) : null;
      const showEvent = event_handler_default.trigger(innerElem, EVENT_SHOW, { relatedTarget: active });
      if (showEvent.defaultPrevented || hideEvent && hideEvent.defaultPrevented) {
        return;
      }
      this._deactivate(active, innerElem);
      this._activate(innerElem, active);
    }
    // Private
    _activate(element, relatedElem) {
      if (!element) {
        return;
      }
      element.classList.add(CLASS_NAME_ACTIVE);
      this._activate(selector_engine_default.getElementFromSelector(element));
      const complete = () => {
        if (element.getAttribute("role") !== "tab") {
          element.classList.add(CLASS_NAME_SHOW);
          return;
        }
        element.removeAttribute("tabindex");
        element.setAttribute("aria-selected", true);
        this._toggleDropDown(element, true);
        event_handler_default.trigger(element, EVENT_SHOWN, {
          relatedTarget: relatedElem
        });
      };
      this._queueCallback(complete, element, element.classList.contains(CLASS_NAME_FADE));
    }
    _deactivate(element, relatedElem) {
      if (!element) {
        return;
      }
      element.classList.remove(CLASS_NAME_ACTIVE);
      element.blur();
      this._deactivate(selector_engine_default.getElementFromSelector(element));
      const complete = () => {
        if (element.getAttribute("role") !== "tab") {
          element.classList.remove(CLASS_NAME_SHOW);
          return;
        }
        element.setAttribute("aria-selected", false);
        element.setAttribute("tabindex", "-1");
        this._toggleDropDown(element, false);
        event_handler_default.trigger(element, EVENT_HIDDEN, { relatedTarget: relatedElem });
      };
      this._queueCallback(complete, element, element.classList.contains(CLASS_NAME_FADE));
    }
    _keydown(event) {
      if (![ARROW_LEFT_KEY, ARROW_RIGHT_KEY, ARROW_UP_KEY, ARROW_DOWN_KEY, HOME_KEY, END_KEY].includes(event.key)) {
        return;
      }
      event.stopPropagation();
      event.preventDefault();
      const children = this._getChildren().filter((element) => !isDisabled(element));
      let nextActiveElement;
      if ([HOME_KEY, END_KEY].includes(event.key)) {
        nextActiveElement = children[event.key === HOME_KEY ? 0 : children.length - 1];
      } else {
        const isNext = [ARROW_RIGHT_KEY, ARROW_DOWN_KEY].includes(event.key);
        nextActiveElement = getNextActiveElement(children, event.target, isNext, true);
      }
      if (nextActiveElement) {
        nextActiveElement.focus({ preventScroll: true });
        _Tab.getOrCreateInstance(nextActiveElement).show();
      }
    }
    _getChildren() {
      return selector_engine_default.find(SELECTOR_INNER_ELEM, this._parent);
    }
    _getActiveElem() {
      return this._getChildren().find((child) => this._elemIsActive(child)) || null;
    }
    _setInitialAttributes(parent, children) {
      this._setAttributeIfNotExists(parent, "role", "tablist");
      for (const child of children) {
        this._setInitialAttributesOnChild(child);
      }
    }
    _setInitialAttributesOnChild(child) {
      child = this._getInnerElement(child);
      const isActive = this._elemIsActive(child);
      const outerElem = this._getOuterElement(child);
      child.setAttribute("aria-selected", isActive);
      if (outerElem !== child) {
        this._setAttributeIfNotExists(outerElem, "role", "presentation");
      }
      if (!isActive) {
        child.setAttribute("tabindex", "-1");
      }
      this._setAttributeIfNotExists(child, "role", "tab");
      this._setInitialAttributesOnTargetPanel(child);
    }
    _setInitialAttributesOnTargetPanel(child) {
      const target = selector_engine_default.getElementFromSelector(child);
      if (!target) {
        return;
      }
      this._setAttributeIfNotExists(target, "role", "tabpanel");
      if (child.id) {
        this._setAttributeIfNotExists(target, "aria-labelledby", `${child.id}`);
      }
    }
    _toggleDropDown(element, open) {
      const outerElem = this._getOuterElement(element);
      if (!outerElem.classList.contains(CLASS_DROPDOWN)) {
        return;
      }
      const toggle = (selector, className) => {
        const element2 = selector_engine_default.findOne(selector, outerElem);
        if (element2) {
          element2.classList.toggle(className, open);
        }
      };
      toggle(SELECTOR_DROPDOWN_TOGGLE, CLASS_NAME_ACTIVE);
      toggle(SELECTOR_DROPDOWN_MENU, CLASS_NAME_SHOW);
      outerElem.setAttribute("aria-expanded", open);
    }
    _setAttributeIfNotExists(element, attribute, value) {
      if (!element.hasAttribute(attribute)) {
        element.setAttribute(attribute, value);
      }
    }
    _elemIsActive(elem) {
      return elem.classList.contains(CLASS_NAME_ACTIVE);
    }
    // Try to get the inner element (usually the .nav-link)
    _getInnerElement(elem) {
      return elem.matches(SELECTOR_INNER_ELEM) ? elem : selector_engine_default.findOne(SELECTOR_INNER_ELEM, elem);
    }
    // Try to get the outer element (usually the .nav-item)
    _getOuterElement(elem) {
      return elem.closest(SELECTOR_OUTER) || elem;
    }
    // Static
    static jQueryInterface(config) {
      return this.each(function() {
        const data = _Tab.getOrCreateInstance(this);
        if (typeof config !== "string") {
          return;
        }
        if (data[config] === void 0 || config.startsWith("_") || config === "constructor") {
          throw new TypeError(`No method named "${config}"`);
        }
        data[config]();
      });
    }
  };
  event_handler_default.on(document, EVENT_CLICK_DATA_API, SELECTOR_DATA_TOGGLE, function(event) {
    if (["A", "AREA"].includes(this.tagName)) {
      event.preventDefault();
    }
    if (isDisabled(this)) {
      return;
    }
    Tab.getOrCreateInstance(this).show();
  });
  event_handler_default.on(window, EVENT_LOAD_DATA_API, () => {
    for (const element of selector_engine_default.find(SELECTOR_DATA_TOGGLE_ACTIVE)) {
      Tab.getOrCreateInstance(element);
    }
  });
  defineJQueryPlugin(Tab);
  var tab_default = Tab;

  // <stdin>
  var stdin_default = {
    Tab: tab_default
  };
})();
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsibnMtaHVnby1pbXA6L2hvbWUvY2xvdmlzLy5jYWNoZS9odWdvX2NhY2hlL21vZHVsZXMvZmlsZWNhY2hlL21vZHVsZXMvcGtnL21vZC9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwQHY1LjMuMitpbmNvbXBhdGlibGUvanMvc3JjL2RvbS9kYXRhLmpzIiwgIm5zLWh1Z28taW1wOi9ob21lL2Nsb3Zpcy8uY2FjaGUvaHVnb19jYWNoZS9tb2R1bGVzL2ZpbGVjYWNoZS9tb2R1bGVzL3BrZy9tb2QvZ2l0aHViLmNvbS90d2JzL2Jvb3RzdHJhcEB2NS4zLjIraW5jb21wYXRpYmxlL2pzL3NyYy91dGlsL2luZGV4LmpzIiwgIm5zLWh1Z28taW1wOi9ob21lL2Nsb3Zpcy8uY2FjaGUvaHVnb19jYWNoZS9tb2R1bGVzL2ZpbGVjYWNoZS9tb2R1bGVzL3BrZy9tb2QvZ2l0aHViLmNvbS90d2JzL2Jvb3RzdHJhcEB2NS4zLjIraW5jb21wYXRpYmxlL2pzL3NyYy9kb20vZXZlbnQtaGFuZGxlci5qcyIsICJucy1odWdvLWltcDovaG9tZS9jbG92aXMvLmNhY2hlL2h1Z29fY2FjaGUvbW9kdWxlcy9maWxlY2FjaGUvbW9kdWxlcy9wa2cvbW9kL2dpdGh1Yi5jb20vdHdicy9ib290c3RyYXBAdjUuMy4yK2luY29tcGF0aWJsZS9qcy9zcmMvZG9tL21hbmlwdWxhdG9yLmpzIiwgIm5zLWh1Z28taW1wOi9ob21lL2Nsb3Zpcy8uY2FjaGUvaHVnb19jYWNoZS9tb2R1bGVzL2ZpbGVjYWNoZS9tb2R1bGVzL3BrZy9tb2QvZ2l0aHViLmNvbS90d2JzL2Jvb3RzdHJhcEB2NS4zLjIraW5jb21wYXRpYmxlL2pzL3NyYy91dGlsL2NvbmZpZy5qcyIsICJucy1odWdvLWltcDovaG9tZS9jbG92aXMvLmNhY2hlL2h1Z29fY2FjaGUvbW9kdWxlcy9maWxlY2FjaGUvbW9kdWxlcy9wa2cvbW9kL2dpdGh1Yi5jb20vdHdicy9ib290c3RyYXBAdjUuMy4yK2luY29tcGF0aWJsZS9qcy9zcmMvYmFzZS1jb21wb25lbnQuanMiLCAibnMtaHVnby1pbXA6L2hvbWUvY2xvdmlzLy5jYWNoZS9odWdvX2NhY2hlL21vZHVsZXMvZmlsZWNhY2hlL21vZHVsZXMvcGtnL21vZC9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwQHY1LjMuMitpbmNvbXBhdGlibGUvanMvc3JjL2RvbS9zZWxlY3Rvci1lbmdpbmUuanMiLCAibnMtaHVnby1pbXA6L2hvbWUvY2xvdmlzLy5jYWNoZS9odWdvX2NhY2hlL21vZHVsZXMvZmlsZWNhY2hlL21vZHVsZXMvcGtnL21vZC9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwQHY1LjMuMitpbmNvbXBhdGlibGUvanMvc3JjL3RhYi5qcyIsICI8c3RkaW4+Il0sCiAgInNvdXJjZXNDb250ZW50IjogWyIvKipcbiAqIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG4gKiBCb290c3RyYXAgZG9tL2RhdGEuanNcbiAqIExpY2Vuc2VkIHVuZGVyIE1JVCAoaHR0cHM6Ly9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwL2Jsb2IvbWFpbi9MSUNFTlNFKVxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqL1xuXG4vKipcbiAqIENvbnN0YW50c1xuICovXG5cbmNvbnN0IGVsZW1lbnRNYXAgPSBuZXcgTWFwKClcblxuZXhwb3J0IGRlZmF1bHQge1xuICBzZXQoZWxlbWVudCwga2V5LCBpbnN0YW5jZSkge1xuICAgIGlmICghZWxlbWVudE1hcC5oYXMoZWxlbWVudCkpIHtcbiAgICAgIGVsZW1lbnRNYXAuc2V0KGVsZW1lbnQsIG5ldyBNYXAoKSlcbiAgICB9XG5cbiAgICBjb25zdCBpbnN0YW5jZU1hcCA9IGVsZW1lbnRNYXAuZ2V0KGVsZW1lbnQpXG5cbiAgICAvLyBtYWtlIGl0IGNsZWFyIHdlIG9ubHkgd2FudCBvbmUgaW5zdGFuY2UgcGVyIGVsZW1lbnRcbiAgICAvLyBjYW4gYmUgcmVtb3ZlZCBsYXRlciB3aGVuIG11bHRpcGxlIGtleS9pbnN0YW5jZXMgYXJlIGZpbmUgdG8gYmUgdXNlZFxuICAgIGlmICghaW5zdGFuY2VNYXAuaGFzKGtleSkgJiYgaW5zdGFuY2VNYXAuc2l6ZSAhPT0gMCkge1xuICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIG5vLWNvbnNvbGVcbiAgICAgIGNvbnNvbGUuZXJyb3IoYEJvb3RzdHJhcCBkb2Vzbid0IGFsbG93IG1vcmUgdGhhbiBvbmUgaW5zdGFuY2UgcGVyIGVsZW1lbnQuIEJvdW5kIGluc3RhbmNlOiAke0FycmF5LmZyb20oaW5zdGFuY2VNYXAua2V5cygpKVswXX0uYClcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIGluc3RhbmNlTWFwLnNldChrZXksIGluc3RhbmNlKVxuICB9LFxuXG4gIGdldChlbGVtZW50LCBrZXkpIHtcbiAgICBpZiAoZWxlbWVudE1hcC5oYXMoZWxlbWVudCkpIHtcbiAgICAgIHJldHVybiBlbGVtZW50TWFwLmdldChlbGVtZW50KS5nZXQoa2V5KSB8fCBudWxsXG4gICAgfVxuXG4gICAgcmV0dXJuIG51bGxcbiAgfSxcblxuICByZW1vdmUoZWxlbWVudCwga2V5KSB7XG4gICAgaWYgKCFlbGVtZW50TWFwLmhhcyhlbGVtZW50KSkge1xuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgY29uc3QgaW5zdGFuY2VNYXAgPSBlbGVtZW50TWFwLmdldChlbGVtZW50KVxuXG4gICAgaW5zdGFuY2VNYXAuZGVsZXRlKGtleSlcblxuICAgIC8vIGZyZWUgdXAgZWxlbWVudCByZWZlcmVuY2VzIGlmIHRoZXJlIGFyZSBubyBpbnN0YW5jZXMgbGVmdCBmb3IgYW4gZWxlbWVudFxuICAgIGlmIChpbnN0YW5jZU1hcC5zaXplID09PSAwKSB7XG4gICAgICBlbGVtZW50TWFwLmRlbGV0ZShlbGVtZW50KVxuICAgIH1cbiAgfVxufVxuIiwgIi8qKlxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqIEJvb3RzdHJhcCB1dGlsL2luZGV4LmpzXG4gKiBMaWNlbnNlZCB1bmRlciBNSVQgKGh0dHBzOi8vZ2l0aHViLmNvbS90d2JzL2Jvb3RzdHJhcC9ibG9iL21haW4vTElDRU5TRSlcbiAqIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG4gKi9cblxuY29uc3QgTUFYX1VJRCA9IDFfMDAwXzAwMFxuY29uc3QgTUlMTElTRUNPTkRTX01VTFRJUExJRVIgPSAxMDAwXG5jb25zdCBUUkFOU0lUSU9OX0VORCA9ICd0cmFuc2l0aW9uZW5kJ1xuXG4vKipcbiAqIFByb3Blcmx5IGVzY2FwZSBJRHMgc2VsZWN0b3JzIHRvIGhhbmRsZSB3ZWlyZCBJRHNcbiAqIEBwYXJhbSB7c3RyaW5nfSBzZWxlY3RvclxuICogQHJldHVybnMge3N0cmluZ31cbiAqL1xuY29uc3QgcGFyc2VTZWxlY3RvciA9IHNlbGVjdG9yID0+IHtcbiAgaWYgKHNlbGVjdG9yICYmIHdpbmRvdy5DU1MgJiYgd2luZG93LkNTUy5lc2NhcGUpIHtcbiAgICAvLyBkb2N1bWVudC5xdWVyeVNlbGVjdG9yIG5lZWRzIGVzY2FwaW5nIHRvIGhhbmRsZSBJRHMgKGh0bWw1KykgY29udGFpbmluZyBmb3IgaW5zdGFuY2UgL1xuICAgIHNlbGVjdG9yID0gc2VsZWN0b3IucmVwbGFjZSgvIyhbXlxcc1wiIyddKykvZywgKG1hdGNoLCBpZCkgPT4gYCMke0NTUy5lc2NhcGUoaWQpfWApXG4gIH1cblxuICByZXR1cm4gc2VsZWN0b3Jcbn1cblxuLy8gU2hvdXQtb3V0IEFuZ3VzIENyb2xsIChodHRwczovL2dvby5nbC9weHdRR3ApXG5jb25zdCB0b1R5cGUgPSBvYmplY3QgPT4ge1xuICBpZiAob2JqZWN0ID09PSBudWxsIHx8IG9iamVjdCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgcmV0dXJuIGAke29iamVjdH1gXG4gIH1cblxuICByZXR1cm4gT2JqZWN0LnByb3RvdHlwZS50b1N0cmluZy5jYWxsKG9iamVjdCkubWF0Y2goL1xccyhbYS16XSspL2kpWzFdLnRvTG93ZXJDYXNlKClcbn1cblxuLyoqXG4gKiBQdWJsaWMgVXRpbCBBUElcbiAqL1xuXG5jb25zdCBnZXRVSUQgPSBwcmVmaXggPT4ge1xuICBkbyB7XG4gICAgcHJlZml4ICs9IE1hdGguZmxvb3IoTWF0aC5yYW5kb20oKSAqIE1BWF9VSUQpXG4gIH0gd2hpbGUgKGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKHByZWZpeCkpXG5cbiAgcmV0dXJuIHByZWZpeFxufVxuXG5jb25zdCBnZXRUcmFuc2l0aW9uRHVyYXRpb25Gcm9tRWxlbWVudCA9IGVsZW1lbnQgPT4ge1xuICBpZiAoIWVsZW1lbnQpIHtcbiAgICByZXR1cm4gMFxuICB9XG5cbiAgLy8gR2V0IHRyYW5zaXRpb24tZHVyYXRpb24gb2YgdGhlIGVsZW1lbnRcbiAgbGV0IHsgdHJhbnNpdGlvbkR1cmF0aW9uLCB0cmFuc2l0aW9uRGVsYXkgfSA9IHdpbmRvdy5nZXRDb21wdXRlZFN0eWxlKGVsZW1lbnQpXG5cbiAgY29uc3QgZmxvYXRUcmFuc2l0aW9uRHVyYXRpb24gPSBOdW1iZXIucGFyc2VGbG9hdCh0cmFuc2l0aW9uRHVyYXRpb24pXG4gIGNvbnN0IGZsb2F0VHJhbnNpdGlvbkRlbGF5ID0gTnVtYmVyLnBhcnNlRmxvYXQodHJhbnNpdGlvbkRlbGF5KVxuXG4gIC8vIFJldHVybiAwIGlmIGVsZW1lbnQgb3IgdHJhbnNpdGlvbiBkdXJhdGlvbiBpcyBub3QgZm91bmRcbiAgaWYgKCFmbG9hdFRyYW5zaXRpb25EdXJhdGlvbiAmJiAhZmxvYXRUcmFuc2l0aW9uRGVsYXkpIHtcbiAgICByZXR1cm4gMFxuICB9XG5cbiAgLy8gSWYgbXVsdGlwbGUgZHVyYXRpb25zIGFyZSBkZWZpbmVkLCB0YWtlIHRoZSBmaXJzdFxuICB0cmFuc2l0aW9uRHVyYXRpb24gPSB0cmFuc2l0aW9uRHVyYXRpb24uc3BsaXQoJywnKVswXVxuICB0cmFuc2l0aW9uRGVsYXkgPSB0cmFuc2l0aW9uRGVsYXkuc3BsaXQoJywnKVswXVxuXG4gIHJldHVybiAoTnVtYmVyLnBhcnNlRmxvYXQodHJhbnNpdGlvbkR1cmF0aW9uKSArIE51bWJlci5wYXJzZUZsb2F0KHRyYW5zaXRpb25EZWxheSkpICogTUlMTElTRUNPTkRTX01VTFRJUExJRVJcbn1cblxuY29uc3QgdHJpZ2dlclRyYW5zaXRpb25FbmQgPSBlbGVtZW50ID0+IHtcbiAgZWxlbWVudC5kaXNwYXRjaEV2ZW50KG5ldyBFdmVudChUUkFOU0lUSU9OX0VORCkpXG59XG5cbmNvbnN0IGlzRWxlbWVudCA9IG9iamVjdCA9PiB7XG4gIGlmICghb2JqZWN0IHx8IHR5cGVvZiBvYmplY3QgIT09ICdvYmplY3QnKSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cblxuICBpZiAodHlwZW9mIG9iamVjdC5qcXVlcnkgIT09ICd1bmRlZmluZWQnKSB7XG4gICAgb2JqZWN0ID0gb2JqZWN0WzBdXG4gIH1cblxuICByZXR1cm4gdHlwZW9mIG9iamVjdC5ub2RlVHlwZSAhPT0gJ3VuZGVmaW5lZCdcbn1cblxuY29uc3QgZ2V0RWxlbWVudCA9IG9iamVjdCA9PiB7XG4gIC8vIGl0J3MgYSBqUXVlcnkgb2JqZWN0IG9yIGEgbm9kZSBlbGVtZW50XG4gIGlmIChpc0VsZW1lbnQob2JqZWN0KSkge1xuICAgIHJldHVybiBvYmplY3QuanF1ZXJ5ID8gb2JqZWN0WzBdIDogb2JqZWN0XG4gIH1cblxuICBpZiAodHlwZW9mIG9iamVjdCA9PT0gJ3N0cmluZycgJiYgb2JqZWN0Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gZG9jdW1lbnQucXVlcnlTZWxlY3RvcihwYXJzZVNlbGVjdG9yKG9iamVjdCkpXG4gIH1cblxuICByZXR1cm4gbnVsbFxufVxuXG5jb25zdCBpc1Zpc2libGUgPSBlbGVtZW50ID0+IHtcbiAgaWYgKCFpc0VsZW1lbnQoZWxlbWVudCkgfHwgZWxlbWVudC5nZXRDbGllbnRSZWN0cygpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBmYWxzZVxuICB9XG5cbiAgY29uc3QgZWxlbWVudElzVmlzaWJsZSA9IGdldENvbXB1dGVkU3R5bGUoZWxlbWVudCkuZ2V0UHJvcGVydHlWYWx1ZSgndmlzaWJpbGl0eScpID09PSAndmlzaWJsZSdcbiAgLy8gSGFuZGxlIGBkZXRhaWxzYCBlbGVtZW50IGFzIGl0cyBjb250ZW50IG1heSBmYWxzaWUgYXBwZWFyIHZpc2libGUgd2hlbiBpdCBpcyBjbG9zZWRcbiAgY29uc3QgY2xvc2VkRGV0YWlscyA9IGVsZW1lbnQuY2xvc2VzdCgnZGV0YWlsczpub3QoW29wZW5dKScpXG5cbiAgaWYgKCFjbG9zZWREZXRhaWxzKSB7XG4gICAgcmV0dXJuIGVsZW1lbnRJc1Zpc2libGVcbiAgfVxuXG4gIGlmIChjbG9zZWREZXRhaWxzICE9PSBlbGVtZW50KSB7XG4gICAgY29uc3Qgc3VtbWFyeSA9IGVsZW1lbnQuY2xvc2VzdCgnc3VtbWFyeScpXG4gICAgaWYgKHN1bW1hcnkgJiYgc3VtbWFyeS5wYXJlbnROb2RlICE9PSBjbG9zZWREZXRhaWxzKSB7XG4gICAgICByZXR1cm4gZmFsc2VcbiAgICB9XG5cbiAgICBpZiAoc3VtbWFyeSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIGZhbHNlXG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIGVsZW1lbnRJc1Zpc2libGVcbn1cblxuY29uc3QgaXNEaXNhYmxlZCA9IGVsZW1lbnQgPT4ge1xuICBpZiAoIWVsZW1lbnQgfHwgZWxlbWVudC5ub2RlVHlwZSAhPT0gTm9kZS5FTEVNRU5UX05PREUpIHtcbiAgICByZXR1cm4gdHJ1ZVxuICB9XG5cbiAgaWYgKGVsZW1lbnQuY2xhc3NMaXN0LmNvbnRhaW5zKCdkaXNhYmxlZCcpKSB7XG4gICAgcmV0dXJuIHRydWVcbiAgfVxuXG4gIGlmICh0eXBlb2YgZWxlbWVudC5kaXNhYmxlZCAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICByZXR1cm4gZWxlbWVudC5kaXNhYmxlZFxuICB9XG5cbiAgcmV0dXJuIGVsZW1lbnQuaGFzQXR0cmlidXRlKCdkaXNhYmxlZCcpICYmIGVsZW1lbnQuZ2V0QXR0cmlidXRlKCdkaXNhYmxlZCcpICE9PSAnZmFsc2UnXG59XG5cbmNvbnN0IGZpbmRTaGFkb3dSb290ID0gZWxlbWVudCA9PiB7XG4gIGlmICghZG9jdW1lbnQuZG9jdW1lbnRFbGVtZW50LmF0dGFjaFNoYWRvdykge1xuICAgIHJldHVybiBudWxsXG4gIH1cblxuICAvLyBDYW4gZmluZCB0aGUgc2hhZG93IHJvb3Qgb3RoZXJ3aXNlIGl0J2xsIHJldHVybiB0aGUgZG9jdW1lbnRcbiAgaWYgKHR5cGVvZiBlbGVtZW50LmdldFJvb3ROb2RlID09PSAnZnVuY3Rpb24nKSB7XG4gICAgY29uc3Qgcm9vdCA9IGVsZW1lbnQuZ2V0Um9vdE5vZGUoKVxuICAgIHJldHVybiByb290IGluc3RhbmNlb2YgU2hhZG93Um9vdCA/IHJvb3QgOiBudWxsXG4gIH1cblxuICBpZiAoZWxlbWVudCBpbnN0YW5jZW9mIFNoYWRvd1Jvb3QpIHtcbiAgICByZXR1cm4gZWxlbWVudFxuICB9XG5cbiAgLy8gd2hlbiB3ZSBkb24ndCBmaW5kIGEgc2hhZG93IHJvb3RcbiAgaWYgKCFlbGVtZW50LnBhcmVudE5vZGUpIHtcbiAgICByZXR1cm4gbnVsbFxuICB9XG5cbiAgcmV0dXJuIGZpbmRTaGFkb3dSb290KGVsZW1lbnQucGFyZW50Tm9kZSlcbn1cblxuY29uc3Qgbm9vcCA9ICgpID0+IHt9XG5cbi8qKlxuICogVHJpY2sgdG8gcmVzdGFydCBhbiBlbGVtZW50J3MgYW5pbWF0aW9uXG4gKlxuICogQHBhcmFtIHtIVE1MRWxlbWVudH0gZWxlbWVudFxuICogQHJldHVybiB2b2lkXG4gKlxuICogQHNlZSBodHRwczovL3d3dy5jaGFyaXN0aGVvLmlvL2Jsb2cvMjAyMS8wMi9yZXN0YXJ0LWEtY3NzLWFuaW1hdGlvbi13aXRoLWphdmFzY3JpcHQvI3Jlc3RhcnRpbmctYS1jc3MtYW5pbWF0aW9uXG4gKi9cbmNvbnN0IHJlZmxvdyA9IGVsZW1lbnQgPT4ge1xuICBlbGVtZW50Lm9mZnNldEhlaWdodCAvLyBlc2xpbnQtZGlzYWJsZS1saW5lIG5vLXVudXNlZC1leHByZXNzaW9uc1xufVxuXG5jb25zdCBnZXRqUXVlcnkgPSAoKSA9PiB7XG4gIGlmICh3aW5kb3cualF1ZXJ5ICYmICFkb2N1bWVudC5ib2R5Lmhhc0F0dHJpYnV0ZSgnZGF0YS1icy1uby1qcXVlcnknKSkge1xuICAgIHJldHVybiB3aW5kb3cualF1ZXJ5XG4gIH1cblxuICByZXR1cm4gbnVsbFxufVxuXG5jb25zdCBET01Db250ZW50TG9hZGVkQ2FsbGJhY2tzID0gW11cblxuY29uc3Qgb25ET01Db250ZW50TG9hZGVkID0gY2FsbGJhY2sgPT4ge1xuICBpZiAoZG9jdW1lbnQucmVhZHlTdGF0ZSA9PT0gJ2xvYWRpbmcnKSB7XG4gICAgLy8gYWRkIGxpc3RlbmVyIG9uIHRoZSBmaXJzdCBjYWxsIHdoZW4gdGhlIGRvY3VtZW50IGlzIGluIGxvYWRpbmcgc3RhdGVcbiAgICBpZiAoIURPTUNvbnRlbnRMb2FkZWRDYWxsYmFja3MubGVuZ3RoKSB7XG4gICAgICBkb2N1bWVudC5hZGRFdmVudExpc3RlbmVyKCdET01Db250ZW50TG9hZGVkJywgKCkgPT4ge1xuICAgICAgICBmb3IgKGNvbnN0IGNhbGxiYWNrIG9mIERPTUNvbnRlbnRMb2FkZWRDYWxsYmFja3MpIHtcbiAgICAgICAgICBjYWxsYmFjaygpXG4gICAgICAgIH1cbiAgICAgIH0pXG4gICAgfVxuXG4gICAgRE9NQ29udGVudExvYWRlZENhbGxiYWNrcy5wdXNoKGNhbGxiYWNrKVxuICB9IGVsc2Uge1xuICAgIGNhbGxiYWNrKClcbiAgfVxufVxuXG5jb25zdCBpc1JUTCA9ICgpID0+IGRvY3VtZW50LmRvY3VtZW50RWxlbWVudC5kaXIgPT09ICdydGwnXG5cbmNvbnN0IGRlZmluZUpRdWVyeVBsdWdpbiA9IHBsdWdpbiA9PiB7XG4gIG9uRE9NQ29udGVudExvYWRlZCgoKSA9PiB7XG4gICAgY29uc3QgJCA9IGdldGpRdWVyeSgpXG4gICAgLyogaXN0YW5idWwgaWdub3JlIGlmICovXG4gICAgaWYgKCQpIHtcbiAgICAgIGNvbnN0IG5hbWUgPSBwbHVnaW4uTkFNRVxuICAgICAgY29uc3QgSlFVRVJZX05PX0NPTkZMSUNUID0gJC5mbltuYW1lXVxuICAgICAgJC5mbltuYW1lXSA9IHBsdWdpbi5qUXVlcnlJbnRlcmZhY2VcbiAgICAgICQuZm5bbmFtZV0uQ29uc3RydWN0b3IgPSBwbHVnaW5cbiAgICAgICQuZm5bbmFtZV0ubm9Db25mbGljdCA9ICgpID0+IHtcbiAgICAgICAgJC5mbltuYW1lXSA9IEpRVUVSWV9OT19DT05GTElDVFxuICAgICAgICByZXR1cm4gcGx1Z2luLmpRdWVyeUludGVyZmFjZVxuICAgICAgfVxuICAgIH1cbiAgfSlcbn1cblxuY29uc3QgZXhlY3V0ZSA9IChwb3NzaWJsZUNhbGxiYWNrLCBhcmdzID0gW10sIGRlZmF1bHRWYWx1ZSA9IHBvc3NpYmxlQ2FsbGJhY2spID0+IHtcbiAgcmV0dXJuIHR5cGVvZiBwb3NzaWJsZUNhbGxiYWNrID09PSAnZnVuY3Rpb24nID8gcG9zc2libGVDYWxsYmFjayguLi5hcmdzKSA6IGRlZmF1bHRWYWx1ZVxufVxuXG5jb25zdCBleGVjdXRlQWZ0ZXJUcmFuc2l0aW9uID0gKGNhbGxiYWNrLCB0cmFuc2l0aW9uRWxlbWVudCwgd2FpdEZvclRyYW5zaXRpb24gPSB0cnVlKSA9PiB7XG4gIGlmICghd2FpdEZvclRyYW5zaXRpb24pIHtcbiAgICBleGVjdXRlKGNhbGxiYWNrKVxuICAgIHJldHVyblxuICB9XG5cbiAgY29uc3QgZHVyYXRpb25QYWRkaW5nID0gNVxuICBjb25zdCBlbXVsYXRlZER1cmF0aW9uID0gZ2V0VHJhbnNpdGlvbkR1cmF0aW9uRnJvbUVsZW1lbnQodHJhbnNpdGlvbkVsZW1lbnQpICsgZHVyYXRpb25QYWRkaW5nXG5cbiAgbGV0IGNhbGxlZCA9IGZhbHNlXG5cbiAgY29uc3QgaGFuZGxlciA9ICh7IHRhcmdldCB9KSA9PiB7XG4gICAgaWYgKHRhcmdldCAhPT0gdHJhbnNpdGlvbkVsZW1lbnQpIHtcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIGNhbGxlZCA9IHRydWVcbiAgICB0cmFuc2l0aW9uRWxlbWVudC5yZW1vdmVFdmVudExpc3RlbmVyKFRSQU5TSVRJT05fRU5ELCBoYW5kbGVyKVxuICAgIGV4ZWN1dGUoY2FsbGJhY2spXG4gIH1cblxuICB0cmFuc2l0aW9uRWxlbWVudC5hZGRFdmVudExpc3RlbmVyKFRSQU5TSVRJT05fRU5ELCBoYW5kbGVyKVxuICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICBpZiAoIWNhbGxlZCkge1xuICAgICAgdHJpZ2dlclRyYW5zaXRpb25FbmQodHJhbnNpdGlvbkVsZW1lbnQpXG4gICAgfVxuICB9LCBlbXVsYXRlZER1cmF0aW9uKVxufVxuXG4vKipcbiAqIFJldHVybiB0aGUgcHJldmlvdXMvbmV4dCBlbGVtZW50IG9mIGEgbGlzdC5cbiAqXG4gKiBAcGFyYW0ge2FycmF5fSBsaXN0ICAgIFRoZSBsaXN0IG9mIGVsZW1lbnRzXG4gKiBAcGFyYW0gYWN0aXZlRWxlbWVudCAgIFRoZSBhY3RpdmUgZWxlbWVudFxuICogQHBhcmFtIHNob3VsZEdldE5leHQgICBDaG9vc2UgdG8gZ2V0IG5leHQgb3IgcHJldmlvdXMgZWxlbWVudFxuICogQHBhcmFtIGlzQ3ljbGVBbGxvd2VkXG4gKiBAcmV0dXJuIHtFbGVtZW50fGVsZW19IFRoZSBwcm9wZXIgZWxlbWVudFxuICovXG5jb25zdCBnZXROZXh0QWN0aXZlRWxlbWVudCA9IChsaXN0LCBhY3RpdmVFbGVtZW50LCBzaG91bGRHZXROZXh0LCBpc0N5Y2xlQWxsb3dlZCkgPT4ge1xuICBjb25zdCBsaXN0TGVuZ3RoID0gbGlzdC5sZW5ndGhcbiAgbGV0IGluZGV4ID0gbGlzdC5pbmRleE9mKGFjdGl2ZUVsZW1lbnQpXG5cbiAgLy8gaWYgdGhlIGVsZW1lbnQgZG9lcyBub3QgZXhpc3QgaW4gdGhlIGxpc3QgcmV0dXJuIGFuIGVsZW1lbnRcbiAgLy8gZGVwZW5kaW5nIG9uIHRoZSBkaXJlY3Rpb24gYW5kIGlmIGN5Y2xlIGlzIGFsbG93ZWRcbiAgaWYgKGluZGV4ID09PSAtMSkge1xuICAgIHJldHVybiAhc2hvdWxkR2V0TmV4dCAmJiBpc0N5Y2xlQWxsb3dlZCA/IGxpc3RbbGlzdExlbmd0aCAtIDFdIDogbGlzdFswXVxuICB9XG5cbiAgaW5kZXggKz0gc2hvdWxkR2V0TmV4dCA/IDEgOiAtMVxuXG4gIGlmIChpc0N5Y2xlQWxsb3dlZCkge1xuICAgIGluZGV4ID0gKGluZGV4ICsgbGlzdExlbmd0aCkgJSBsaXN0TGVuZ3RoXG4gIH1cblxuICByZXR1cm4gbGlzdFtNYXRoLm1heCgwLCBNYXRoLm1pbihpbmRleCwgbGlzdExlbmd0aCAtIDEpKV1cbn1cblxuZXhwb3J0IHtcbiAgZGVmaW5lSlF1ZXJ5UGx1Z2luLFxuICBleGVjdXRlLFxuICBleGVjdXRlQWZ0ZXJUcmFuc2l0aW9uLFxuICBmaW5kU2hhZG93Um9vdCxcbiAgZ2V0RWxlbWVudCxcbiAgZ2V0alF1ZXJ5LFxuICBnZXROZXh0QWN0aXZlRWxlbWVudCxcbiAgZ2V0VHJhbnNpdGlvbkR1cmF0aW9uRnJvbUVsZW1lbnQsXG4gIGdldFVJRCxcbiAgaXNEaXNhYmxlZCxcbiAgaXNFbGVtZW50LFxuICBpc1JUTCxcbiAgaXNWaXNpYmxlLFxuICBub29wLFxuICBvbkRPTUNvbnRlbnRMb2FkZWQsXG4gIHBhcnNlU2VsZWN0b3IsXG4gIHJlZmxvdyxcbiAgdHJpZ2dlclRyYW5zaXRpb25FbmQsXG4gIHRvVHlwZVxufVxuIiwgIi8qKlxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqIEJvb3RzdHJhcCBkb20vZXZlbnQtaGFuZGxlci5qc1xuICogTGljZW5zZWQgdW5kZXIgTUlUIChodHRwczovL2dpdGh1Yi5jb20vdHdicy9ib290c3RyYXAvYmxvYi9tYWluL0xJQ0VOU0UpXG4gKiAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICovXG5cbmltcG9ydCB7IGdldGpRdWVyeSB9IGZyb20gJy4uL3V0aWwvaW5kZXguanMnXG5cbi8qKlxuICogQ29uc3RhbnRzXG4gKi9cblxuY29uc3QgbmFtZXNwYWNlUmVnZXggPSAvW14uXSooPz1cXC4uKilcXC58LiovXG5jb25zdCBzdHJpcE5hbWVSZWdleCA9IC9cXC4uKi9cbmNvbnN0IHN0cmlwVWlkUmVnZXggPSAvOjpcXGQrJC9cbmNvbnN0IGV2ZW50UmVnaXN0cnkgPSB7fSAvLyBFdmVudHMgc3RvcmFnZVxubGV0IHVpZEV2ZW50ID0gMVxuY29uc3QgY3VzdG9tRXZlbnRzID0ge1xuICBtb3VzZWVudGVyOiAnbW91c2VvdmVyJyxcbiAgbW91c2VsZWF2ZTogJ21vdXNlb3V0J1xufVxuXG5jb25zdCBuYXRpdmVFdmVudHMgPSBuZXcgU2V0KFtcbiAgJ2NsaWNrJyxcbiAgJ2RibGNsaWNrJyxcbiAgJ21vdXNldXAnLFxuICAnbW91c2Vkb3duJyxcbiAgJ2NvbnRleHRtZW51JyxcbiAgJ21vdXNld2hlZWwnLFxuICAnRE9NTW91c2VTY3JvbGwnLFxuICAnbW91c2VvdmVyJyxcbiAgJ21vdXNlb3V0JyxcbiAgJ21vdXNlbW92ZScsXG4gICdzZWxlY3RzdGFydCcsXG4gICdzZWxlY3RlbmQnLFxuICAna2V5ZG93bicsXG4gICdrZXlwcmVzcycsXG4gICdrZXl1cCcsXG4gICdvcmllbnRhdGlvbmNoYW5nZScsXG4gICd0b3VjaHN0YXJ0JyxcbiAgJ3RvdWNobW92ZScsXG4gICd0b3VjaGVuZCcsXG4gICd0b3VjaGNhbmNlbCcsXG4gICdwb2ludGVyZG93bicsXG4gICdwb2ludGVybW92ZScsXG4gICdwb2ludGVydXAnLFxuICAncG9pbnRlcmxlYXZlJyxcbiAgJ3BvaW50ZXJjYW5jZWwnLFxuICAnZ2VzdHVyZXN0YXJ0JyxcbiAgJ2dlc3R1cmVjaGFuZ2UnLFxuICAnZ2VzdHVyZWVuZCcsXG4gICdmb2N1cycsXG4gICdibHVyJyxcbiAgJ2NoYW5nZScsXG4gICdyZXNldCcsXG4gICdzZWxlY3QnLFxuICAnc3VibWl0JyxcbiAgJ2ZvY3VzaW4nLFxuICAnZm9jdXNvdXQnLFxuICAnbG9hZCcsXG4gICd1bmxvYWQnLFxuICAnYmVmb3JldW5sb2FkJyxcbiAgJ3Jlc2l6ZScsXG4gICdtb3ZlJyxcbiAgJ0RPTUNvbnRlbnRMb2FkZWQnLFxuICAncmVhZHlzdGF0ZWNoYW5nZScsXG4gICdlcnJvcicsXG4gICdhYm9ydCcsXG4gICdzY3JvbGwnXG5dKVxuXG4vKipcbiAqIFByaXZhdGUgbWV0aG9kc1xuICovXG5cbmZ1bmN0aW9uIG1ha2VFdmVudFVpZChlbGVtZW50LCB1aWQpIHtcbiAgcmV0dXJuICh1aWQgJiYgYCR7dWlkfTo6JHt1aWRFdmVudCsrfWApIHx8IGVsZW1lbnQudWlkRXZlbnQgfHwgdWlkRXZlbnQrK1xufVxuXG5mdW5jdGlvbiBnZXRFbGVtZW50RXZlbnRzKGVsZW1lbnQpIHtcbiAgY29uc3QgdWlkID0gbWFrZUV2ZW50VWlkKGVsZW1lbnQpXG5cbiAgZWxlbWVudC51aWRFdmVudCA9IHVpZFxuICBldmVudFJlZ2lzdHJ5W3VpZF0gPSBldmVudFJlZ2lzdHJ5W3VpZF0gfHwge31cblxuICByZXR1cm4gZXZlbnRSZWdpc3RyeVt1aWRdXG59XG5cbmZ1bmN0aW9uIGJvb3RzdHJhcEhhbmRsZXIoZWxlbWVudCwgZm4pIHtcbiAgcmV0dXJuIGZ1bmN0aW9uIGhhbmRsZXIoZXZlbnQpIHtcbiAgICBoeWRyYXRlT2JqKGV2ZW50LCB7IGRlbGVnYXRlVGFyZ2V0OiBlbGVtZW50IH0pXG5cbiAgICBpZiAoaGFuZGxlci5vbmVPZmYpIHtcbiAgICAgIEV2ZW50SGFuZGxlci5vZmYoZWxlbWVudCwgZXZlbnQudHlwZSwgZm4pXG4gICAgfVxuXG4gICAgcmV0dXJuIGZuLmFwcGx5KGVsZW1lbnQsIFtldmVudF0pXG4gIH1cbn1cblxuZnVuY3Rpb24gYm9vdHN0cmFwRGVsZWdhdGlvbkhhbmRsZXIoZWxlbWVudCwgc2VsZWN0b3IsIGZuKSB7XG4gIHJldHVybiBmdW5jdGlvbiBoYW5kbGVyKGV2ZW50KSB7XG4gICAgY29uc3QgZG9tRWxlbWVudHMgPSBlbGVtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoc2VsZWN0b3IpXG5cbiAgICBmb3IgKGxldCB7IHRhcmdldCB9ID0gZXZlbnQ7IHRhcmdldCAmJiB0YXJnZXQgIT09IHRoaXM7IHRhcmdldCA9IHRhcmdldC5wYXJlbnROb2RlKSB7XG4gICAgICBmb3IgKGNvbnN0IGRvbUVsZW1lbnQgb2YgZG9tRWxlbWVudHMpIHtcbiAgICAgICAgaWYgKGRvbUVsZW1lbnQgIT09IHRhcmdldCkge1xuICAgICAgICAgIGNvbnRpbnVlXG4gICAgICAgIH1cblxuICAgICAgICBoeWRyYXRlT2JqKGV2ZW50LCB7IGRlbGVnYXRlVGFyZ2V0OiB0YXJnZXQgfSlcblxuICAgICAgICBpZiAoaGFuZGxlci5vbmVPZmYpIHtcbiAgICAgICAgICBFdmVudEhhbmRsZXIub2ZmKGVsZW1lbnQsIGV2ZW50LnR5cGUsIHNlbGVjdG9yLCBmbilcbiAgICAgICAgfVxuXG4gICAgICAgIHJldHVybiBmbi5hcHBseSh0YXJnZXQsIFtldmVudF0pXG4gICAgICB9XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIGZpbmRIYW5kbGVyKGV2ZW50cywgY2FsbGFibGUsIGRlbGVnYXRpb25TZWxlY3RvciA9IG51bGwpIHtcbiAgcmV0dXJuIE9iamVjdC52YWx1ZXMoZXZlbnRzKVxuICAgIC5maW5kKGV2ZW50ID0+IGV2ZW50LmNhbGxhYmxlID09PSBjYWxsYWJsZSAmJiBldmVudC5kZWxlZ2F0aW9uU2VsZWN0b3IgPT09IGRlbGVnYXRpb25TZWxlY3Rvcilcbn1cblxuZnVuY3Rpb24gbm9ybWFsaXplUGFyYW1ldGVycyhvcmlnaW5hbFR5cGVFdmVudCwgaGFuZGxlciwgZGVsZWdhdGlvbkZ1bmN0aW9uKSB7XG4gIGNvbnN0IGlzRGVsZWdhdGVkID0gdHlwZW9mIGhhbmRsZXIgPT09ICdzdHJpbmcnXG4gIC8vIFRPRE86IHRvb2x0aXAgcGFzc2VzIGBmYWxzZWAgaW5zdGVhZCBvZiBzZWxlY3Rvciwgc28gd2UgbmVlZCB0byBjaGVja1xuICBjb25zdCBjYWxsYWJsZSA9IGlzRGVsZWdhdGVkID8gZGVsZWdhdGlvbkZ1bmN0aW9uIDogKGhhbmRsZXIgfHwgZGVsZWdhdGlvbkZ1bmN0aW9uKVxuICBsZXQgdHlwZUV2ZW50ID0gZ2V0VHlwZUV2ZW50KG9yaWdpbmFsVHlwZUV2ZW50KVxuXG4gIGlmICghbmF0aXZlRXZlbnRzLmhhcyh0eXBlRXZlbnQpKSB7XG4gICAgdHlwZUV2ZW50ID0gb3JpZ2luYWxUeXBlRXZlbnRcbiAgfVxuXG4gIHJldHVybiBbaXNEZWxlZ2F0ZWQsIGNhbGxhYmxlLCB0eXBlRXZlbnRdXG59XG5cbmZ1bmN0aW9uIGFkZEhhbmRsZXIoZWxlbWVudCwgb3JpZ2luYWxUeXBlRXZlbnQsIGhhbmRsZXIsIGRlbGVnYXRpb25GdW5jdGlvbiwgb25lT2ZmKSB7XG4gIGlmICh0eXBlb2Ygb3JpZ2luYWxUeXBlRXZlbnQgIT09ICdzdHJpbmcnIHx8ICFlbGVtZW50KSB7XG4gICAgcmV0dXJuXG4gIH1cblxuICBsZXQgW2lzRGVsZWdhdGVkLCBjYWxsYWJsZSwgdHlwZUV2ZW50XSA9IG5vcm1hbGl6ZVBhcmFtZXRlcnMob3JpZ2luYWxUeXBlRXZlbnQsIGhhbmRsZXIsIGRlbGVnYXRpb25GdW5jdGlvbilcblxuICAvLyBpbiBjYXNlIG9mIG1vdXNlZW50ZXIgb3IgbW91c2VsZWF2ZSB3cmFwIHRoZSBoYW5kbGVyIHdpdGhpbiBhIGZ1bmN0aW9uIHRoYXQgY2hlY2tzIGZvciBpdHMgRE9NIHBvc2l0aW9uXG4gIC8vIHRoaXMgcHJldmVudHMgdGhlIGhhbmRsZXIgZnJvbSBiZWluZyBkaXNwYXRjaGVkIHRoZSBzYW1lIHdheSBhcyBtb3VzZW92ZXIgb3IgbW91c2VvdXQgZG9lc1xuICBpZiAob3JpZ2luYWxUeXBlRXZlbnQgaW4gY3VzdG9tRXZlbnRzKSB7XG4gICAgY29uc3Qgd3JhcEZ1bmN0aW9uID0gZm4gPT4ge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uIChldmVudCkge1xuICAgICAgICBpZiAoIWV2ZW50LnJlbGF0ZWRUYXJnZXQgfHwgKGV2ZW50LnJlbGF0ZWRUYXJnZXQgIT09IGV2ZW50LmRlbGVnYXRlVGFyZ2V0ICYmICFldmVudC5kZWxlZ2F0ZVRhcmdldC5jb250YWlucyhldmVudC5yZWxhdGVkVGFyZ2V0KSkpIHtcbiAgICAgICAgICByZXR1cm4gZm4uY2FsbCh0aGlzLCBldmVudClcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGNhbGxhYmxlID0gd3JhcEZ1bmN0aW9uKGNhbGxhYmxlKVxuICB9XG5cbiAgY29uc3QgZXZlbnRzID0gZ2V0RWxlbWVudEV2ZW50cyhlbGVtZW50KVxuICBjb25zdCBoYW5kbGVycyA9IGV2ZW50c1t0eXBlRXZlbnRdIHx8IChldmVudHNbdHlwZUV2ZW50XSA9IHt9KVxuICBjb25zdCBwcmV2aW91c0Z1bmN0aW9uID0gZmluZEhhbmRsZXIoaGFuZGxlcnMsIGNhbGxhYmxlLCBpc0RlbGVnYXRlZCA/IGhhbmRsZXIgOiBudWxsKVxuXG4gIGlmIChwcmV2aW91c0Z1bmN0aW9uKSB7XG4gICAgcHJldmlvdXNGdW5jdGlvbi5vbmVPZmYgPSBwcmV2aW91c0Z1bmN0aW9uLm9uZU9mZiAmJiBvbmVPZmZcblxuICAgIHJldHVyblxuICB9XG5cbiAgY29uc3QgdWlkID0gbWFrZUV2ZW50VWlkKGNhbGxhYmxlLCBvcmlnaW5hbFR5cGVFdmVudC5yZXBsYWNlKG5hbWVzcGFjZVJlZ2V4LCAnJykpXG4gIGNvbnN0IGZuID0gaXNEZWxlZ2F0ZWQgP1xuICAgIGJvb3RzdHJhcERlbGVnYXRpb25IYW5kbGVyKGVsZW1lbnQsIGhhbmRsZXIsIGNhbGxhYmxlKSA6XG4gICAgYm9vdHN0cmFwSGFuZGxlcihlbGVtZW50LCBjYWxsYWJsZSlcblxuICBmbi5kZWxlZ2F0aW9uU2VsZWN0b3IgPSBpc0RlbGVnYXRlZCA/IGhhbmRsZXIgOiBudWxsXG4gIGZuLmNhbGxhYmxlID0gY2FsbGFibGVcbiAgZm4ub25lT2ZmID0gb25lT2ZmXG4gIGZuLnVpZEV2ZW50ID0gdWlkXG4gIGhhbmRsZXJzW3VpZF0gPSBmblxuXG4gIGVsZW1lbnQuYWRkRXZlbnRMaXN0ZW5lcih0eXBlRXZlbnQsIGZuLCBpc0RlbGVnYXRlZClcbn1cblxuZnVuY3Rpb24gcmVtb3ZlSGFuZGxlcihlbGVtZW50LCBldmVudHMsIHR5cGVFdmVudCwgaGFuZGxlciwgZGVsZWdhdGlvblNlbGVjdG9yKSB7XG4gIGNvbnN0IGZuID0gZmluZEhhbmRsZXIoZXZlbnRzW3R5cGVFdmVudF0sIGhhbmRsZXIsIGRlbGVnYXRpb25TZWxlY3RvcilcblxuICBpZiAoIWZuKSB7XG4gICAgcmV0dXJuXG4gIH1cblxuICBlbGVtZW50LnJlbW92ZUV2ZW50TGlzdGVuZXIodHlwZUV2ZW50LCBmbiwgQm9vbGVhbihkZWxlZ2F0aW9uU2VsZWN0b3IpKVxuICBkZWxldGUgZXZlbnRzW3R5cGVFdmVudF1bZm4udWlkRXZlbnRdXG59XG5cbmZ1bmN0aW9uIHJlbW92ZU5hbWVzcGFjZWRIYW5kbGVycyhlbGVtZW50LCBldmVudHMsIHR5cGVFdmVudCwgbmFtZXNwYWNlKSB7XG4gIGNvbnN0IHN0b3JlRWxlbWVudEV2ZW50ID0gZXZlbnRzW3R5cGVFdmVudF0gfHwge31cblxuICBmb3IgKGNvbnN0IFtoYW5kbGVyS2V5LCBldmVudF0gb2YgT2JqZWN0LmVudHJpZXMoc3RvcmVFbGVtZW50RXZlbnQpKSB7XG4gICAgaWYgKGhhbmRsZXJLZXkuaW5jbHVkZXMobmFtZXNwYWNlKSkge1xuICAgICAgcmVtb3ZlSGFuZGxlcihlbGVtZW50LCBldmVudHMsIHR5cGVFdmVudCwgZXZlbnQuY2FsbGFibGUsIGV2ZW50LmRlbGVnYXRpb25TZWxlY3RvcilcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0VHlwZUV2ZW50KGV2ZW50KSB7XG4gIC8vIGFsbG93IHRvIGdldCB0aGUgbmF0aXZlIGV2ZW50cyBmcm9tIG5hbWVzcGFjZWQgZXZlbnRzICgnY2xpY2suYnMuYnV0dG9uJyAtLT4gJ2NsaWNrJylcbiAgZXZlbnQgPSBldmVudC5yZXBsYWNlKHN0cmlwTmFtZVJlZ2V4LCAnJylcbiAgcmV0dXJuIGN1c3RvbUV2ZW50c1tldmVudF0gfHwgZXZlbnRcbn1cblxuY29uc3QgRXZlbnRIYW5kbGVyID0ge1xuICBvbihlbGVtZW50LCBldmVudCwgaGFuZGxlciwgZGVsZWdhdGlvbkZ1bmN0aW9uKSB7XG4gICAgYWRkSGFuZGxlcihlbGVtZW50LCBldmVudCwgaGFuZGxlciwgZGVsZWdhdGlvbkZ1bmN0aW9uLCBmYWxzZSlcbiAgfSxcblxuICBvbmUoZWxlbWVudCwgZXZlbnQsIGhhbmRsZXIsIGRlbGVnYXRpb25GdW5jdGlvbikge1xuICAgIGFkZEhhbmRsZXIoZWxlbWVudCwgZXZlbnQsIGhhbmRsZXIsIGRlbGVnYXRpb25GdW5jdGlvbiwgdHJ1ZSlcbiAgfSxcblxuICBvZmYoZWxlbWVudCwgb3JpZ2luYWxUeXBlRXZlbnQsIGhhbmRsZXIsIGRlbGVnYXRpb25GdW5jdGlvbikge1xuICAgIGlmICh0eXBlb2Ygb3JpZ2luYWxUeXBlRXZlbnQgIT09ICdzdHJpbmcnIHx8ICFlbGVtZW50KSB7XG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICBjb25zdCBbaXNEZWxlZ2F0ZWQsIGNhbGxhYmxlLCB0eXBlRXZlbnRdID0gbm9ybWFsaXplUGFyYW1ldGVycyhvcmlnaW5hbFR5cGVFdmVudCwgaGFuZGxlciwgZGVsZWdhdGlvbkZ1bmN0aW9uKVxuICAgIGNvbnN0IGluTmFtZXNwYWNlID0gdHlwZUV2ZW50ICE9PSBvcmlnaW5hbFR5cGVFdmVudFxuICAgIGNvbnN0IGV2ZW50cyA9IGdldEVsZW1lbnRFdmVudHMoZWxlbWVudClcbiAgICBjb25zdCBzdG9yZUVsZW1lbnRFdmVudCA9IGV2ZW50c1t0eXBlRXZlbnRdIHx8IHt9XG4gICAgY29uc3QgaXNOYW1lc3BhY2UgPSBvcmlnaW5hbFR5cGVFdmVudC5zdGFydHNXaXRoKCcuJylcblxuICAgIGlmICh0eXBlb2YgY2FsbGFibGUgIT09ICd1bmRlZmluZWQnKSB7XG4gICAgICAvLyBTaW1wbGVzdCBjYXNlOiBoYW5kbGVyIGlzIHBhc3NlZCwgcmVtb3ZlIHRoYXQgbGlzdGVuZXIgT05MWS5cbiAgICAgIGlmICghT2JqZWN0LmtleXMoc3RvcmVFbGVtZW50RXZlbnQpLmxlbmd0aCkge1xuICAgICAgICByZXR1cm5cbiAgICAgIH1cblxuICAgICAgcmVtb3ZlSGFuZGxlcihlbGVtZW50LCBldmVudHMsIHR5cGVFdmVudCwgY2FsbGFibGUsIGlzRGVsZWdhdGVkID8gaGFuZGxlciA6IG51bGwpXG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICBpZiAoaXNOYW1lc3BhY2UpIHtcbiAgICAgIGZvciAoY29uc3QgZWxlbWVudEV2ZW50IG9mIE9iamVjdC5rZXlzKGV2ZW50cykpIHtcbiAgICAgICAgcmVtb3ZlTmFtZXNwYWNlZEhhbmRsZXJzKGVsZW1lbnQsIGV2ZW50cywgZWxlbWVudEV2ZW50LCBvcmlnaW5hbFR5cGVFdmVudC5zbGljZSgxKSlcbiAgICAgIH1cbiAgICB9XG5cbiAgICBmb3IgKGNvbnN0IFtrZXlIYW5kbGVycywgZXZlbnRdIG9mIE9iamVjdC5lbnRyaWVzKHN0b3JlRWxlbWVudEV2ZW50KSkge1xuICAgICAgY29uc3QgaGFuZGxlcktleSA9IGtleUhhbmRsZXJzLnJlcGxhY2Uoc3RyaXBVaWRSZWdleCwgJycpXG5cbiAgICAgIGlmICghaW5OYW1lc3BhY2UgfHwgb3JpZ2luYWxUeXBlRXZlbnQuaW5jbHVkZXMoaGFuZGxlcktleSkpIHtcbiAgICAgICAgcmVtb3ZlSGFuZGxlcihlbGVtZW50LCBldmVudHMsIHR5cGVFdmVudCwgZXZlbnQuY2FsbGFibGUsIGV2ZW50LmRlbGVnYXRpb25TZWxlY3RvcilcbiAgICAgIH1cbiAgICB9XG4gIH0sXG5cbiAgdHJpZ2dlcihlbGVtZW50LCBldmVudCwgYXJncykge1xuICAgIGlmICh0eXBlb2YgZXZlbnQgIT09ICdzdHJpbmcnIHx8ICFlbGVtZW50KSB7XG4gICAgICByZXR1cm4gbnVsbFxuICAgIH1cblxuICAgIGNvbnN0ICQgPSBnZXRqUXVlcnkoKVxuICAgIGNvbnN0IHR5cGVFdmVudCA9IGdldFR5cGVFdmVudChldmVudClcbiAgICBjb25zdCBpbk5hbWVzcGFjZSA9IGV2ZW50ICE9PSB0eXBlRXZlbnRcblxuICAgIGxldCBqUXVlcnlFdmVudCA9IG51bGxcbiAgICBsZXQgYnViYmxlcyA9IHRydWVcbiAgICBsZXQgbmF0aXZlRGlzcGF0Y2ggPSB0cnVlXG4gICAgbGV0IGRlZmF1bHRQcmV2ZW50ZWQgPSBmYWxzZVxuXG4gICAgaWYgKGluTmFtZXNwYWNlICYmICQpIHtcbiAgICAgIGpRdWVyeUV2ZW50ID0gJC5FdmVudChldmVudCwgYXJncylcblxuICAgICAgJChlbGVtZW50KS50cmlnZ2VyKGpRdWVyeUV2ZW50KVxuICAgICAgYnViYmxlcyA9ICFqUXVlcnlFdmVudC5pc1Byb3BhZ2F0aW9uU3RvcHBlZCgpXG4gICAgICBuYXRpdmVEaXNwYXRjaCA9ICFqUXVlcnlFdmVudC5pc0ltbWVkaWF0ZVByb3BhZ2F0aW9uU3RvcHBlZCgpXG4gICAgICBkZWZhdWx0UHJldmVudGVkID0galF1ZXJ5RXZlbnQuaXNEZWZhdWx0UHJldmVudGVkKClcbiAgICB9XG5cbiAgICBjb25zdCBldnQgPSBoeWRyYXRlT2JqKG5ldyBFdmVudChldmVudCwgeyBidWJibGVzLCBjYW5jZWxhYmxlOiB0cnVlIH0pLCBhcmdzKVxuXG4gICAgaWYgKGRlZmF1bHRQcmV2ZW50ZWQpIHtcbiAgICAgIGV2dC5wcmV2ZW50RGVmYXVsdCgpXG4gICAgfVxuXG4gICAgaWYgKG5hdGl2ZURpc3BhdGNoKSB7XG4gICAgICBlbGVtZW50LmRpc3BhdGNoRXZlbnQoZXZ0KVxuICAgIH1cblxuICAgIGlmIChldnQuZGVmYXVsdFByZXZlbnRlZCAmJiBqUXVlcnlFdmVudCkge1xuICAgICAgalF1ZXJ5RXZlbnQucHJldmVudERlZmF1bHQoKVxuICAgIH1cblxuICAgIHJldHVybiBldnRcbiAgfVxufVxuXG5mdW5jdGlvbiBoeWRyYXRlT2JqKG9iaiwgbWV0YSA9IHt9KSB7XG4gIGZvciAoY29uc3QgW2tleSwgdmFsdWVdIG9mIE9iamVjdC5lbnRyaWVzKG1ldGEpKSB7XG4gICAgdHJ5IHtcbiAgICAgIG9ialtrZXldID0gdmFsdWVcbiAgICB9IGNhdGNoIHtcbiAgICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShvYmosIGtleSwge1xuICAgICAgICBjb25maWd1cmFibGU6IHRydWUsXG4gICAgICAgIGdldCgpIHtcbiAgICAgICAgICByZXR1cm4gdmFsdWVcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICB9XG4gIH1cblxuICByZXR1cm4gb2JqXG59XG5cbmV4cG9ydCBkZWZhdWx0IEV2ZW50SGFuZGxlclxuIiwgIi8qKlxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqIEJvb3RzdHJhcCBkb20vbWFuaXB1bGF0b3IuanNcbiAqIExpY2Vuc2VkIHVuZGVyIE1JVCAoaHR0cHM6Ly9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwL2Jsb2IvbWFpbi9MSUNFTlNFKVxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqL1xuXG5mdW5jdGlvbiBub3JtYWxpemVEYXRhKHZhbHVlKSB7XG4gIGlmICh2YWx1ZSA9PT0gJ3RydWUnKSB7XG4gICAgcmV0dXJuIHRydWVcbiAgfVxuXG4gIGlmICh2YWx1ZSA9PT0gJ2ZhbHNlJykge1xuICAgIHJldHVybiBmYWxzZVxuICB9XG5cbiAgaWYgKHZhbHVlID09PSBOdW1iZXIodmFsdWUpLnRvU3RyaW5nKCkpIHtcbiAgICByZXR1cm4gTnVtYmVyKHZhbHVlKVxuICB9XG5cbiAgaWYgKHZhbHVlID09PSAnJyB8fCB2YWx1ZSA9PT0gJ251bGwnKSB7XG4gICAgcmV0dXJuIG51bGxcbiAgfVxuXG4gIGlmICh0eXBlb2YgdmFsdWUgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuIHZhbHVlXG4gIH1cblxuICB0cnkge1xuICAgIHJldHVybiBKU09OLnBhcnNlKGRlY29kZVVSSUNvbXBvbmVudCh2YWx1ZSkpXG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiB2YWx1ZVxuICB9XG59XG5cbmZ1bmN0aW9uIG5vcm1hbGl6ZURhdGFLZXkoa2V5KSB7XG4gIHJldHVybiBrZXkucmVwbGFjZSgvW0EtWl0vZywgY2hyID0+IGAtJHtjaHIudG9Mb3dlckNhc2UoKX1gKVxufVxuXG5jb25zdCBNYW5pcHVsYXRvciA9IHtcbiAgc2V0RGF0YUF0dHJpYnV0ZShlbGVtZW50LCBrZXksIHZhbHVlKSB7XG4gICAgZWxlbWVudC5zZXRBdHRyaWJ1dGUoYGRhdGEtYnMtJHtub3JtYWxpemVEYXRhS2V5KGtleSl9YCwgdmFsdWUpXG4gIH0sXG5cbiAgcmVtb3ZlRGF0YUF0dHJpYnV0ZShlbGVtZW50LCBrZXkpIHtcbiAgICBlbGVtZW50LnJlbW92ZUF0dHJpYnV0ZShgZGF0YS1icy0ke25vcm1hbGl6ZURhdGFLZXkoa2V5KX1gKVxuICB9LFxuXG4gIGdldERhdGFBdHRyaWJ1dGVzKGVsZW1lbnQpIHtcbiAgICBpZiAoIWVsZW1lbnQpIHtcbiAgICAgIHJldHVybiB7fVxuICAgIH1cblxuICAgIGNvbnN0IGF0dHJpYnV0ZXMgPSB7fVxuICAgIGNvbnN0IGJzS2V5cyA9IE9iamVjdC5rZXlzKGVsZW1lbnQuZGF0YXNldCkuZmlsdGVyKGtleSA9PiBrZXkuc3RhcnRzV2l0aCgnYnMnKSAmJiAha2V5LnN0YXJ0c1dpdGgoJ2JzQ29uZmlnJykpXG5cbiAgICBmb3IgKGNvbnN0IGtleSBvZiBic0tleXMpIHtcbiAgICAgIGxldCBwdXJlS2V5ID0ga2V5LnJlcGxhY2UoL15icy8sICcnKVxuICAgICAgcHVyZUtleSA9IHB1cmVLZXkuY2hhckF0KDApLnRvTG93ZXJDYXNlKCkgKyBwdXJlS2V5LnNsaWNlKDEsIHB1cmVLZXkubGVuZ3RoKVxuICAgICAgYXR0cmlidXRlc1twdXJlS2V5XSA9IG5vcm1hbGl6ZURhdGEoZWxlbWVudC5kYXRhc2V0W2tleV0pXG4gICAgfVxuXG4gICAgcmV0dXJuIGF0dHJpYnV0ZXNcbiAgfSxcblxuICBnZXREYXRhQXR0cmlidXRlKGVsZW1lbnQsIGtleSkge1xuICAgIHJldHVybiBub3JtYWxpemVEYXRhKGVsZW1lbnQuZ2V0QXR0cmlidXRlKGBkYXRhLWJzLSR7bm9ybWFsaXplRGF0YUtleShrZXkpfWApKVxuICB9XG59XG5cbmV4cG9ydCBkZWZhdWx0IE1hbmlwdWxhdG9yXG4iLCAiLyoqXG4gKiAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICogQm9vdHN0cmFwIHV0aWwvY29uZmlnLmpzXG4gKiBMaWNlbnNlZCB1bmRlciBNSVQgKGh0dHBzOi8vZ2l0aHViLmNvbS90d2JzL2Jvb3RzdHJhcC9ibG9iL21haW4vTElDRU5TRSlcbiAqIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG4gKi9cblxuaW1wb3J0IE1hbmlwdWxhdG9yIGZyb20gJy4uL2RvbS9tYW5pcHVsYXRvci5qcydcbmltcG9ydCB7IGlzRWxlbWVudCwgdG9UeXBlIH0gZnJvbSAnLi9pbmRleC5qcydcblxuLyoqXG4gKiBDbGFzcyBkZWZpbml0aW9uXG4gKi9cblxuY2xhc3MgQ29uZmlnIHtcbiAgLy8gR2V0dGVyc1xuICBzdGF0aWMgZ2V0IERlZmF1bHQoKSB7XG4gICAgcmV0dXJuIHt9XG4gIH1cblxuICBzdGF0aWMgZ2V0IERlZmF1bHRUeXBlKCkge1xuICAgIHJldHVybiB7fVxuICB9XG5cbiAgc3RhdGljIGdldCBOQU1FKCkge1xuICAgIHRocm93IG5ldyBFcnJvcignWW91IGhhdmUgdG8gaW1wbGVtZW50IHRoZSBzdGF0aWMgbWV0aG9kIFwiTkFNRVwiLCBmb3IgZWFjaCBjb21wb25lbnQhJylcbiAgfVxuXG4gIF9nZXRDb25maWcoY29uZmlnKSB7XG4gICAgY29uZmlnID0gdGhpcy5fbWVyZ2VDb25maWdPYmooY29uZmlnKVxuICAgIGNvbmZpZyA9IHRoaXMuX2NvbmZpZ0FmdGVyTWVyZ2UoY29uZmlnKVxuICAgIHRoaXMuX3R5cGVDaGVja0NvbmZpZyhjb25maWcpXG4gICAgcmV0dXJuIGNvbmZpZ1xuICB9XG5cbiAgX2NvbmZpZ0FmdGVyTWVyZ2UoY29uZmlnKSB7XG4gICAgcmV0dXJuIGNvbmZpZ1xuICB9XG5cbiAgX21lcmdlQ29uZmlnT2JqKGNvbmZpZywgZWxlbWVudCkge1xuICAgIGNvbnN0IGpzb25Db25maWcgPSBpc0VsZW1lbnQoZWxlbWVudCkgPyBNYW5pcHVsYXRvci5nZXREYXRhQXR0cmlidXRlKGVsZW1lbnQsICdjb25maWcnKSA6IHt9IC8vIHRyeSB0byBwYXJzZVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIC4uLnRoaXMuY29uc3RydWN0b3IuRGVmYXVsdCxcbiAgICAgIC4uLih0eXBlb2YganNvbkNvbmZpZyA9PT0gJ29iamVjdCcgPyBqc29uQ29uZmlnIDoge30pLFxuICAgICAgLi4uKGlzRWxlbWVudChlbGVtZW50KSA/IE1hbmlwdWxhdG9yLmdldERhdGFBdHRyaWJ1dGVzKGVsZW1lbnQpIDoge30pLFxuICAgICAgLi4uKHR5cGVvZiBjb25maWcgPT09ICdvYmplY3QnID8gY29uZmlnIDoge30pXG4gICAgfVxuICB9XG5cbiAgX3R5cGVDaGVja0NvbmZpZyhjb25maWcsIGNvbmZpZ1R5cGVzID0gdGhpcy5jb25zdHJ1Y3Rvci5EZWZhdWx0VHlwZSkge1xuICAgIGZvciAoY29uc3QgW3Byb3BlcnR5LCBleHBlY3RlZFR5cGVzXSBvZiBPYmplY3QuZW50cmllcyhjb25maWdUeXBlcykpIHtcbiAgICAgIGNvbnN0IHZhbHVlID0gY29uZmlnW3Byb3BlcnR5XVxuICAgICAgY29uc3QgdmFsdWVUeXBlID0gaXNFbGVtZW50KHZhbHVlKSA/ICdlbGVtZW50JyA6IHRvVHlwZSh2YWx1ZSlcblxuICAgICAgaWYgKCFuZXcgUmVnRXhwKGV4cGVjdGVkVHlwZXMpLnRlc3QodmFsdWVUeXBlKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKFxuICAgICAgICAgIGAke3RoaXMuY29uc3RydWN0b3IuTkFNRS50b1VwcGVyQ2FzZSgpfTogT3B0aW9uIFwiJHtwcm9wZXJ0eX1cIiBwcm92aWRlZCB0eXBlIFwiJHt2YWx1ZVR5cGV9XCIgYnV0IGV4cGVjdGVkIHR5cGUgXCIke2V4cGVjdGVkVHlwZXN9XCIuYFxuICAgICAgICApXG4gICAgICB9XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBkZWZhdWx0IENvbmZpZ1xuIiwgIi8qKlxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqIEJvb3RzdHJhcCBiYXNlLWNvbXBvbmVudC5qc1xuICogTGljZW5zZWQgdW5kZXIgTUlUIChodHRwczovL2dpdGh1Yi5jb20vdHdicy9ib290c3RyYXAvYmxvYi9tYWluL0xJQ0VOU0UpXG4gKiAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICovXG5cbmltcG9ydCBEYXRhIGZyb20gJy4vZG9tL2RhdGEuanMnXG5pbXBvcnQgRXZlbnRIYW5kbGVyIGZyb20gJy4vZG9tL2V2ZW50LWhhbmRsZXIuanMnXG5pbXBvcnQgQ29uZmlnIGZyb20gJy4vdXRpbC9jb25maWcuanMnXG5pbXBvcnQgeyBleGVjdXRlQWZ0ZXJUcmFuc2l0aW9uLCBnZXRFbGVtZW50IH0gZnJvbSAnLi91dGlsL2luZGV4LmpzJ1xuXG4vKipcbiAqIENvbnN0YW50c1xuICovXG5cbmNvbnN0IFZFUlNJT04gPSAnNS4zLjInXG5cbi8qKlxuICogQ2xhc3MgZGVmaW5pdGlvblxuICovXG5cbmNsYXNzIEJhc2VDb21wb25lbnQgZXh0ZW5kcyBDb25maWcge1xuICBjb25zdHJ1Y3RvcihlbGVtZW50LCBjb25maWcpIHtcbiAgICBzdXBlcigpXG5cbiAgICBlbGVtZW50ID0gZ2V0RWxlbWVudChlbGVtZW50KVxuICAgIGlmICghZWxlbWVudCkge1xuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgdGhpcy5fZWxlbWVudCA9IGVsZW1lbnRcbiAgICB0aGlzLl9jb25maWcgPSB0aGlzLl9nZXRDb25maWcoY29uZmlnKVxuXG4gICAgRGF0YS5zZXQodGhpcy5fZWxlbWVudCwgdGhpcy5jb25zdHJ1Y3Rvci5EQVRBX0tFWSwgdGhpcylcbiAgfVxuXG4gIC8vIFB1YmxpY1xuICBkaXNwb3NlKCkge1xuICAgIERhdGEucmVtb3ZlKHRoaXMuX2VsZW1lbnQsIHRoaXMuY29uc3RydWN0b3IuREFUQV9LRVkpXG4gICAgRXZlbnRIYW5kbGVyLm9mZih0aGlzLl9lbGVtZW50LCB0aGlzLmNvbnN0cnVjdG9yLkVWRU5UX0tFWSlcblxuICAgIGZvciAoY29uc3QgcHJvcGVydHlOYW1lIG9mIE9iamVjdC5nZXRPd25Qcm9wZXJ0eU5hbWVzKHRoaXMpKSB7XG4gICAgICB0aGlzW3Byb3BlcnR5TmFtZV0gPSBudWxsXG4gICAgfVxuICB9XG5cbiAgX3F1ZXVlQ2FsbGJhY2soY2FsbGJhY2ssIGVsZW1lbnQsIGlzQW5pbWF0ZWQgPSB0cnVlKSB7XG4gICAgZXhlY3V0ZUFmdGVyVHJhbnNpdGlvbihjYWxsYmFjaywgZWxlbWVudCwgaXNBbmltYXRlZClcbiAgfVxuXG4gIF9nZXRDb25maWcoY29uZmlnKSB7XG4gICAgY29uZmlnID0gdGhpcy5fbWVyZ2VDb25maWdPYmooY29uZmlnLCB0aGlzLl9lbGVtZW50KVxuICAgIGNvbmZpZyA9IHRoaXMuX2NvbmZpZ0FmdGVyTWVyZ2UoY29uZmlnKVxuICAgIHRoaXMuX3R5cGVDaGVja0NvbmZpZyhjb25maWcpXG4gICAgcmV0dXJuIGNvbmZpZ1xuICB9XG5cbiAgLy8gU3RhdGljXG4gIHN0YXRpYyBnZXRJbnN0YW5jZShlbGVtZW50KSB7XG4gICAgcmV0dXJuIERhdGEuZ2V0KGdldEVsZW1lbnQoZWxlbWVudCksIHRoaXMuREFUQV9LRVkpXG4gIH1cblxuICBzdGF0aWMgZ2V0T3JDcmVhdGVJbnN0YW5jZShlbGVtZW50LCBjb25maWcgPSB7fSkge1xuICAgIHJldHVybiB0aGlzLmdldEluc3RhbmNlKGVsZW1lbnQpIHx8IG5ldyB0aGlzKGVsZW1lbnQsIHR5cGVvZiBjb25maWcgPT09ICdvYmplY3QnID8gY29uZmlnIDogbnVsbClcbiAgfVxuXG4gIHN0YXRpYyBnZXQgVkVSU0lPTigpIHtcbiAgICByZXR1cm4gVkVSU0lPTlxuICB9XG5cbiAgc3RhdGljIGdldCBEQVRBX0tFWSgpIHtcbiAgICByZXR1cm4gYGJzLiR7dGhpcy5OQU1FfWBcbiAgfVxuXG4gIHN0YXRpYyBnZXQgRVZFTlRfS0VZKCkge1xuICAgIHJldHVybiBgLiR7dGhpcy5EQVRBX0tFWX1gXG4gIH1cblxuICBzdGF0aWMgZXZlbnROYW1lKG5hbWUpIHtcbiAgICByZXR1cm4gYCR7bmFtZX0ke3RoaXMuRVZFTlRfS0VZfWBcbiAgfVxufVxuXG5leHBvcnQgZGVmYXVsdCBCYXNlQ29tcG9uZW50XG4iLCAiLyoqXG4gKiAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuICogQm9vdHN0cmFwIGRvbS9zZWxlY3Rvci1lbmdpbmUuanNcbiAqIExpY2Vuc2VkIHVuZGVyIE1JVCAoaHR0cHM6Ly9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwL2Jsb2IvbWFpbi9MSUNFTlNFKVxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqL1xuXG5pbXBvcnQgeyBpc0Rpc2FibGVkLCBpc1Zpc2libGUsIHBhcnNlU2VsZWN0b3IgfSBmcm9tICcuLi91dGlsL2luZGV4LmpzJ1xuXG5jb25zdCBnZXRTZWxlY3RvciA9IGVsZW1lbnQgPT4ge1xuICBsZXQgc2VsZWN0b3IgPSBlbGVtZW50LmdldEF0dHJpYnV0ZSgnZGF0YS1icy10YXJnZXQnKVxuXG4gIGlmICghc2VsZWN0b3IgfHwgc2VsZWN0b3IgPT09ICcjJykge1xuICAgIGxldCBocmVmQXR0cmlidXRlID0gZWxlbWVudC5nZXRBdHRyaWJ1dGUoJ2hyZWYnKVxuXG4gICAgLy8gVGhlIG9ubHkgdmFsaWQgY29udGVudCB0aGF0IGNvdWxkIGRvdWJsZSBhcyBhIHNlbGVjdG9yIGFyZSBJRHMgb3IgY2xhc3NlcyxcbiAgICAvLyBzbyBldmVyeXRoaW5nIHN0YXJ0aW5nIHdpdGggYCNgIG9yIGAuYC4gSWYgYSBcInJlYWxcIiBVUkwgaXMgdXNlZCBhcyB0aGUgc2VsZWN0b3IsXG4gICAgLy8gYGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JgIHdpbGwgcmlnaHRmdWxseSBjb21wbGFpbiBpdCBpcyBpbnZhbGlkLlxuICAgIC8vIFNlZSBodHRwczovL2dpdGh1Yi5jb20vdHdicy9ib290c3RyYXAvaXNzdWVzLzMyMjczXG4gICAgaWYgKCFocmVmQXR0cmlidXRlIHx8ICghaHJlZkF0dHJpYnV0ZS5pbmNsdWRlcygnIycpICYmICFocmVmQXR0cmlidXRlLnN0YXJ0c1dpdGgoJy4nKSkpIHtcbiAgICAgIHJldHVybiBudWxsXG4gICAgfVxuXG4gICAgLy8gSnVzdCBpbiBjYXNlIHNvbWUgQ01TIHB1dHMgb3V0IGEgZnVsbCBVUkwgd2l0aCB0aGUgYW5jaG9yIGFwcGVuZGVkXG4gICAgaWYgKGhyZWZBdHRyaWJ1dGUuaW5jbHVkZXMoJyMnKSAmJiAhaHJlZkF0dHJpYnV0ZS5zdGFydHNXaXRoKCcjJykpIHtcbiAgICAgIGhyZWZBdHRyaWJ1dGUgPSBgIyR7aHJlZkF0dHJpYnV0ZS5zcGxpdCgnIycpWzFdfWBcbiAgICB9XG5cbiAgICBzZWxlY3RvciA9IGhyZWZBdHRyaWJ1dGUgJiYgaHJlZkF0dHJpYnV0ZSAhPT0gJyMnID8gcGFyc2VTZWxlY3RvcihocmVmQXR0cmlidXRlLnRyaW0oKSkgOiBudWxsXG4gIH1cblxuICByZXR1cm4gc2VsZWN0b3Jcbn1cblxuY29uc3QgU2VsZWN0b3JFbmdpbmUgPSB7XG4gIGZpbmQoc2VsZWN0b3IsIGVsZW1lbnQgPSBkb2N1bWVudC5kb2N1bWVudEVsZW1lbnQpIHtcbiAgICByZXR1cm4gW10uY29uY2F0KC4uLkVsZW1lbnQucHJvdG90eXBlLnF1ZXJ5U2VsZWN0b3JBbGwuY2FsbChlbGVtZW50LCBzZWxlY3RvcikpXG4gIH0sXG5cbiAgZmluZE9uZShzZWxlY3RvciwgZWxlbWVudCA9IGRvY3VtZW50LmRvY3VtZW50RWxlbWVudCkge1xuICAgIHJldHVybiBFbGVtZW50LnByb3RvdHlwZS5xdWVyeVNlbGVjdG9yLmNhbGwoZWxlbWVudCwgc2VsZWN0b3IpXG4gIH0sXG5cbiAgY2hpbGRyZW4oZWxlbWVudCwgc2VsZWN0b3IpIHtcbiAgICByZXR1cm4gW10uY29uY2F0KC4uLmVsZW1lbnQuY2hpbGRyZW4pLmZpbHRlcihjaGlsZCA9PiBjaGlsZC5tYXRjaGVzKHNlbGVjdG9yKSlcbiAgfSxcblxuICBwYXJlbnRzKGVsZW1lbnQsIHNlbGVjdG9yKSB7XG4gICAgY29uc3QgcGFyZW50cyA9IFtdXG4gICAgbGV0IGFuY2VzdG9yID0gZWxlbWVudC5wYXJlbnROb2RlLmNsb3Nlc3Qoc2VsZWN0b3IpXG5cbiAgICB3aGlsZSAoYW5jZXN0b3IpIHtcbiAgICAgIHBhcmVudHMucHVzaChhbmNlc3RvcilcbiAgICAgIGFuY2VzdG9yID0gYW5jZXN0b3IucGFyZW50Tm9kZS5jbG9zZXN0KHNlbGVjdG9yKVxuICAgIH1cblxuICAgIHJldHVybiBwYXJlbnRzXG4gIH0sXG5cbiAgcHJldihlbGVtZW50LCBzZWxlY3Rvcikge1xuICAgIGxldCBwcmV2aW91cyA9IGVsZW1lbnQucHJldmlvdXNFbGVtZW50U2libGluZ1xuXG4gICAgd2hpbGUgKHByZXZpb3VzKSB7XG4gICAgICBpZiAocHJldmlvdXMubWF0Y2hlcyhzZWxlY3RvcikpIHtcbiAgICAgICAgcmV0dXJuIFtwcmV2aW91c11cbiAgICAgIH1cblxuICAgICAgcHJldmlvdXMgPSBwcmV2aW91cy5wcmV2aW91c0VsZW1lbnRTaWJsaW5nXG4gICAgfVxuXG4gICAgcmV0dXJuIFtdXG4gIH0sXG4gIC8vIFRPRE86IHRoaXMgaXMgbm93IHVudXNlZDsgcmVtb3ZlIGxhdGVyIGFsb25nIHdpdGggcHJldigpXG4gIG5leHQoZWxlbWVudCwgc2VsZWN0b3IpIHtcbiAgICBsZXQgbmV4dCA9IGVsZW1lbnQubmV4dEVsZW1lbnRTaWJsaW5nXG5cbiAgICB3aGlsZSAobmV4dCkge1xuICAgICAgaWYgKG5leHQubWF0Y2hlcyhzZWxlY3RvcikpIHtcbiAgICAgICAgcmV0dXJuIFtuZXh0XVxuICAgICAgfVxuXG4gICAgICBuZXh0ID0gbmV4dC5uZXh0RWxlbWVudFNpYmxpbmdcbiAgICB9XG5cbiAgICByZXR1cm4gW11cbiAgfSxcblxuICBmb2N1c2FibGVDaGlsZHJlbihlbGVtZW50KSB7XG4gICAgY29uc3QgZm9jdXNhYmxlcyA9IFtcbiAgICAgICdhJyxcbiAgICAgICdidXR0b24nLFxuICAgICAgJ2lucHV0JyxcbiAgICAgICd0ZXh0YXJlYScsXG4gICAgICAnc2VsZWN0JyxcbiAgICAgICdkZXRhaWxzJyxcbiAgICAgICdbdGFiaW5kZXhdJyxcbiAgICAgICdbY29udGVudGVkaXRhYmxlPVwidHJ1ZVwiXSdcbiAgICBdLm1hcChzZWxlY3RvciA9PiBgJHtzZWxlY3Rvcn06bm90KFt0YWJpbmRleF49XCItXCJdKWApLmpvaW4oJywnKVxuXG4gICAgcmV0dXJuIHRoaXMuZmluZChmb2N1c2FibGVzLCBlbGVtZW50KS5maWx0ZXIoZWwgPT4gIWlzRGlzYWJsZWQoZWwpICYmIGlzVmlzaWJsZShlbCkpXG4gIH0sXG5cbiAgZ2V0U2VsZWN0b3JGcm9tRWxlbWVudChlbGVtZW50KSB7XG4gICAgY29uc3Qgc2VsZWN0b3IgPSBnZXRTZWxlY3RvcihlbGVtZW50KVxuXG4gICAgaWYgKHNlbGVjdG9yKSB7XG4gICAgICByZXR1cm4gU2VsZWN0b3JFbmdpbmUuZmluZE9uZShzZWxlY3RvcikgPyBzZWxlY3RvciA6IG51bGxcbiAgICB9XG5cbiAgICByZXR1cm4gbnVsbFxuICB9LFxuXG4gIGdldEVsZW1lbnRGcm9tU2VsZWN0b3IoZWxlbWVudCkge1xuICAgIGNvbnN0IHNlbGVjdG9yID0gZ2V0U2VsZWN0b3IoZWxlbWVudClcblxuICAgIHJldHVybiBzZWxlY3RvciA/IFNlbGVjdG9yRW5naW5lLmZpbmRPbmUoc2VsZWN0b3IpIDogbnVsbFxuICB9LFxuXG4gIGdldE11bHRpcGxlRWxlbWVudHNGcm9tU2VsZWN0b3IoZWxlbWVudCkge1xuICAgIGNvbnN0IHNlbGVjdG9yID0gZ2V0U2VsZWN0b3IoZWxlbWVudClcblxuICAgIHJldHVybiBzZWxlY3RvciA/IFNlbGVjdG9yRW5naW5lLmZpbmQoc2VsZWN0b3IpIDogW11cbiAgfVxufVxuXG5leHBvcnQgZGVmYXVsdCBTZWxlY3RvckVuZ2luZVxuIiwgIi8qKlxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqIEJvb3RzdHJhcCB0YWIuanNcbiAqIExpY2Vuc2VkIHVuZGVyIE1JVCAoaHR0cHM6Ly9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwL2Jsb2IvbWFpbi9MSUNFTlNFKVxuICogLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cbiAqL1xuXG5pbXBvcnQgQmFzZUNvbXBvbmVudCBmcm9tICcuL2Jhc2UtY29tcG9uZW50LmpzJ1xuaW1wb3J0IEV2ZW50SGFuZGxlciBmcm9tICcuL2RvbS9ldmVudC1oYW5kbGVyLmpzJ1xuaW1wb3J0IFNlbGVjdG9yRW5naW5lIGZyb20gJy4vZG9tL3NlbGVjdG9yLWVuZ2luZS5qcydcbmltcG9ydCB7IGRlZmluZUpRdWVyeVBsdWdpbiwgZ2V0TmV4dEFjdGl2ZUVsZW1lbnQsIGlzRGlzYWJsZWQgfSBmcm9tICcuL3V0aWwvaW5kZXguanMnXG5cbi8qKlxuICogQ29uc3RhbnRzXG4gKi9cblxuY29uc3QgTkFNRSA9ICd0YWInXG5jb25zdCBEQVRBX0tFWSA9ICdicy50YWInXG5jb25zdCBFVkVOVF9LRVkgPSBgLiR7REFUQV9LRVl9YFxuXG5jb25zdCBFVkVOVF9ISURFID0gYGhpZGUke0VWRU5UX0tFWX1gXG5jb25zdCBFVkVOVF9ISURERU4gPSBgaGlkZGVuJHtFVkVOVF9LRVl9YFxuY29uc3QgRVZFTlRfU0hPVyA9IGBzaG93JHtFVkVOVF9LRVl9YFxuY29uc3QgRVZFTlRfU0hPV04gPSBgc2hvd24ke0VWRU5UX0tFWX1gXG5jb25zdCBFVkVOVF9DTElDS19EQVRBX0FQSSA9IGBjbGljayR7RVZFTlRfS0VZfWBcbmNvbnN0IEVWRU5UX0tFWURPV04gPSBga2V5ZG93biR7RVZFTlRfS0VZfWBcbmNvbnN0IEVWRU5UX0xPQURfREFUQV9BUEkgPSBgbG9hZCR7RVZFTlRfS0VZfWBcblxuY29uc3QgQVJST1dfTEVGVF9LRVkgPSAnQXJyb3dMZWZ0J1xuY29uc3QgQVJST1dfUklHSFRfS0VZID0gJ0Fycm93UmlnaHQnXG5jb25zdCBBUlJPV19VUF9LRVkgPSAnQXJyb3dVcCdcbmNvbnN0IEFSUk9XX0RPV05fS0VZID0gJ0Fycm93RG93bidcbmNvbnN0IEhPTUVfS0VZID0gJ0hvbWUnXG5jb25zdCBFTkRfS0VZID0gJ0VuZCdcblxuY29uc3QgQ0xBU1NfTkFNRV9BQ1RJVkUgPSAnYWN0aXZlJ1xuY29uc3QgQ0xBU1NfTkFNRV9GQURFID0gJ2ZhZGUnXG5jb25zdCBDTEFTU19OQU1FX1NIT1cgPSAnc2hvdydcbmNvbnN0IENMQVNTX0RST1BET1dOID0gJ2Ryb3Bkb3duJ1xuXG5jb25zdCBTRUxFQ1RPUl9EUk9QRE9XTl9UT0dHTEUgPSAnLmRyb3Bkb3duLXRvZ2dsZSdcbmNvbnN0IFNFTEVDVE9SX0RST1BET1dOX01FTlUgPSAnLmRyb3Bkb3duLW1lbnUnXG5jb25zdCBOT1RfU0VMRUNUT1JfRFJPUERPV05fVE9HR0xFID0gYDpub3QoJHtTRUxFQ1RPUl9EUk9QRE9XTl9UT0dHTEV9KWBcblxuY29uc3QgU0VMRUNUT1JfVEFCX1BBTkVMID0gJy5saXN0LWdyb3VwLCAubmF2LCBbcm9sZT1cInRhYmxpc3RcIl0nXG5jb25zdCBTRUxFQ1RPUl9PVVRFUiA9ICcubmF2LWl0ZW0sIC5saXN0LWdyb3VwLWl0ZW0nXG5jb25zdCBTRUxFQ1RPUl9JTk5FUiA9IGAubmF2LWxpbmske05PVF9TRUxFQ1RPUl9EUk9QRE9XTl9UT0dHTEV9LCAubGlzdC1ncm91cC1pdGVtJHtOT1RfU0VMRUNUT1JfRFJPUERPV05fVE9HR0xFfSwgW3JvbGU9XCJ0YWJcIl0ke05PVF9TRUxFQ1RPUl9EUk9QRE9XTl9UT0dHTEV9YFxuY29uc3QgU0VMRUNUT1JfREFUQV9UT0dHTEUgPSAnW2RhdGEtYnMtdG9nZ2xlPVwidGFiXCJdLCBbZGF0YS1icy10b2dnbGU9XCJwaWxsXCJdLCBbZGF0YS1icy10b2dnbGU9XCJsaXN0XCJdJyAvLyBUT0RPOiBjb3VsZCBvbmx5IGJlIGB0YWJgIGluIHY2XG5jb25zdCBTRUxFQ1RPUl9JTk5FUl9FTEVNID0gYCR7U0VMRUNUT1JfSU5ORVJ9LCAke1NFTEVDVE9SX0RBVEFfVE9HR0xFfWBcblxuY29uc3QgU0VMRUNUT1JfREFUQV9UT0dHTEVfQUNUSVZFID0gYC4ke0NMQVNTX05BTUVfQUNUSVZFfVtkYXRhLWJzLXRvZ2dsZT1cInRhYlwiXSwgLiR7Q0xBU1NfTkFNRV9BQ1RJVkV9W2RhdGEtYnMtdG9nZ2xlPVwicGlsbFwiXSwgLiR7Q0xBU1NfTkFNRV9BQ1RJVkV9W2RhdGEtYnMtdG9nZ2xlPVwibGlzdFwiXWBcblxuLyoqXG4gKiBDbGFzcyBkZWZpbml0aW9uXG4gKi9cblxuY2xhc3MgVGFiIGV4dGVuZHMgQmFzZUNvbXBvbmVudCB7XG4gIGNvbnN0cnVjdG9yKGVsZW1lbnQpIHtcbiAgICBzdXBlcihlbGVtZW50KVxuICAgIHRoaXMuX3BhcmVudCA9IHRoaXMuX2VsZW1lbnQuY2xvc2VzdChTRUxFQ1RPUl9UQUJfUEFORUwpXG5cbiAgICBpZiAoIXRoaXMuX3BhcmVudCkge1xuICAgICAgcmV0dXJuXG4gICAgICAvLyBUT0RPOiBzaG91bGQgdGhyb3cgZXhjZXB0aW9uIGluIHY2XG4gICAgICAvLyB0aHJvdyBuZXcgVHlwZUVycm9yKGAke2VsZW1lbnQub3V0ZXJIVE1MfSBoYXMgbm90IGEgdmFsaWQgcGFyZW50ICR7U0VMRUNUT1JfSU5ORVJfRUxFTX1gKVxuICAgIH1cblxuICAgIC8vIFNldCB1cCBpbml0aWFsIGFyaWEgYXR0cmlidXRlc1xuICAgIHRoaXMuX3NldEluaXRpYWxBdHRyaWJ1dGVzKHRoaXMuX3BhcmVudCwgdGhpcy5fZ2V0Q2hpbGRyZW4oKSlcblxuICAgIEV2ZW50SGFuZGxlci5vbih0aGlzLl9lbGVtZW50LCBFVkVOVF9LRVlET1dOLCBldmVudCA9PiB0aGlzLl9rZXlkb3duKGV2ZW50KSlcbiAgfVxuXG4gIC8vIEdldHRlcnNcbiAgc3RhdGljIGdldCBOQU1FKCkge1xuICAgIHJldHVybiBOQU1FXG4gIH1cblxuICAvLyBQdWJsaWNcbiAgc2hvdygpIHsgLy8gU2hvd3MgdGhpcyBlbGVtIGFuZCBkZWFjdGl2YXRlIHRoZSBhY3RpdmUgc2libGluZyBpZiBleGlzdHNcbiAgICBjb25zdCBpbm5lckVsZW0gPSB0aGlzLl9lbGVtZW50XG4gICAgaWYgKHRoaXMuX2VsZW1Jc0FjdGl2ZShpbm5lckVsZW0pKSB7XG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICAvLyBTZWFyY2ggZm9yIGFjdGl2ZSB0YWIgb24gc2FtZSBwYXJlbnQgdG8gZGVhY3RpdmF0ZSBpdFxuICAgIGNvbnN0IGFjdGl2ZSA9IHRoaXMuX2dldEFjdGl2ZUVsZW0oKVxuXG4gICAgY29uc3QgaGlkZUV2ZW50ID0gYWN0aXZlID9cbiAgICAgIEV2ZW50SGFuZGxlci50cmlnZ2VyKGFjdGl2ZSwgRVZFTlRfSElERSwgeyByZWxhdGVkVGFyZ2V0OiBpbm5lckVsZW0gfSkgOlxuICAgICAgbnVsbFxuXG4gICAgY29uc3Qgc2hvd0V2ZW50ID0gRXZlbnRIYW5kbGVyLnRyaWdnZXIoaW5uZXJFbGVtLCBFVkVOVF9TSE9XLCB7IHJlbGF0ZWRUYXJnZXQ6IGFjdGl2ZSB9KVxuXG4gICAgaWYgKHNob3dFdmVudC5kZWZhdWx0UHJldmVudGVkIHx8IChoaWRlRXZlbnQgJiYgaGlkZUV2ZW50LmRlZmF1bHRQcmV2ZW50ZWQpKSB7XG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICB0aGlzLl9kZWFjdGl2YXRlKGFjdGl2ZSwgaW5uZXJFbGVtKVxuICAgIHRoaXMuX2FjdGl2YXRlKGlubmVyRWxlbSwgYWN0aXZlKVxuICB9XG5cbiAgLy8gUHJpdmF0ZVxuICBfYWN0aXZhdGUoZWxlbWVudCwgcmVsYXRlZEVsZW0pIHtcbiAgICBpZiAoIWVsZW1lbnQpIHtcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIGVsZW1lbnQuY2xhc3NMaXN0LmFkZChDTEFTU19OQU1FX0FDVElWRSlcblxuICAgIHRoaXMuX2FjdGl2YXRlKFNlbGVjdG9yRW5naW5lLmdldEVsZW1lbnRGcm9tU2VsZWN0b3IoZWxlbWVudCkpIC8vIFNlYXJjaCBhbmQgYWN0aXZhdGUvc2hvdyB0aGUgcHJvcGVyIHNlY3Rpb25cblxuICAgIGNvbnN0IGNvbXBsZXRlID0gKCkgPT4ge1xuICAgICAgaWYgKGVsZW1lbnQuZ2V0QXR0cmlidXRlKCdyb2xlJykgIT09ICd0YWInKSB7XG4gICAgICAgIGVsZW1lbnQuY2xhc3NMaXN0LmFkZChDTEFTU19OQU1FX1NIT1cpXG4gICAgICAgIHJldHVyblxuICAgICAgfVxuXG4gICAgICBlbGVtZW50LnJlbW92ZUF0dHJpYnV0ZSgndGFiaW5kZXgnKVxuICAgICAgZWxlbWVudC5zZXRBdHRyaWJ1dGUoJ2FyaWEtc2VsZWN0ZWQnLCB0cnVlKVxuICAgICAgdGhpcy5fdG9nZ2xlRHJvcERvd24oZWxlbWVudCwgdHJ1ZSlcbiAgICAgIEV2ZW50SGFuZGxlci50cmlnZ2VyKGVsZW1lbnQsIEVWRU5UX1NIT1dOLCB7XG4gICAgICAgIHJlbGF0ZWRUYXJnZXQ6IHJlbGF0ZWRFbGVtXG4gICAgICB9KVxuICAgIH1cblxuICAgIHRoaXMuX3F1ZXVlQ2FsbGJhY2soY29tcGxldGUsIGVsZW1lbnQsIGVsZW1lbnQuY2xhc3NMaXN0LmNvbnRhaW5zKENMQVNTX05BTUVfRkFERSkpXG4gIH1cblxuICBfZGVhY3RpdmF0ZShlbGVtZW50LCByZWxhdGVkRWxlbSkge1xuICAgIGlmICghZWxlbWVudCkge1xuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgZWxlbWVudC5jbGFzc0xpc3QucmVtb3ZlKENMQVNTX05BTUVfQUNUSVZFKVxuICAgIGVsZW1lbnQuYmx1cigpXG5cbiAgICB0aGlzLl9kZWFjdGl2YXRlKFNlbGVjdG9yRW5naW5lLmdldEVsZW1lbnRGcm9tU2VsZWN0b3IoZWxlbWVudCkpIC8vIFNlYXJjaCBhbmQgZGVhY3RpdmF0ZSB0aGUgc2hvd24gc2VjdGlvbiB0b29cblxuICAgIGNvbnN0IGNvbXBsZXRlID0gKCkgPT4ge1xuICAgICAgaWYgKGVsZW1lbnQuZ2V0QXR0cmlidXRlKCdyb2xlJykgIT09ICd0YWInKSB7XG4gICAgICAgIGVsZW1lbnQuY2xhc3NMaXN0LnJlbW92ZShDTEFTU19OQU1FX1NIT1cpXG4gICAgICAgIHJldHVyblxuICAgICAgfVxuXG4gICAgICBlbGVtZW50LnNldEF0dHJpYnV0ZSgnYXJpYS1zZWxlY3RlZCcsIGZhbHNlKVxuICAgICAgZWxlbWVudC5zZXRBdHRyaWJ1dGUoJ3RhYmluZGV4JywgJy0xJylcbiAgICAgIHRoaXMuX3RvZ2dsZURyb3BEb3duKGVsZW1lbnQsIGZhbHNlKVxuICAgICAgRXZlbnRIYW5kbGVyLnRyaWdnZXIoZWxlbWVudCwgRVZFTlRfSElEREVOLCB7IHJlbGF0ZWRUYXJnZXQ6IHJlbGF0ZWRFbGVtIH0pXG4gICAgfVxuXG4gICAgdGhpcy5fcXVldWVDYWxsYmFjayhjb21wbGV0ZSwgZWxlbWVudCwgZWxlbWVudC5jbGFzc0xpc3QuY29udGFpbnMoQ0xBU1NfTkFNRV9GQURFKSlcbiAgfVxuXG4gIF9rZXlkb3duKGV2ZW50KSB7XG4gICAgaWYgKCEoW0FSUk9XX0xFRlRfS0VZLCBBUlJPV19SSUdIVF9LRVksIEFSUk9XX1VQX0tFWSwgQVJST1dfRE9XTl9LRVksIEhPTUVfS0VZLCBFTkRfS0VZXS5pbmNsdWRlcyhldmVudC5rZXkpKSkge1xuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCkvLyBzdG9wUHJvcGFnYXRpb24vcHJldmVudERlZmF1bHQgYm90aCBhZGRlZCB0byBzdXBwb3J0IHVwL2Rvd24ga2V5cyB3aXRob3V0IHNjcm9sbGluZyB0aGUgcGFnZVxuICAgIGV2ZW50LnByZXZlbnREZWZhdWx0KClcblxuICAgIGNvbnN0IGNoaWxkcmVuID0gdGhpcy5fZ2V0Q2hpbGRyZW4oKS5maWx0ZXIoZWxlbWVudCA9PiAhaXNEaXNhYmxlZChlbGVtZW50KSlcbiAgICBsZXQgbmV4dEFjdGl2ZUVsZW1lbnRcblxuICAgIGlmIChbSE9NRV9LRVksIEVORF9LRVldLmluY2x1ZGVzKGV2ZW50LmtleSkpIHtcbiAgICAgIG5leHRBY3RpdmVFbGVtZW50ID0gY2hpbGRyZW5bZXZlbnQua2V5ID09PSBIT01FX0tFWSA/IDAgOiBjaGlsZHJlbi5sZW5ndGggLSAxXVxuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBpc05leHQgPSBbQVJST1dfUklHSFRfS0VZLCBBUlJPV19ET1dOX0tFWV0uaW5jbHVkZXMoZXZlbnQua2V5KVxuICAgICAgbmV4dEFjdGl2ZUVsZW1lbnQgPSBnZXROZXh0QWN0aXZlRWxlbWVudChjaGlsZHJlbiwgZXZlbnQudGFyZ2V0LCBpc05leHQsIHRydWUpXG4gICAgfVxuXG4gICAgaWYgKG5leHRBY3RpdmVFbGVtZW50KSB7XG4gICAgICBuZXh0QWN0aXZlRWxlbWVudC5mb2N1cyh7IHByZXZlbnRTY3JvbGw6IHRydWUgfSlcbiAgICAgIFRhYi5nZXRPckNyZWF0ZUluc3RhbmNlKG5leHRBY3RpdmVFbGVtZW50KS5zaG93KClcbiAgICB9XG4gIH1cblxuICBfZ2V0Q2hpbGRyZW4oKSB7IC8vIGNvbGxlY3Rpb24gb2YgaW5uZXIgZWxlbWVudHNcbiAgICByZXR1cm4gU2VsZWN0b3JFbmdpbmUuZmluZChTRUxFQ1RPUl9JTk5FUl9FTEVNLCB0aGlzLl9wYXJlbnQpXG4gIH1cblxuICBfZ2V0QWN0aXZlRWxlbSgpIHtcbiAgICByZXR1cm4gdGhpcy5fZ2V0Q2hpbGRyZW4oKS5maW5kKGNoaWxkID0+IHRoaXMuX2VsZW1Jc0FjdGl2ZShjaGlsZCkpIHx8IG51bGxcbiAgfVxuXG4gIF9zZXRJbml0aWFsQXR0cmlidXRlcyhwYXJlbnQsIGNoaWxkcmVuKSB7XG4gICAgdGhpcy5fc2V0QXR0cmlidXRlSWZOb3RFeGlzdHMocGFyZW50LCAncm9sZScsICd0YWJsaXN0JylcblxuICAgIGZvciAoY29uc3QgY2hpbGQgb2YgY2hpbGRyZW4pIHtcbiAgICAgIHRoaXMuX3NldEluaXRpYWxBdHRyaWJ1dGVzT25DaGlsZChjaGlsZClcbiAgICB9XG4gIH1cblxuICBfc2V0SW5pdGlhbEF0dHJpYnV0ZXNPbkNoaWxkKGNoaWxkKSB7XG4gICAgY2hpbGQgPSB0aGlzLl9nZXRJbm5lckVsZW1lbnQoY2hpbGQpXG4gICAgY29uc3QgaXNBY3RpdmUgPSB0aGlzLl9lbGVtSXNBY3RpdmUoY2hpbGQpXG4gICAgY29uc3Qgb3V0ZXJFbGVtID0gdGhpcy5fZ2V0T3V0ZXJFbGVtZW50KGNoaWxkKVxuICAgIGNoaWxkLnNldEF0dHJpYnV0ZSgnYXJpYS1zZWxlY3RlZCcsIGlzQWN0aXZlKVxuXG4gICAgaWYgKG91dGVyRWxlbSAhPT0gY2hpbGQpIHtcbiAgICAgIHRoaXMuX3NldEF0dHJpYnV0ZUlmTm90RXhpc3RzKG91dGVyRWxlbSwgJ3JvbGUnLCAncHJlc2VudGF0aW9uJylcbiAgICB9XG5cbiAgICBpZiAoIWlzQWN0aXZlKSB7XG4gICAgICBjaGlsZC5zZXRBdHRyaWJ1dGUoJ3RhYmluZGV4JywgJy0xJylcbiAgICB9XG5cbiAgICB0aGlzLl9zZXRBdHRyaWJ1dGVJZk5vdEV4aXN0cyhjaGlsZCwgJ3JvbGUnLCAndGFiJylcblxuICAgIC8vIHNldCBhdHRyaWJ1dGVzIHRvIHRoZSByZWxhdGVkIHBhbmVsIHRvb1xuICAgIHRoaXMuX3NldEluaXRpYWxBdHRyaWJ1dGVzT25UYXJnZXRQYW5lbChjaGlsZClcbiAgfVxuXG4gIF9zZXRJbml0aWFsQXR0cmlidXRlc09uVGFyZ2V0UGFuZWwoY2hpbGQpIHtcbiAgICBjb25zdCB0YXJnZXQgPSBTZWxlY3RvckVuZ2luZS5nZXRFbGVtZW50RnJvbVNlbGVjdG9yKGNoaWxkKVxuXG4gICAgaWYgKCF0YXJnZXQpIHtcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIHRoaXMuX3NldEF0dHJpYnV0ZUlmTm90RXhpc3RzKHRhcmdldCwgJ3JvbGUnLCAndGFicGFuZWwnKVxuXG4gICAgaWYgKGNoaWxkLmlkKSB7XG4gICAgICB0aGlzLl9zZXRBdHRyaWJ1dGVJZk5vdEV4aXN0cyh0YXJnZXQsICdhcmlhLWxhYmVsbGVkYnknLCBgJHtjaGlsZC5pZH1gKVxuICAgIH1cbiAgfVxuXG4gIF90b2dnbGVEcm9wRG93bihlbGVtZW50LCBvcGVuKSB7XG4gICAgY29uc3Qgb3V0ZXJFbGVtID0gdGhpcy5fZ2V0T3V0ZXJFbGVtZW50KGVsZW1lbnQpXG4gICAgaWYgKCFvdXRlckVsZW0uY2xhc3NMaXN0LmNvbnRhaW5zKENMQVNTX0RST1BET1dOKSkge1xuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgY29uc3QgdG9nZ2xlID0gKHNlbGVjdG9yLCBjbGFzc05hbWUpID0+IHtcbiAgICAgIGNvbnN0IGVsZW1lbnQgPSBTZWxlY3RvckVuZ2luZS5maW5kT25lKHNlbGVjdG9yLCBvdXRlckVsZW0pXG4gICAgICBpZiAoZWxlbWVudCkge1xuICAgICAgICBlbGVtZW50LmNsYXNzTGlzdC50b2dnbGUoY2xhc3NOYW1lLCBvcGVuKVxuICAgICAgfVxuICAgIH1cblxuICAgIHRvZ2dsZShTRUxFQ1RPUl9EUk9QRE9XTl9UT0dHTEUsIENMQVNTX05BTUVfQUNUSVZFKVxuICAgIHRvZ2dsZShTRUxFQ1RPUl9EUk9QRE9XTl9NRU5VLCBDTEFTU19OQU1FX1NIT1cpXG4gICAgb3V0ZXJFbGVtLnNldEF0dHJpYnV0ZSgnYXJpYS1leHBhbmRlZCcsIG9wZW4pXG4gIH1cblxuICBfc2V0QXR0cmlidXRlSWZOb3RFeGlzdHMoZWxlbWVudCwgYXR0cmlidXRlLCB2YWx1ZSkge1xuICAgIGlmICghZWxlbWVudC5oYXNBdHRyaWJ1dGUoYXR0cmlidXRlKSkge1xuICAgICAgZWxlbWVudC5zZXRBdHRyaWJ1dGUoYXR0cmlidXRlLCB2YWx1ZSlcbiAgICB9XG4gIH1cblxuICBfZWxlbUlzQWN0aXZlKGVsZW0pIHtcbiAgICByZXR1cm4gZWxlbS5jbGFzc0xpc3QuY29udGFpbnMoQ0xBU1NfTkFNRV9BQ1RJVkUpXG4gIH1cblxuICAvLyBUcnkgdG8gZ2V0IHRoZSBpbm5lciBlbGVtZW50ICh1c3VhbGx5IHRoZSAubmF2LWxpbmspXG4gIF9nZXRJbm5lckVsZW1lbnQoZWxlbSkge1xuICAgIHJldHVybiBlbGVtLm1hdGNoZXMoU0VMRUNUT1JfSU5ORVJfRUxFTSkgPyBlbGVtIDogU2VsZWN0b3JFbmdpbmUuZmluZE9uZShTRUxFQ1RPUl9JTk5FUl9FTEVNLCBlbGVtKVxuICB9XG5cbiAgLy8gVHJ5IHRvIGdldCB0aGUgb3V0ZXIgZWxlbWVudCAodXN1YWxseSB0aGUgLm5hdi1pdGVtKVxuICBfZ2V0T3V0ZXJFbGVtZW50KGVsZW0pIHtcbiAgICByZXR1cm4gZWxlbS5jbG9zZXN0KFNFTEVDVE9SX09VVEVSKSB8fCBlbGVtXG4gIH1cblxuICAvLyBTdGF0aWNcbiAgc3RhdGljIGpRdWVyeUludGVyZmFjZShjb25maWcpIHtcbiAgICByZXR1cm4gdGhpcy5lYWNoKGZ1bmN0aW9uICgpIHtcbiAgICAgIGNvbnN0IGRhdGEgPSBUYWIuZ2V0T3JDcmVhdGVJbnN0YW5jZSh0aGlzKVxuXG4gICAgICBpZiAodHlwZW9mIGNvbmZpZyAhPT0gJ3N0cmluZycpIHtcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG5cbiAgICAgIGlmIChkYXRhW2NvbmZpZ10gPT09IHVuZGVmaW5lZCB8fCBjb25maWcuc3RhcnRzV2l0aCgnXycpIHx8IGNvbmZpZyA9PT0gJ2NvbnN0cnVjdG9yJykge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBObyBtZXRob2QgbmFtZWQgXCIke2NvbmZpZ31cImApXG4gICAgICB9XG5cbiAgICAgIGRhdGFbY29uZmlnXSgpXG4gICAgfSlcbiAgfVxufVxuXG4vKipcbiAqIERhdGEgQVBJIGltcGxlbWVudGF0aW9uXG4gKi9cblxuRXZlbnRIYW5kbGVyLm9uKGRvY3VtZW50LCBFVkVOVF9DTElDS19EQVRBX0FQSSwgU0VMRUNUT1JfREFUQV9UT0dHTEUsIGZ1bmN0aW9uIChldmVudCkge1xuICBpZiAoWydBJywgJ0FSRUEnXS5pbmNsdWRlcyh0aGlzLnRhZ05hbWUpKSB7XG4gICAgZXZlbnQucHJldmVudERlZmF1bHQoKVxuICB9XG5cbiAgaWYgKGlzRGlzYWJsZWQodGhpcykpIHtcbiAgICByZXR1cm5cbiAgfVxuXG4gIFRhYi5nZXRPckNyZWF0ZUluc3RhbmNlKHRoaXMpLnNob3coKVxufSlcblxuLyoqXG4gKiBJbml0aWFsaXplIG9uIGZvY3VzXG4gKi9cbkV2ZW50SGFuZGxlci5vbih3aW5kb3csIEVWRU5UX0xPQURfREFUQV9BUEksICgpID0+IHtcbiAgZm9yIChjb25zdCBlbGVtZW50IG9mIFNlbGVjdG9yRW5naW5lLmZpbmQoU0VMRUNUT1JfREFUQV9UT0dHTEVfQUNUSVZFKSkge1xuICAgIFRhYi5nZXRPckNyZWF0ZUluc3RhbmNlKGVsZW1lbnQpXG4gIH1cbn0pXG4vKipcbiAqIGpRdWVyeVxuICovXG5cbmRlZmluZUpRdWVyeVBsdWdpbihUYWIpXG5cbmV4cG9ydCBkZWZhdWx0IFRhYlxuIiwgIi8vIEltcG9ydCB0aGUgQm9vdHN0cmFwIGNvbXBvbmVudHMgd2Ugd2FudCB0byB1c2UuXG4vLyBTZWUgaHR0cHM6Ly9naXRodWIuY29tL3R3YnMvYm9vdHN0cmFwL2Jsb2IvbWFpbi9qcy9pbmRleC51bWQuanNcbmltcG9ydCBUYWIgZnJvbSBcIi9qcy9ib290c3RyYXAvc3JjL3RhYlwiO1xuXG5leHBvcnQgZGVmYXVsdCB7XG4gICAgVGFiXG59Il0sCiAgIm1hcHBpbmdzIjogIjs7QUFXQSxNQUFNLGFBQWEsb0JBQUksSUFBSTtBQUUzQixNQUFPLGVBQVE7QUFBQSxJQUNiLElBQUksU0FBUyxLQUFLLFVBQVU7QUFDMUIsVUFBSSxDQUFDLFdBQVcsSUFBSSxPQUFPLEdBQUc7QUFDNUIsbUJBQVcsSUFBSSxTQUFTLG9CQUFJLElBQUksQ0FBQztBQUFBLE1BQ25DO0FBRUEsWUFBTSxjQUFjLFdBQVcsSUFBSSxPQUFPO0FBSTFDLFVBQUksQ0FBQyxZQUFZLElBQUksR0FBRyxLQUFLLFlBQVksU0FBUyxHQUFHO0FBRW5ELGdCQUFRLE1BQU0sK0VBQStFLE1BQU0sS0FBSyxZQUFZLEtBQUssQ0FBQyxFQUFFLENBQUMsQ0FBQyxHQUFHO0FBQ2pJO0FBQUEsTUFDRjtBQUVBLGtCQUFZLElBQUksS0FBSyxRQUFRO0FBQUEsSUFDL0I7QUFBQSxJQUVBLElBQUksU0FBUyxLQUFLO0FBQ2hCLFVBQUksV0FBVyxJQUFJLE9BQU8sR0FBRztBQUMzQixlQUFPLFdBQVcsSUFBSSxPQUFPLEVBQUUsSUFBSSxHQUFHLEtBQUs7QUFBQSxNQUM3QztBQUVBLGFBQU87QUFBQSxJQUNUO0FBQUEsSUFFQSxPQUFPLFNBQVMsS0FBSztBQUNuQixVQUFJLENBQUMsV0FBVyxJQUFJLE9BQU8sR0FBRztBQUM1QjtBQUFBLE1BQ0Y7QUFFQSxZQUFNLGNBQWMsV0FBVyxJQUFJLE9BQU87QUFFMUMs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
