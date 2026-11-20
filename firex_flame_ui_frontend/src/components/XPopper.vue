<template>
  <span>
    <span ref="popperWrapper" v-show="!disabled && showPopper">
      <slot></slot>
    </span>
    <slot name="reference"></slot>
  </span>
</template>

<script>
import {
  computePosition, flip, shift, arrow, autoUpdate,
} from '@floating-ui/dom';

/*
 * Replaces vue-popperjs, which has no Vue 3 build. Deliberately reproduces that
 * component's DOM structure and behaviour: the default slot's root element (the
 * one the caller gives class="popper") is the positioned element, an arrow div is
 * appended to it, and the x-placement attribute drives the placement-specific CSS
 * below. That stylesheet is vue-popperjs/dist/vue-popper.css verbatim, so all
 * existing .popper styling in callers keeps applying unchanged.
 */
export default {
  name: 'XPopper',
  props: {
    trigger: {
      type: String,
      default: 'hover',
      validator: value => ['click', 'hover'].indexOf(value) > -1,
    },
    disabled: { type: Boolean, default: false },
    delayOnMouseOver: { type: Number, default: 10 },
    delayOnMouseOut: { type: Number, default: 10 },
    visibleArrow: { type: Boolean, default: true },
    options: { type: Object, default: () => ({}) },
  },
  emits: ['show', 'hide'],
  data() {
    return {
      showPopper: false,
    };
  },
  watch: {
    showPopper(value) {
      if (value) {
        this.$emit('show', this);
        this.startAutoUpdate();
      } else {
        this.stopAutoUpdate();
        this.$emit('hide', this);
      }
    },
    disabled(value) {
      if (value) {
        this.showPopper = false;
      }
    },
  },
  mounted() {
    this.listeners = [];
    // The reference slot renders immediately after the popper wrapper, and the popper
    // itself is the default slot's root element.
    this.referenceElm = this.$refs.popperWrapper.nextElementSibling;
    this.popperElm = this.$refs.popperWrapper.firstElementChild;

    if (this.visibleArrow && this.popperElm) {
      this.appendArrow();
    }

    if (this.trigger === 'click') {
      this.addListener(this.referenceElm, 'click', this.doToggle);
      this.addListener(document, 'click', this.handleDocumentClick);
    } else {
      ['mouseover', 'focus'].forEach((e) => {
        this.addListener(this.referenceElm, e, this.onMouseOver);
        this.addListener(this.popperElm, e, this.onMouseOver);
      });
      ['mouseout', 'blur'].forEach((e) => {
        this.addListener(this.referenceElm, e, this.onMouseOut);
        this.addListener(this.popperElm, e, this.onMouseOut);
      });
    }
  },
  beforeUnmount() {
    clearTimeout(this.showTimer);
    this.stopAutoUpdate();
    this.listeners.forEach(([el, event, handler]) => el.removeEventListener(event, handler));
    this.listeners = [];
  },
  methods: {
    addListener(element, event, handler) {
      if (!element) {
        return;
      }
      element.addEventListener(event, handler, false);
      this.listeners.push([element, event, handler]);
    },

    appendArrow() {
      const arrowElm = document.createElement('div');
      arrowElm.setAttribute('x-arrow', '');
      arrowElm.className = 'popper__arrow';
      this.popperElm.appendChild(arrowElm);
      this.arrowElm = arrowElm;
    },

    doToggle() {
      this.showPopper = !this.showPopper;
    },

    handleDocumentClick(e) {
      if (!this.$el || this.$el.contains(e.target)) {
        return;
      }
      this.showPopper = false;
    },

    onMouseOver() {
      clearTimeout(this.showTimer);
      this.showTimer = setTimeout(() => { this.showPopper = true; }, this.delayOnMouseOver);
    },

    onMouseOut() {
      clearTimeout(this.showTimer);
      this.showTimer = setTimeout(() => { this.showPopper = false; }, this.delayOnMouseOut);
    },

    startAutoUpdate() {
      this.stopAutoUpdate();
      // Wait for v-show to un-hide the popper, otherwise it can't be measured.
      this.$nextTick(() => {
        if (!this.showPopper || !this.referenceElm || !this.popperElm) {
          return;
        }
        this.cleanupAutoUpdate = autoUpdate(
          this.referenceElm, this.popperElm, this.updatePosition,
        );
      });
    },

    stopAutoUpdate() {
      if (this.cleanupAutoUpdate) {
        this.cleanupAutoUpdate();
        this.cleanupAutoUpdate = null;
      }
    },

    updatePosition() {
      const middleware = [flip(), shift({ padding: 5 })];
      if (this.arrowElm) {
        middleware.push(arrow({ element: this.arrowElm, padding: 5 }));
      }
      computePosition(this.referenceElm, this.popperElm, {
        placement: this.options.placement || 'bottom',
        strategy: 'absolute',
        middleware,
      }).then(({
        x, y, placement, middlewareData,
      }) => {
        this.popperElm.style.left = `${x}px`;
        this.popperElm.style.top = `${y}px`;
        this.popperElm.setAttribute('x-placement', placement);

        if (this.arrowElm && middlewareData.arrow) {
          // Only the cross-axis offset is set; the other axis comes from the stylesheet.
          const arrowData = middlewareData.arrow;
          this.arrowElm.style.left = arrowData.x == null ? '' : `${arrowData.x}px`;
          this.arrowElm.style.top = arrowData.y == null ? '' : `${arrowData.y}px`;
        }
      });
    },
  },
};
</script>

<style>
  .popper {
    width: auto;
    background-color: #fafafa;
    color: #212121;
    text-align: center;
    padding: 2px;
    display: inline-block;
    border-radius: 3px;
    position: absolute;
    font-size: 14px;
    font-weight: normal;
    border: 1px #ebebeb solid;
    z-index: 200000;
    -moz-box-shadow: rgb(58, 58, 58) 0 0 6px 0;
    -webkit-box-shadow: rgb(58, 58, 58) 0 0 6px 0;
    box-shadow: rgb(58, 58, 58) 0 0 6px 0;
  }

  .popper .popper__arrow {
    width: 0;
    height: 0;
    border-style: solid;
    position: absolute;
    margin: 5px;
  }

  .popper[x-placement^="top"] {
    margin-bottom: 5px;
  }

  .popper[x-placement^="top"] .popper__arrow {
    border-width: 5px 5px 0 5px;
    border-color: #fafafa transparent transparent transparent;
    bottom: -5px;
    left: calc(50% - 5px);
    margin-top: 0;
    margin-bottom: 0;
  }

  .popper[x-placement^="bottom"] {
    margin-top: 5px;
  }

  .popper[x-placement^="bottom"] .popper__arrow {
    border-width: 0 5px 5px 5px;
    border-color: transparent transparent #fafafa transparent;
    top: -5px;
    left: calc(50% - 5px);
    margin-top: 0;
    margin-bottom: 0;
  }

  .popper[x-placement^="right"] {
    margin-left: 5px;
  }

  .popper[x-placement^="right"] .popper__arrow {
    border-width: 5px 5px 5px 0;
    border-color: transparent #fafafa transparent transparent;
    left: -5px;
    top: calc(50% - 5px);
    margin-left: 0;
    margin-right: 0;
  }

  .popper[x-placement^="left"] {
    margin-right: 5px;
  }

  .popper[x-placement^="left"] .popper__arrow {
    border-width: 5px 0 5px 5px;
    border-color: transparent transparent transparent #fafafa;
    right: -5px;
    top: calc(50% - 5px);
    margin-left: 0;
    margin-right: 0;
  }
</style>
