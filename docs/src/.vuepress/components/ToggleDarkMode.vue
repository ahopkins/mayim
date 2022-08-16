<template>
  <div class="dark-mode-widget">
    <input type="checkbox" id="theme-toggle" @click="toggleDarkTheme" />
    <label for="theme-toggle"><span>&nbsp;</span></label>
  </div>
</template>

<script>
const prefersDarkScheme = window.matchMedia("(prefers-color-scheme: dark)");
export default {
  mounted() {
    this.checkUserPreference();
  },
  methods: {
    toggleDarkTheme() {
      const body = document.body;
      body.classList.toggle("dark-mode");
      if (body.classList.contains("dark-mode")) {
        localStorage.setItem("dark-theme", true);
        this.notify();
      } else {
        body.classList.remove("dark-mode");
        setTimeout(() => {
          localStorage.setItem("dark-theme", false);
          this.notify();
        }, 100);
      }
    },
    checkUserPreference() {
      if (localStorage.getItem("dark-theme") === null) {
        localStorage.setItem("dark-theme", prefersDarkScheme.matches);
      }
      const isDarkMode =
        JSON.parse(localStorage.getItem("dark-theme")) === true;
      this.notify();
      if (isDarkMode) {
        document.body.classList.add("dark-mode");
        document.getElementById("theme-toggle").checked = true;
        localStorage.setItem("dark-theme", "true");
      }
    },
    notify() {
      const isDarkMode =
        JSON.parse(localStorage.getItem("dark-theme")) === true;
      this.$emit("darkmode", isDarkMode);
    },
  },
};
</script>

<style>
.dark-mode-widget {
  display: table;
  margin: auto 0 auto 1em;
}
#theme-toggle {
  display: none;
}
#theme-toggle + label {
  font-size: 1rem;
  display: flex;
  width: 3em;
  border-radius: 1em;
  background-size: auto 4em;
  background-position: bottom;
  background-image: linear-gradient(
    180deg,
    #111111 0%,
    #111154 19%,
    #7092a8 66%,
    #c7f0f5 100%
  );
  transition: 0.2s;
  overflow: hidden;
}
#theme-toggle + label span {
  background: #fffad8;
  border-radius: 50%;
  height: 1.5em;
  width: 1.5em;
  transform: translateX(-0.125em) scale(0.65);
  transition: 0.2s;
  cursor: pointer;
  box-shadow: 0 0 0.25em 0.0625em #fbee8d, 0 0 2em 0 #ffeb3b,
    inset -0.25em -0.25em 0 0 #fbee8e,
    inset -0.3125em -0.3125em 0 0.625em #fff5b2;
  margin-top: -0.125em;
}
#theme-toggle:checked {
  font-size: 5rem;
}
#theme-toggle:checked + label {
  background-position: top;
}
#theme-toggle:checked + label span {
  background: transparent;
  transform: translateX(calc(100%)) scale(0.65);
  box-shadow: inset -0.1875em -0.1875em 0 0 #fbe7ef,
    inset -0.5625em -0.5625em 0 0 #fffff7;
}
@media (max-width: 719px) {
  .dark-mode-widget {
    display: none;
  }
}
</style>
