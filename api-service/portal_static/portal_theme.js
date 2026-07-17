(function () {
  var theme = "light";
  try {
    var stored = window.localStorage.getItem("sndbx.theme");
    if (stored === "dark" || stored === "light") theme = stored;
  } catch (_) {
    theme = "light";
  }
  document.documentElement.setAttribute("data-theme", theme);
  document.documentElement.style.colorScheme = theme;
})();
