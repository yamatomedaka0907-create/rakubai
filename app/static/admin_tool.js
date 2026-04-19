(function () {
  const button = document.getElementById("toolMenuButton");
  const sidebar = document.getElementById("toolSidebar");
  if (!button || !sidebar) return;
  button.addEventListener("click", function () {
    sidebar.classList.toggle("is-open");
    document.body.classList.toggle("tool-nav-open", sidebar.classList.contains("is-open"));
  });
  document.addEventListener("click", function (event) {
    if (window.innerWidth > 980) return;
    const target = event.target;
    if (!sidebar.contains(target) && !button.contains(target)) {
      sidebar.classList.remove("is-open");
      document.body.classList.remove("tool-nav-open");
    }
  });
})();
