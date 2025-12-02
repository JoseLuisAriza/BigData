// Pequeño script para hacer que los mensajes flash desaparezcan solos.

document.addEventListener("DOMContentLoaded", function () {
  const alerts = document.querySelectorAll(".alert");
  alerts.forEach((alert) => {
    setTimeout(() => {
      alert.classList.add("show");
    }, 50);

    setTimeout(() => {
      alert.classList.remove("show");
      alert.classList.add("fade");
    }, 4000);
  });
});
