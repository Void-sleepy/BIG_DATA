document.getElementById('loginForm').addEventListener('submit', function (e) {
  e.preventDefault();

  const email = document.getElementById('email').value;
  const password = document.getElementById('password').value;

  const validEmail = "user@example.com";
  const validPassword = "1234";

  if (email === validEmail && password === validPassword) {
    window.location.href = "Home.html";
  } else {
    document.getElementById('error-msg').innerText = "Invalid email or password!";
  }
});
