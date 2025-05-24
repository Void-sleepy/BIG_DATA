document.getElementById('SignUpForm').addEventListener('submit', function (e) {
    e.preventDefault();
  
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
    const cpassword = document.getElementById('cpassword').value;
  
    if (password !== cpassword) {
      document.getElementById('error-msg').innerText = "Passwords do not match!";
      return;
    }
  
    // Simulated success case
    if (email && password) {
      window.location.href = "Home.html";
    } else {
      document.getElementById('error-msg').innerText = "Please fill in all fields!";
    }
  });
      