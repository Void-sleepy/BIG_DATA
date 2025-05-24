const editBtn = document.getElementById('editBtn');
const editForm = document.getElementById('editForm');
const cancelBtn = document.getElementById('cancelBtn');
const profileDetails = document.getElementById('profileDetails');
const profileName = document.getElementById('profileName');
const profileGender = document.getElementById('profileGender');
const profileEmail = document.getElementById('profileEmail');
const profilePassword = document.getElementById('profilePassword');

editBtn.addEventListener('click', () => {
  editForm.classList.toggle('hidden');
  profileDetails.classList.toggle('hidden');
  editBtn.textContent = editForm.classList.contains('hidden') ? 'Edit Profile' : 'View Profile';

  if (!editForm.classList.contains('hidden')) {
    // Fill form with current profile data when editing starts
    document.getElementById('editName').value = profileName.textContent;
    document.getElementById('editGender').value = profileGender.textContent;
    document.getElementById('editEmail').value = profileEmail.textContent;
  }
});

cancelBtn.addEventListener('click', () => {
  editForm.classList.add('hidden');
  profileDetails.classList.remove('hidden');
  editBtn.textContent = 'Edit Profile';
  editForm.querySelector('form').reset();
});

editForm.querySelector('form').addEventListener('submit', (e) => {
  e.preventDefault();
  const newName = document.getElementById('editName').value.trim();
  const newGender = document.getElementById('editGender').value;
  const newEmail = document.getElementById('editEmail').value.trim();
  const newPassword = document.getElementById('editPassword').value;

  if (newName && newEmail) {
    profileName.textContent = newName;
    profileGender.textContent = newGender;
    profileEmail.textContent = newEmail;
    profilePassword.textContent = newPassword ? '••••••••' : profilePassword.textContent;

    editForm.classList.add('hidden');
    profileDetails.classList.remove('hidden');
    editBtn.textContent = 'Edit Profile';
    editForm.querySelector('form').reset();
  }
});


const logoutBtn = document.getElementById('logoutBtn');
logoutBtn.addEventListener('click', () => {
  // Redirect to Login page (or perform logout logic)
  window.location.href = 'Login.html';
});
