// Shibuya не запоминает прокрутку левого меню при переходе между страницами.
// Сохраняем её и восстанавливаем, чтобы читатель не терял место в оглавлении.
(() => {
  const key = 'tallyho:sidebar-scroll';

  document.addEventListener('DOMContentLoaded', () => {
    const sidebar = document.querySelector('#lside .sy-scrollbar');
    if (!sidebar) return;

    try {
      const saved = sessionStorage.getItem(key);
      if (saved !== null) {
        const position = Number(saved);
        if (Number.isFinite(position) && position >= 0) {
          sidebar.scrollTop = position;
        }
      }
    } catch {
      // Настройки браузера могут закрыть доступ к хранилищу.
    }

    window.addEventListener('pagehide', () => {
      try {
        sessionStorage.setItem(key, String(sidebar.scrollTop));
      } catch {
        // Навигация работает и без хранилища.
      }
    });
  });
})();
