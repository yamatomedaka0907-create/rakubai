(() => {
  const shopId = document.body?.dataset?.bookingEditorShopId;
  if (!shopId) return;

  const toastEl = document.getElementById('booking-editor-toast');
  const showToast = (message, isError = false) => {
    if (!toastEl) return;
    toastEl.textContent = message;
    toastEl.style.background = isError ? '#991b1b' : '#111827';
    toastEl.classList.add('show');
    window.clearTimeout(showToast._timer);
    showToast._timer = window.setTimeout(() => toastEl.classList.remove('show'), 2200);
  };

  const normalizeText = (value) => String(value || '').replace(/\u00a0/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
  const collectPayload = () => {
    const payload = {};
    document.querySelectorAll('[data-booking-field]').forEach((el) => {
      payload[el.dataset.bookingField] = normalizeText(el.innerText);
    });
    return payload;
  };

  const save = async () => {
    try {
      const res = await fetch(`/admin/${shopId}/booking-page/editor/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(collectPayload()),
      });
      if (!res.ok) throw new Error('save_failed');
      showToast('予約ページを保存しました。');
    } catch (err) {
      showToast('保存に失敗しました。', true);
    }
  };

  document.querySelectorAll('[data-booking-editor-action="save"]').forEach((btn) => {
    btn.addEventListener('click', save);
  });
})();
