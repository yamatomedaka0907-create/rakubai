(() => {
  const boot = window.SITE_EDITOR_BOOTSTRAP;
  if (!boot) return;

  const state = {
    homepage: { ...boot.homepage },
    sections: (boot.sections || []).filter((s) => s.section_type !== 'hero').map((s) => ({ ...s, items: Array.isArray(s.items) ? s.items.map((i) => ({ ...i })) : [] })),
    dirty: false,
  };

  const shopId = boot.shopId;
  const body = document.body;
  const toastEl = document.getElementById('editor-toast');
  const panelEl = document.getElementById('editor-panel');
  const popoverEl = document.getElementById('editor-popover');
  const colorPickerEl = document.getElementById('editor-color-picker');
  let popoverCloseTimer = null;

  const showToast = (message, error = false) => {
    if (!toastEl) return;
    toastEl.textContent = message;
    toastEl.style.background = error ? '#991b1b' : '#111827';
    toastEl.classList.add('show');
    setTimeout(() => toastEl.classList.remove('show'), 2200);
  };

  const markDirty = () => { state.dirty = true; };
  window.addEventListener('beforeunload', (e) => {
    if (!state.dirty) return;
    e.preventDefault();
    e.returnValue = '';
  });

  const findSection = (sectionId) => state.sections.find((s) => Number(s.id) === Number(sectionId));
  const sectionEl = (sectionId) => document.querySelector(`[data-section-id="${sectionId}"]`);
  const normalizeText = (value) => (value || '').replace(/\u00a0/g, ' ').trim();

  const applyManagedCss = () => {
    let styleEl = document.getElementById('editor-managed-styles');
    if (!styleEl) {
      styleEl = document.createElement('style');
      styleEl.id = 'editor-managed-styles';
      document.head.appendChild(styleEl);
    }
    styleEl.textContent = state.homepage.custom_css || '';
  };

  const upsertManagedCssBlock = (key, css) => {
    const start = `/* managed:${key}:start */`;
    const end = `/* managed:${key}:end */`;
    const pattern = new RegExp(`${start}[\\s\\S]*?${end}`, 'g');
    const current = state.homepage.custom_css || '';
    const nextBlock = css ? `${start}\n${css}\n${end}` : '';
    let next = current.replace(pattern, '').trim();
    if (nextBlock) next = `${next}${next ? '\n\n' : ''}${nextBlock}`;
    state.homepage.custom_css = next;
    applyManagedCss();
    markDirty();
  };

  const getStyleValue = (prop, fallback) => {
    const value = getComputedStyle(document.documentElement).getPropertyValue(prop).trim();
    return value || fallback;
  };

  const openColorPicker = (initialValue, onChange) => {
    if (!colorPickerEl) return;
    colorPickerEl.value = /^#[0-9a-fA-F]{6}$/.test(initialValue || '') ? initialValue : '#2563eb';
    const handler = () => {
      colorPickerEl.removeEventListener('input', handler);
      colorPickerEl.removeEventListener('change', handler);
      onChange(colorPickerEl.value);
    };
    colorPickerEl.addEventListener('input', handler, { once: true });
    colorPickerEl.addEventListener('change', handler, { once: true });
    colorPickerEl.click();
  };

  const closePopover = () => {
    if (!popoverEl) return;
    popoverEl.hidden = true;
    popoverEl.innerHTML = '';
  };

  const openPopover = (anchor, title, actions = []) => {
    if (!popoverEl || !anchor || !actions.length) return;
    popoverEl.innerHTML = `<div class="editor-popover-title">${title}</div>`;
    actions.forEach((action) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = action.label;
      if (action.danger) btn.classList.add('danger');
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        closePopover();
        try {
          await action.run();
        } catch (err) {
          showToast(err.message || '操作に失敗しました。', true);
        }
      });
      popoverEl.appendChild(btn);
    });
    const rect = anchor.getBoundingClientRect();
    popoverEl.hidden = false;
    const top = Math.min(window.innerHeight - popoverEl.offsetHeight - 12, rect.top + 16);
    const left = Math.min(window.innerWidth - popoverEl.offsetWidth - 12, Math.max(12, rect.left + 16));
    popoverEl.style.top = `${Math.max(12, top)}px`;
    popoverEl.style.left = `${left}px`;
  };

  document.addEventListener('click', (e) => {
    if (!popoverEl || popoverEl.hidden) return;
    if (popoverEl.contains(e.target)) return;
    if (e.target.closest('[data-edit-region]')) return;
    closePopover();
  });

  document.querySelectorAll('[data-homepage-field]').forEach((el) => {
    el.addEventListener('input', () => {
      const field = el.dataset.homepageField;
      if (el.dataset.readonly === 'true') return;
      state.homepage[field] = normalizeText(el.innerText);
      if (field === 'site_title') document.title = state.homepage[field] || document.title;
      markDirty();
    });
  });

  document.querySelectorAll('[data-section-field]').forEach((el) => {
    el.addEventListener('input', () => {
      const section = findSection(el.closest('[data-section-id]').dataset.sectionId);
      if (!section) return;
      section[el.dataset.sectionField] = normalizeText(el.innerText);
      markDirty();
    });
  });

  document.querySelectorAll('[data-item-field]').forEach((el) => {
    el.addEventListener('input', () => {
      const wrapper = el.closest('[data-item-index]');
      const section = findSection(el.closest('[data-section-id]').dataset.sectionId);
      if (!wrapper || !section) return;
      const item = section.items[Number(wrapper.dataset.itemIndex)];
      if (!item) return;
      item[el.dataset.itemField] = normalizeText(el.innerText);
      markDirty();
    });
  });

  const uploadFile = async (file, category = 'editor') => {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('category', category);
    const res = await fetch(`/admin/${shopId}/website/editor/upload`, { method: 'POST', body: fd });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || 'アップロードに失敗しました。');
    return data.url;
  };

  const askImage = async () => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    const file = await new Promise((resolve) => {
      input.onchange = () => resolve(input.files?.[0] || null);
      input.click();
    });
    if (!file) return null;
    return await uploadFile(file);
  };

  const focusEditable = (selector) => {
    const el = document.querySelector(selector);
    if (!el) return showToast('この場所では直接編集できる項目がまだありません。', true);
    if (panelEl && !panelEl.hidden) panelEl.hidden = true;
    el.focus();
    const range = document.createRange();
    range.selectNodeContents(el);
    range.collapse(false);
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
  };

  document.querySelectorAll('[data-homepage-image]').forEach((el) => {
    el.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      const field = el.dataset.homepageImage;
      try {
        const url = await askImage();
        if (url === null) return;
        state.homepage[field] = url;
        if (el.tagName === 'IMG') {
          el.src = url || '';
        } else if (url) {
          location.reload();
          return;
        }
        markDirty();
        showToast('画像を反映しました。');
      } catch (err) {
        showToast(err.message, true);
      }
    });
  });

  document.querySelectorAll('[data-section-image]').forEach((el) => {
    el.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      const sec = findSection(el.closest('[data-section-id]').dataset.sectionId);
      if (!sec) return;
      try {
        const url = await askImage();
        if (url === null) return;
        sec.image_url = url;
        if (el.tagName === 'IMG') el.src = url || '';
        else location.reload();
        markDirty();
        showToast('画像を反映しました。');
      } catch (err) {
        showToast(err.message, true);
      }
    });
  });

  document.querySelectorAll('[data-item-image]').forEach((el) => {
    el.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      const card = el.closest('[data-item-index]');
      const sec = findSection(el.closest('[data-section-id]').dataset.sectionId);
      if (!card || !sec) return;
      const item = sec.items[Number(card.dataset.itemIndex)];
      try {
        const url = await askImage();
        if (url === null) return;
        item.url = url;
        el.src = url || '';
        markDirty();
        showToast('画像を反映しました。');
      } catch (err) {
        showToast(err.message, true);
      }
    });
  });

  const editLink = (target, model, labelKey, urlKey) => {
    target.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const nextLabel = prompt('ボタン文言を入力', model[labelKey] || target.textContent || '');
      if (nextLabel === null) return;
      const nextUrl = prompt('リンクURLを入力', model[urlKey] || target.getAttribute('href') || '');
      if (nextUrl === null) return;
      model[labelKey] = nextLabel.trim();
      model[urlKey] = nextUrl.trim();
      target.textContent = model[labelKey] || 'ボタンを編集';
      target.setAttribute('href', model[urlKey] || '#');
      markDirty();
    });
  };

  document.querySelectorAll('[data-homepage-link]').forEach((el) => editLink(el, state.homepage, 'reserve_button_label', 'reserve_button_url'));
  document.querySelectorAll('[data-section-link]').forEach((el) => {
    const sec = findSection(el.closest('[data-section-id]').dataset.sectionId);
    if (sec) editLink(el, sec, 'button_label', 'button_url');
  });

  const rerenderIndexes = (sectionNode, sec) => {
    [...sectionNode.querySelectorAll('[data-item-index]')].forEach((node, idx) => { node.dataset.itemIndex = idx; });
    sec.items = sec.items.filter(Boolean);
  };

  document.querySelectorAll('[data-item-action]').forEach((btn) => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const action = btn.dataset.itemAction;
      const card = btn.closest('[data-item-index]');
      const secNode = btn.closest('[data-section-id]');
      const sec = findSection(secNode.dataset.sectionId);
      if (!card || !sec) return;
      const idx = Number(card.dataset.itemIndex);
      if (action === 'delete') {
        sec.items.splice(idx, 1);
        card.remove();
        rerenderIndexes(secNode, sec);
        markDirty();
        return;
      }
      if (action === 'up' && idx > 0) {
        [sec.items[idx - 1], sec.items[idx]] = [sec.items[idx], sec.items[idx - 1]];
        card.parentNode.insertBefore(card, card.previousElementSibling);
        rerenderIndexes(secNode, sec);
        markDirty();
        return;
      }
      if (action === 'down' && idx < sec.items.length - 1) {
        [sec.items[idx + 1], sec.items[idx]] = [sec.items[idx], sec.items[idx + 1]];
        card.parentNode.insertBefore(card.nextElementSibling, card);
        rerenderIndexes(secNode, sec);
        markDirty();
        return;
      }
      if (action === 'edit') {
        const item = sec.items[idx];
        Object.keys(item).forEach((key) => {
          const next = prompt(`${key} を入力`, item[key] || '');
          if (next !== null) item[key] = next.trim();
        });
        location.reload();
      }
      if (action === 'image') {
        const item = sec.items[idx];
        try {
          const url = await askImage();
          if (url === null) return;
          item.url = url;
          const img = card.querySelector('img');
          if (img) img.src = url;
          markDirty();
        } catch (err) {
          showToast(err.message, true);
        }
      }
    });
  });

  document.querySelectorAll('[data-add-item]').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const sec = findSection(btn.closest('[data-section-id]').dataset.sectionId);
      if (!sec) return;
      let item;
      if (sec.section_type === 'menu') item = { title: '新しいメニュー', price: '¥0', description: '説明を入力' };
      else if (sec.section_type === 'gallery') item = { label: '写真', url: '' };
      else if (sec.section_type === 'news') item = { date: new Date().toISOString().slice(0, 10), title: 'お知らせを入力' };
      else item = { title: 'タイトル', description: '説明を入力' };
      sec.items.push(item);
      location.reload();
    });
  });

  document.querySelectorAll('[data-section-action]').forEach((btn) => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const action = btn.dataset.sectionAction;
      const secNode = btn.closest('[data-section-id]');
      const sec = findSection(secNode.dataset.sectionId);
      if (!sec) return;
      const idx = state.sections.findIndex((s) => Number(s.id) === Number(sec.id));
      if (action === 'delete') {
        if (!confirm('このセクションを削除しますか？')) return;
        const res = await fetch(`/admin/${shopId}/website/editor/section/${sec.id}/delete`, { method: 'POST' });
        const data = await res.json();
        if (!data.ok) return showToast(data.error || '削除に失敗しました。', true);
        state.sections.splice(idx, 1);
        secNode.remove();
        markDirty();
        showToast('セクションを削除しました。');
        return;
      }
      if (action === 'toggle') {
        sec.is_visible = sec.is_visible ? 0 : 1;
        secNode.classList.toggle('section-is-hidden', !sec.is_visible);
        btn.textContent = sec.is_visible ? '非表示' : '表示';
        markDirty();
        return;
      }
      if (action === 'up' && idx > 0) {
        [state.sections[idx - 1], state.sections[idx]] = [state.sections[idx], state.sections[idx - 1]];
        secNode.parentNode.insertBefore(secNode, secNode.previousElementSibling);
        markDirty();
        return;
      }
      if (action === 'down' && idx < state.sections.length - 1) {
        [state.sections[idx + 1], state.sections[idx]] = [state.sections[idx], state.sections[idx + 1]];
        secNode.parentNode.insertBefore(secNode.nextElementSibling, secNode);
        markDirty();
        return;
      }
      if (action === 'image') {
        try {
          const url = await askImage();
          if (url === null) return;
          sec.image_url = url;
          location.reload();
        } catch (err) {
          showToast(err.message, true);
        }
        return;
      }
      if (action === 'button') {
        const label = prompt('ボタン文言', sec.button_label || '');
        if (label === null) return;
        const url = prompt('ボタンURL', sec.button_url || '');
        if (url === null) return;
        sec.button_label = label.trim();
        sec.button_url = url.trim();
        const link = secNode.querySelector('[data-section-link]');
        if (link) {
          link.textContent = sec.button_label || 'ボタンを編集';
          link.setAttribute('href', sec.button_url || '#');
        }
        markDirty();
      }
    });
  });

  document.querySelectorAll('[data-homepage-input], [data-homepage-checkbox]').forEach((el) => {
    el.addEventListener('input', () => {
      const key = el.dataset.homepageInput || el.dataset.homepageCheckbox;
      state.homepage[key] = el.type === 'checkbox' ? (el.checked ? 1 : 0) : el.value;
      markDirty();
    });
  });

  const applySettings = (silent = false) => {
    const root = document.documentElement.style;
    root.setProperty('--primary', state.homepage.primary_color || '#2563eb');
    root.setProperty('--bg', state.homepage.background_color || '#f8fafc');
    root.setProperty('--surface', state.homepage.surface_color || '#ffffff');
    root.setProperty('--text', state.homepage.text_color || '#111827');
    root.setProperty('--sub', state.homepage.subtext_color || '#6b7280');
    root.setProperty('--font', state.homepage.font_family || "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif");
    applyManagedCss();
    if (!silent) showToast('画面に反映しました。保存で確定します。');
  };

  const save = async () => {
    const payload = {
      homepage: state.homepage,
      sections: state.sections.map((s, index) => ({ ...s, sort_order: (index + 1) * 10 })),
      section_order: state.sections.map((s) => s.id),
    };
    const res = await fetch(`/admin/${shopId}/website/editor/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || '保存に失敗しました。');
    state.dirty = false;
    showToast('保存しました。');
  };

  document.querySelectorAll('[data-editor-action]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const action = btn.dataset.editorAction;
      if (action === 'toggle-panel' && panelEl) panelEl.hidden = !panelEl.hidden;
      if (action === 'apply-settings') applySettings();
      if (action === 'save') {
        try { await save(); } catch (err) { showToast(err.message, true); }
      }
      if (action === 'add-section') {
        const sectionType = prompt('追加するセクションタイプを入力\nabout / text / image_text / menu / features / gallery / news / cta / contact', 'text');
        if (!sectionType) return;
        const res = await fetch(`/admin/${shopId}/website/editor/section/add`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ section_type: sectionType, title: '新しいセクション' }),
        });
        const data = await res.json();
        if (!data.ok) return showToast(data.error || '追加に失敗しました。', true);
        showToast('セクションを追加しました。');
        location.reload();
      }
    });
  });

  const chooseSectionBackgroundColor = (sec, secNode) => {
    const panel = secNode.querySelector('.panel');
    openColorPicker(getComputedStyle(panel).backgroundColor ? '#ffffff' : (state.homepage.surface_color || '#ffffff'), (color) => {
      upsertManagedCssBlock(`section-${sec.id}`, `#section-${sec.id} > .panel{background:${color} !important;}`);
      showToast('セクション背景色を反映しました。');
    });
  };

  const chooseSectionBackgroundImage = async (sec) => {
    const url = await askImage();
    if (url === null) return;
    upsertManagedCssBlock(`section-${sec.id}`, `#section-${sec.id} > .panel{background-image:linear-gradient(rgba(255,255,255,.82),rgba(255,255,255,.82)),url('${url}');background-size:cover;background-position:center;}`);
    showToast('セクション背景画像を反映しました。');
  };

  const setupRegionMenus = () => {
    document.querySelectorAll('[data-edit-region]').forEach((region) => {
      region.addEventListener('click', (e) => {
        if (e.target.closest('button,a,input,textarea,select,[contenteditable="true"],.item-actions,.section-controls,.editor-panel,.editor-shell,.editable-image')) return;
        const type = region.dataset.editRegion;
        if (type === 'topbar') {
          openPopover(region, 'ヘッダーを編集', [
            { label: '店名を編集', run: () => focusEditable('[data-homepage-field="site_title"]') },
            { label: 'ロゴ画像を変更', run: async () => {
                const url = await askImage(); if (url === null) return;
                state.homepage.logo_image_url = url;
                const img = document.querySelector('[data-homepage-image="logo_image_url"]');
                if (img && img.tagName === 'IMG') img.src = url; else location.reload();
                markDirty();
                showToast('ロゴを反映しました。');
              } },
            { label: 'メインカラーを変更', run: () => openColorPicker(state.homepage.primary_color || getStyleValue('--primary', '#2563eb'), (color) => { state.homepage.primary_color = color; applySettings(true); markDirty(); showToast('メインカラーを反映しました。'); }) },
            { label: 'ページ背景色を変更', run: () => openColorPicker(state.homepage.background_color || getStyleValue('--bg', '#f8fafc'), (color) => { state.homepage.background_color = color; applySettings(true); markDirty(); showToast('背景色を反映しました。'); }) },
          ]);
          return;
        }
        if (type === 'hero') {
          openPopover(region, 'メインビジュアルを編集', [
            { label: 'タイトルを編集', run: () => focusEditable('[data-homepage-field="hero_title"]') },
            { label: '説明文を編集', run: () => focusEditable('[data-homepage-field="hero_subtitle"]') },
            { label: '背景画像を変更', run: async () => {
                const url = await askImage(); if (url === null) return;
                state.homepage.hero_image_url = url;
                const img = document.querySelector('[data-homepage-image="hero_image_url"]');
                if (img) img.src = url; else location.reload();
                markDirty();
                showToast('ヘッダー画像を反映しました。');
              } },
            { label: '文字位置を切替', run: () => { state.homepage.hero_align = state.homepage.hero_align === 'center' ? 'left' : 'center'; markDirty(); location.reload(); } },
            { label: 'ページ背景色を変更', run: () => openColorPicker(state.homepage.background_color || getStyleValue('--bg', '#f8fafc'), (color) => { state.homepage.background_color = color; applySettings(true); markDirty(); showToast('背景色を反映しました。'); }) },
          ]);
          return;
        }
        if (type === 'section') {
          const secNode = region.closest('[data-section-id]');
          const sec = findSection(secNode?.dataset.sectionId);
          if (!sec || !secNode) return;
          openPopover(region, 'セクションを編集', [
            { label: '見出しを編集', run: () => focusEditable(`#section-${sec.id} [data-section-field="title"]`) },
            { label: '本文を編集', run: () => focusEditable(`#section-${sec.id} [data-section-field="body_text"], #section-${sec.id} [data-section-field="subtitle"]`) },
            { label: '背景色を変更', run: () => chooseSectionBackgroundColor(sec, secNode) },
            { label: '背景画像を設定', run: () => chooseSectionBackgroundImage(sec) },
            { label: '画像を変更', run: async () => {
                const url = await askImage(); if (url === null) return;
                sec.image_url = url; markDirty(); location.reload();
              } },
          ]);
        }
      });
    });
  };

  applySettings(true);
  setupRegionMenus();
})();
