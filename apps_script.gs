/**
 * Google Apps Script — полис PDF генераторы (1-5 адам)
 *
 * Деплой инструкциясы:
 *   1) Бір рет setup() функциясын Apps Script редакторында іске қосыңыз — ол барлық 5 шаблонды
 *      бірыңғай {{fio_N}} / {{iin_N}} / {{klass_N}} плейсхолдерлеріне айналдырады.
 *   2) Сосын Deploy → Manage deployments → түзету (қарандаш белгісі) → New version → Deploy
 *      (URL сол қалпында).
 */

// people_count → Google Doc ID
const TEMPLATES = {
  1: '1liT64SRtLgiOgl_EnWfTGklZUy2wC2eD6INUWu_uXoI',
  2: '15lnRLxtTEah4Ofpl5p4bkrp5zBJSk9i4EXrkyuKIjOw',
  3: '1RWq-OLcw0W0Y2hO_I7Kp85biWZjR3GoxncFYhjAt0iI',
  4: '1-fW0xXI2q_n5PUgiYWKW6TAAcjGsbhSLcuS3BP_OXvY',
  5: '1wnjfoCE80HmK3K4477RJXy68G7JX4iObL-OrwZsM2RE',
};

const SECRET_TOKEN = 'kulshar-polis-2026';

// ============================================================================
//  HTTP entry points
// ============================================================================

function doGet(e) {
  return _json({ ok: true, msg: 'polis web app is running' });
}

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents || '{}');
    if (data.token !== SECRET_TOKEN) return _json({ ok: false, error: 'unauthorized' });

    const count = parseInt(data.people_count || 1, 10);
    if (!(count >= 1 && count <= 5)) {
      return _json({ ok: false, error: 'people_count 1..5 болуы керек' });
    }
    const templateId = TEMPLATES[count];
    if (!templateId) return _json({ ok: false, error: 'template not found' });

    const persons = Array.isArray(data.persons) ? data.persons : [];

    const copy = DriveApp.getFileById(templateId).makeCopy('polis_' + Date.now());
    const copyId = copy.getId();

    try {
      const doc = DocumentApp.openById(copyId);
      const body = doc.getBody();

      // Common fields (1 машина)
      _replace(body, '{{dogovor_no}}',   data.dogovor_no);
      _replace(body, '{{car_brand}}',    data.car_brand);
      _replace(body, '{{car_number}}',   data.car_number);
      _replace(body, '{{vin}}',          data.vin);
      _replace(body, '{{amount}}',       data.amount);
      _replace(body, '{{date_from}}',    data.date_from);
      _replace(body, '{{date_to}}',      data.date_to);
      _replace(body, '{{dogovor_date}}', data.dogovor_date);

      // Per-person fields
      for (let i = 0; i < count; i++) {
        const p = persons[i] || {};
        _replace(body, `{{fio_${i + 1}}}`,   p.fio);
        _replace(body, `{{iin_${i + 1}}}`,   p.iin);
        _replace(body, `{{klass_${i + 1}}}`, p.klass);
      }

      doc.saveAndClose();

      const pdfBlob = DriveApp.getFileById(copyId).getAs(MimeType.PDF);
      const pdfBase64 = Utilities.base64Encode(pdfBlob.getBytes());
      return _json({ ok: true, pdf_base64: pdfBase64, filename: 'polis.pdf' });
    } finally {
      try { DriveApp.getFileById(copyId).setTrashed(true); } catch (_) {}
    }
  } catch (err) {
    return _json({ ok: false, error: String(err) });
  }
}

// ============================================================================
//  setup() — бір рет іске қосылып, шаблондарды бірыңғай форматқа келтіреді
// ============================================================================

function setup() {
  const log = [];

  for (const [countStr, docId] of Object.entries(TEMPLATES)) {
    const count = parseInt(countStr, 10);
    try {
      const doc = DocumentApp.openById(docId);
      const body = doc.getBody();

      // 1) Common (нomad sample values + amoCRM IDs)
      _swap(body, '0656T160437N',                        '{{dogovor_no}}');
      _swap(body, '{{lead.cf.3100597}}',                 '{{dogovor_no}}');

      _swap(body, 'SHACMAN',                             '{{car_brand}}');
      _swap(body, '{{lead.cf.3099475}}',                 '{{car_brand}}');

      _swap(body, 'A26848',                              '{{car_number}}');
      _swap(body, '{{lead.cf.3099477}}',                 '{{car_number}}');

      _swap(body, 'LZGJR4T48TX015477',                   '{{vin}}');
      _swap(body, '{{lead.cf.3626825}}',                 '{{vin}}');

      _swap(body, '15,000,00 пятнадцать тысяч тенге 00', '{{amount}}');
      _swap(body, '{{lead.cf.3137989}}',                 '{{amount}}');
      _swap(body, '{{lead.cf.3642993}}',                 '{{amount}}');

      _swap(body, '20.03.2026',                          '{{date_from}}');
      _swap(body, '{{lead.cf.3099875}}',                 '{{date_from}}');

      _swap(body, '29.03.2026',                          '{{date_to}}');
      _swap(body, '{{lead.cf.3099877}}',                 '{{date_to}}');

      _swap(body, '19.03.2026',                          '{{dogovor_date}}');
      _swap(body, '{{lead.cf.3113337}}',                 '{{dogovor_date}}');

      // 2) Person 1
      _swap(body, 'МОМИНОВ БАУРЖАН КУДАЙКУЛОВИЧ',        '{{fio_1}}');
      _swap(body, '{{lead.cf.3095999}}',                 '{{fio_1}}');

      _swap(body, '730101399496',                        '{{iin_1}}');
      _swap(body, '{{lead.cf.3096021}}',                 '{{iin_1}}');

      _swap(body, '{{lead.cf.3096027}}',                 '{{klass_1}}');

      // 3) Person 2 (templates 2+)
      if (count >= 2) {
        _swap(body, '{{lead.cf.3099379}}', '{{fio_2}}');
        _swap(body, '{{lead.cf.3099415}}', '{{iin_2}}');
      }

      // 4) Person 3 (template 3+)
      if (count >= 3) {
        _swap(body, '{{lead.cf.3099381}}', '{{fio_3}}');
        _swap(body, '{{lead.cf.3099417}}', '{{iin_3}}');
      }

      // 5) Templates 4-5: literal "фио"/"иин" → placeholders for persons 2..N
      if (count >= 4) {
        for (let i = 2; i <= count; i++) {
          _replaceLiteralOnce(body, 'фио', `{{fio_${i}}}`);
          _replaceLiteralOnce(body, 'иин', `{{iin_${i}}}`);
        }
      }

      // 6) Class column in Insured Persons table
      _addClassPlaceholders(body, count);

      doc.saveAndClose();
      log.push(`✅ ${count}-person OK (${docId})`);
    } catch (err) {
      log.push(`❌ ${count}-person FAILED: ${err}`);
    }
  }

  const out = log.join('\n');
  console.log(out);
  return out;
}

// ============================================================================
//  Helpers
// ============================================================================

function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function _replace(body, placeholder, value) {
  body.replaceText(_escapeRegex(placeholder), String(value == null ? '' : value));
}

// One-shot swap: source text → target placeholder. Idempotent (no-op if source absent).
function _swap(body, from, to) {
  body.replaceText(_escapeRegex(from), to);
}

function _escapeRegex(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Replace just the first remaining occurrence of `literal` with `replacement`.
function _replaceLiteralOnce(body, literal, replacement) {
  const found = body.findText(_escapeRegex(literal));
  if (!found) return;
  const elem = found.getElement().asText();
  const start = found.getStartOffset();
  const end = found.getEndOffsetInclusive();
  elem.deleteText(start, end);
  elem.insertText(start, replacement);
}

// In the Insured Persons table, set the class cell of each data row to {{klass_N}}.
function _addClassPlaceholders(body, count) {
  const tables = body.getTables();
  for (const table of tables) {
    const txt = table.getText();
    if (txt.indexOf('Сақтандырылған') < 0 && txt.indexOf('Застрахованные') < 0) continue;

    const numRows = table.getNumRows();
    let personIdx = 0;
    for (let r = 0; r < numRows; r++) {
      const row = table.getRow(r);
      const firstCellText = row.getCell(0).getText().trim();
      if (!/^\d+$/.test(firstCellText)) continue;  // skip non-data rows
      personIdx++;
      if (personIdx > count) break;
      const lastCell = row.getCell(row.getNumCells() - 1);
      // Replace cell content while keeping paragraph formatting
      const para = lastCell.getChild(0).asParagraph();
      const text = para.editAsText();
      const len = text.getText().length;
      if (len > 0) text.deleteText(0, len - 1);
      text.appendText(`{{klass_${personIdx}}}`);
    }
    return;  // only one such table per doc
  }
}
