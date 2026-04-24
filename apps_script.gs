/**
 * Google Apps Script — Nomad полис PDF генераторы
 * Шаблон: 1liT64SRtLgiOgl_EnWfTGklZUy2wC2eD6INUWu_uXoI
 */

const TEMPLATE_DOC_ID = '1liT64SRtLgiOgl_EnWfTGklZUy2wC2eD6INUWu_uXoI';

// Қарапайым қауіпсіздік — URL ашықта жүрмейтін болсын деп
// Бот осы token-ді бірге жібереді. Қаласаңыз кез келген мәтінге ауыстырыңыз.
const SECRET_TOKEN = 'kulshar-polis-2026';

function doGet(e) {
  return ContentService
    .createTextOutput(JSON.stringify({ ok: true, msg: 'polis web app is running' }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents || '{}');

    if (data.token !== SECRET_TOKEN) {
      return _json({ ok: false, error: 'unauthorized' });
    }

    const copy = DriveApp.getFileById(TEMPLATE_DOC_ID).makeCopy('polis_' + Date.now());
    const copyId = copy.getId();

    try {
      const doc = DocumentApp.openById(copyId);
      const body = doc.getBody();

      const replacements = [
        ['0656T160437N', data.dogovor_no],
        ['МОМИНОВ БАУРЖАН КУДАЙКУЛОВИЧ', data.fio],
        ['730101399496', data.iin],
        ['SHACMAN', data.car_brand],
        ['A26848', data.car_number],
        ['LZGJR4T48TX015477', data.vin],
        ['15,000,00 пятнадцать тысяч тенге 00', data.amount],
        ['20.03.2026', data.date_from],
        ['29.03.2026', data.date_to],
        ['19.03.2026', data.dogovor_date],
      ];

      for (const [anchor, value] of replacements) {
        body.replaceText(_escapeRegex(anchor), String(value == null ? '' : value));
      }
      // Class: standalone "13" (dates with 13 already replaced above)
      body.replaceText('\\b13\\b', String(data.klass == null ? '' : data.klass));

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

function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function _escapeRegex(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
