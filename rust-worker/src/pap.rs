use quick_xml::events::Event;
use quick_xml::{name::QName, Reader};
use std::collections::BTreeSet;

use crate::{date_rfc3339, Error, XmlPublication};

pub(super) fn parse_pap_v15_application(xml: &str) -> Result<XmlPublication, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut state = PapState::default();
    let mut buffer = Vec::new();
    let mut path: Vec<String> = Vec::new();

    loop {
        let event = reader.read_event_into(&mut buffer)?;
        match event {
            Event::Start(start) => {
                let name = local_name(start.name());
                state.on_start(&path, &name);
                path.push(name);
            }
            Event::End(end) => {
                let name = local_name(end.name());
                path.pop();
                state.on_end(&path, &name);
            }
            Event::Empty(empty) => {
                let name = local_name(empty.name());
                state.on_empty(&path, &name);
            }
            Event::Text(text) => state.push_text(&path, text.unescape()?.as_ref()),
            Event::CData(cdata) => {
                state.push_text(&path, &String::from_utf8_lossy(&cdata));
            }
            Event::Eof => break,
            _ => {}
        }
        buffer.clear();
    }

    state.into_publication()
}

fn local_name(name: QName<'_>) -> String {
    std::str::from_utf8(name.local_name().as_ref())
        .unwrap_or("")
        .to_owned()
}

#[derive(Default)]
struct PapState {
    doc_number: Option<String>,
    kind_code: Option<String>,
    document_date: Option<String>,
    filing_date: Option<String>,
    application_number: Option<String>,
    title: Option<String>,
    ipc: BTreeSet<String>,
    us_class: Vec<String>,
    applicants: Vec<String>,
    applicant_seen: BTreeSet<String>,
    assignee: Vec<String>,
    saw_assignee: bool,
    current_uspc: Option<UspcFields>,
    abstract_collector: TextCollector,
    description_collector: TextCollector,
    claims_collector: TextCollector,
    inventor_parts: InventorParts,
    in_assignee_name: bool,
    in_authority_applicant: bool,
    in_claim: bool,
}

#[derive(Default)]
struct InventorParts {
    given: Option<String>,
    middle: Option<String>,
    family: Option<String>,
    target: Option<InventorTarget>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InventorTarget {
    Inventor,
    FirstNamed,
}

#[derive(Default)]
struct UspcFields {
    class: Option<String>,
    subclass: Option<String>,
}

#[derive(Default)]
struct TextCollector {
    paragraphs: Vec<String>,
    current: String,
}

impl TextCollector {
    fn push_fragment(&mut self, text: &str) {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        if !self.current.is_empty() {
            self.current.push(' ');
        }
        self.current.push_str(trimmed);
    }

    fn finish_paragraph(&mut self) {
        let collapsed = self
            .current
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if !collapsed.is_empty() {
            self.paragraphs.push(collapsed);
        }
        self.current.clear();
    }

    fn into_body(mut self) -> String {
        if !self.current.is_empty() {
            self.finish_paragraph();
        }
        self.paragraphs.join("\n\n")
    }
}

impl PapState {
    fn on_start(&mut self, path: &[String], name: &str) {
        match name {
            "uspc" if path_contains(path, "classification-us") => {
                self.current_uspc = Some(UspcFields::default());
            }
            "assignee" if path_contains(path, "subdoc-bibliographic-information") => {
                self.saw_assignee = true;
                self.assignee.clear();
            }
            "name" if self.saw_assignee && path.last().is_some_and(|leaf| leaf == "assignee") => {
                self.in_assignee_name = true;
            }
            "authority-applicant" => self.in_authority_applicant = true,
            "inventor" if path_contains(path, "inventors") => {
                self.inventor_parts = InventorParts {
                    target: Some(InventorTarget::Inventor),
                    ..InventorParts::default()
                };
            }
            "first-named-inventor" if path_contains(path, "inventors") => {
                self.inventor_parts = InventorParts {
                    target: Some(InventorTarget::FirstNamed),
                    ..InventorParts::default()
                };
            }
            "claim" if path_contains(path, "subdoc-claims") => self.in_claim = true,
            _ => {}
        }
    }

    fn on_empty(&mut self, path: &[String], name: &str) {
        self.capture_scalar(path, name, "");
    }

    fn on_end(&mut self, path: &[String], name: &str) {
        match name {
            "uspc" => {
                if let Some(uspc) = self.current_uspc.take() {
                    if let (Some(class), Some(subclass)) = (uspc.class, uspc.subclass) {
                        let formatted = format!("{class} {}", normalize_subclass(&subclass));
                        if !self.us_class.contains(&formatted) {
                            self.us_class.push(formatted);
                        }
                    }
                }
            }
            "assignee" => self.in_assignee_name = false,
            "authority-applicant" => self.in_authority_applicant = false,
            "inventor" | "first-named-inventor" => {
                if let Some(inventor_name) = self.inventor_parts.take_name() {
                    self.push_applicant(inventor_name);
                }
            }
            "paragraph" if path_contains(path, "subdoc-abstract") => {
                self.abstract_collector.finish_paragraph();
            }
            "paragraph" if path_contains(path, "subdoc-description") => {
                self.description_collector.finish_paragraph();
            }
            "claim" if path_contains(path, "subdoc-claims") => {
                self.claims_collector.finish_paragraph();
                self.in_claim = false;
            }
            _ => {}
        }
    }

    fn push_text(&mut self, path: &[String], text: &str) {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }

        if let Some(leaf) = path.last().map(String::as_str) {
            if self.capture_scalar(path, leaf, trimmed) {
                return;
            }
        }

        if self.in_assignee_name {
            self.assignee.push(trimmed.to_owned());
            return;
        }
        if self.in_authority_applicant {
            // PAP uses this element for a role code such as INV, not a name.
            if trimmed.chars().any(|ch| ch.is_ascii_lowercase()) || trimmed.contains(' ') {
                self.push_applicant(trimmed.to_owned());
            }
            return;
        }
        if let Some(part) = path.last().map(String::as_str) {
            if self.inventor_parts.target.is_some() {
                match part {
                    "given-name" => self.inventor_parts.given = Some(trimmed.to_owned()),
                    "middle-name" => self.inventor_parts.middle = Some(trimmed.to_owned()),
                    "family-name" => self.inventor_parts.family = Some(trimmed.to_owned()),
                    _ => {}
                }
            }
        }
        if path_contains(path, "subdoc-abstract")
            && (path.last().is_some_and(|leaf| leaf == "paragraph")
                || path.last().is_some_and(|leaf| leaf == "subdoc-abstract"))
        {
            self.abstract_collector.push_fragment(trimmed);
        } else if path_contains(path, "subdoc-description")
            && (path.last().is_some_and(|leaf| leaf == "paragraph")
                || path.last().is_some_and(|leaf| leaf == "subdoc-description"))
        {
            self.description_collector.push_fragment(trimmed);
        } else if self.in_claim {
            self.claims_collector.push_fragment(trimmed);
        }
    }

    fn capture_scalar(&mut self, path: &[String], leaf: &str, text: &str) -> bool {
        match leaf {
            "doc-number" if is_under_direct_biblio_document_id(path) => {
                append_field(&mut self.doc_number, text);
                true
            }
            "kind-code" if is_under_direct_biblio_document_id(path) => {
                append_field(&mut self.kind_code, text);
                true
            }
            "document-date" if is_under_direct_biblio_document_id(path) => {
                append_field(&mut self.document_date, text);
                true
            }
            "filing-date" if path_ends_with(path, &["domestic-filing-data", "filing-date"]) => {
                append_field(&mut self.filing_date, text);
                true
            }
            "doc-number"
                if path_ends_with(path, &["domestic-filing-data", "application-number", "doc-number"]) =>
            {
                append_field(&mut self.application_number, text);
                true
            }
            "title-of-invention" => {
                append_field(&mut self.title, text);
                true
            }
            "ipc" if path_contains(path, "classification-ipc-primary") => {
                if !text.is_empty() {
                    self.ipc.insert(text.to_owned());
                }
                true
            }
            "class" if self.current_uspc.is_some() => {
                if let Some(uspc) = self.current_uspc.as_mut() {
                    append_field(&mut uspc.class, text);
                }
                true
            }
            "subclass" if self.current_uspc.is_some() => {
                if let Some(uspc) = self.current_uspc.as_mut() {
                    append_field(&mut uspc.subclass, text);
                }
                true
            }
            _ => false,
        }
    }

    fn push_applicant(&mut self, name: String) {
        if self.applicant_seen.insert(name.clone()) {
            self.applicants.push(name);
        }
    }

    fn into_publication(self) -> Result<XmlPublication, Error> {
        let publication_number = self.doc_number.unwrap_or_default();
        let title = self.title.unwrap_or_default();
        if publication_number.is_empty() || title.is_empty() {
            return Err(Error::Ingestion(
                "XML publication is missing publication number or title".into(),
            ));
        }

        let publication_kind = self
            .kind_code
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "A1".into())
            .to_ascii_uppercase();
        let publication_key = "US".to_owned();

        let abstract_body = self.abstract_collector.into_body();
        let description_body = self.description_collector.into_body();
        let claims_body = self.claims_collector.into_body();

        let abstract_text = format!("## Abstract\n\n{abstract_body}");
        let description = format!("## Description\n\n{description_body}");
        let claims = format!("## Claims\n\n{claims_body}");
        let markdown = format!(
            "# {title}\n\n## Abstract\n\n{abstract_body}\n\n## Description\n\n{description_body}\n\n## Claims\n\n{claims_body}"
        );

        Ok(XmlPublication {
            publication_uid: format!("{publication_key}:{publication_number}:{publication_kind}"),
            publication_date: date_rfc3339(&self.document_date.unwrap_or_default()),
            filing_date: date_rfc3339(&self.filing_date.unwrap_or_default()),
            title,
            abstract_text,
            claims,
            description,
            publication_number,
            publication_key,
            application_number: self.application_number.unwrap_or_default(),
            applicant: self.applicants,
            assignee: if self.saw_assignee { self.assignee } else { Vec::new() },
            ipc: self.ipc.into_iter().collect(),
            us_class: self.us_class,
            publication_kind,
            markdown,
        })
    }
}

impl InventorParts {
    fn take_name(&mut self) -> Option<String> {
        self.target.take()?;
        let mut parts = Vec::new();
        if let Some(given) = self.given.take() {
            parts.push(given);
        }
        if let Some(middle) = self.middle.take() {
            parts.push(middle);
        }
        if let Some(family) = self.family.take() {
            parts.push(family);
        }
        (!parts.is_empty()).then(|| parts.join(" "))
    }
}

fn append_field(target: &mut Option<String>, text: &str) {
    if text.is_empty() {
        return;
    }
    *target = Some(match target.take() {
        Some(existing) => format!("{existing} {text}"),
        None => text.to_owned(),
    });
}

fn is_under_direct_biblio_document_id(path: &[String]) -> bool {
    path.len() >= 3
        && path[path.len() - 3] == "subdoc-bibliographic-information"
        && path[path.len() - 2] == "document-id"
}

fn path_ends_with(path: &[String], suffix: &[&str]) -> bool {
    path.len() >= suffix.len()
        && path[path.len() - suffix.len()..]
            .iter()
            .map(String::as_str)
            .eq(suffix.iter().copied())
}

fn path_contains(path: &[String], needle: &str) -> bool {
    path.iter().any(|part| part == needle)
}

fn normalize_subclass(subclass: &str) -> String {
    if subclass.bytes().all(|byte| byte.is_ascii_digit()) {
        let trimmed = subclass.trim_end_matches('0');
        if trimmed.is_empty() {
            "0".to_owned()
        } else {
            trimmed.to_owned()
        }
    } else {
        subclass.to_owned()
    }
}
