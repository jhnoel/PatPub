//! USPTO supplemental `sequence-cwu` (sequence listing) records.
//!
//! These follow their patent in the modern concatenated weekly feed and carry
//! their own `publication-reference/document-id` identity — the same structure
//! the v4.x grant/application adapters parse — but the listing payload itself
//! is not modeled: it round-trips as opaque XML. The bar is a faithful,
//! addressable, retrievable text record with correct identity, not a rich
//! rendering (docs/design/render-supplemental-roots.md).

use crate::error::{ParseError, SerializeError};
use crate::model::bibliographic::{BibliographicInformation, BibliographicPart};
use crate::model::document::{DocumentPart, PatentDocument};
use crate::model::opaque::OpaqueBlock;
use crate::source::detect::SourceFormat;
use crate::source::traits::FormatAdapter;
use crate::source::xml;

use super::grant_v4x;

pub struct UsptoSequenceCwuAdapter;

impl FormatAdapter for UsptoSequenceCwuAdapter {
    fn parse_document(&self, input: &str) -> Result<PatentDocument, ParseError> {
        let document = xml::parse_document(input)?;
        let root = xml::root_element(&document)?;

        if document.node_name(root) != Some("sequence-cwu") {
            return Err(ParseError::UnsupportedStructure(
                "sequence listings require sequence-cwu root".to_string(),
            ));
        }

        let mut parts = Vec::new();
        for child in document.children(root) {
            if document.node_name(child) == Some("publication-reference")
                && let Some(document_id) = grant_v4x::child_element(&document, child, "document-id")
            {
                parts.push(DocumentPart::BibliographicInformation(
                    BibliographicInformation {
                        attributes: xml::attributes(&document, child),
                        parts: vec![BibliographicPart::DocumentId(
                            grant_v4x::parse_document_id_v4(&document, document_id),
                        )],
                    },
                ));
                continue;
            }
            // Everything else — the listing payload — is opaque and
            // re-serializes verbatim.
            parts.push(DocumentPart::Opaque(OpaqueBlock {
                xml: xml::xml_fragment(&document, child),
            }));
        }

        Ok(PatentDocument {
            source_format: SourceFormat::UsptoSequenceCwu,
            prolog: xml::prolog(input, &document),
            attributes: xml::attributes(&document, root),
            parts,
        })
    }

    fn write_document(&self, doc: &PatentDocument) -> Result<String, SerializeError> {
        let mut output = String::new();
        xml::write_prolog(&mut output, &doc.prolog);
        xml::start_tag(&mut output, "sequence-cwu", &doc.attributes);
        for part in &doc.parts {
            match part {
                DocumentPart::BibliographicInformation(info) => {
                    xml::start_tag(&mut output, "publication-reference", &info.attributes);
                    for part in &info.parts {
                        if let BibliographicPart::DocumentId(value) = part {
                            grant_v4x::write_document_id(&mut output, value);
                        }
                    }
                    xml::end_tag(&mut output, "publication-reference");
                }
                DocumentPart::Opaque(value) => xml::write_xml_fragment(&mut output, &value.xml),
                _ => {}
            }
        }
        xml::end_tag(&mut output, "sequence-cwu");
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::bibliographic::DocumentId;

    const FIXTURE: &str = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<sequence-cwu id=\"SEQLST-1\">\n",
        "<publication-reference>\n",
        "<document-id>\n",
        "<country>US</country>\n",
        "<doc-number>12667087</doc-number>\n",
        "<kind>B2</kind>\n",
        "<date>20260630</date>\n",
        "</document-id>\n",
        "</publication-reference>\n",
        "<s400><![CDATA[<210> 1\n<211> 21\n<212> DNA\nacgtacgtacgt acgtacgta 21]]></s400>\n",
        "</sequence-cwu>"
    );

    fn parsed_document_id(doc: &PatentDocument) -> &DocumentId {
        doc.parts
            .iter()
            .find_map(|part| match part {
                DocumentPart::BibliographicInformation(info) => {
                    info.parts.iter().find_map(|part| match part {
                        BibliographicPart::DocumentId(value) => Some(value),
                        _ => None,
                    })
                }
                _ => None,
            })
            .expect("publication-reference identity must parse")
    }

    #[test]
    fn detects_and_parses_identity() {
        let doc = crate::parse_patent_xml(FIXTURE).expect("sequence-cwu must parse");
        assert_eq!(doc.source_format, SourceFormat::UsptoSequenceCwu);

        let id = parsed_document_id(&doc);
        assert_eq!(id.doc_number, "12667087");
        assert_eq!(id.kind_code.as_deref(), Some("B2"));
        assert_eq!(id.document_date.as_deref(), Some("20260630"));
        assert_eq!(id.country_code.as_deref(), Some("US"));
    }

    #[test]
    fn markdown_carries_listing_payload_and_identity() {
        let doc = crate::parse_patent_xml(FIXTURE).unwrap();
        let md = crate::render_markdown(&doc);

        assert!(md.contains("patent_number: US12667087B2"), "{md}");
        assert!(md.contains("publication_date: 20260630"), "{md}");
        assert!(md.contains("acgtacgtacgt"), "payload must render:\n{md}");
    }

    #[test]
    fn write_round_trips_losslessly() {
        let doc = crate::parse_patent_xml(FIXTURE).unwrap();
        let written = crate::write_patent_xml(&doc).expect("sequence-cwu must serialize");
        let reparsed = crate::parse_patent_xml(&written).expect("round-tripped XML must parse");

        assert_eq!(doc, reparsed);
        assert!(written.contains("<doc-number>12667087</doc-number>"));
        assert!(written.contains("acgtacgtacgt"));
    }

    #[test]
    fn missing_identity_still_parses_as_opaque_payload() {
        let doc = crate::parse_patent_xml(
            "<?xml version=\"1.0\"?><sequence-cwu id=\"SEQLST-2\"><s400>acgt</s400></sequence-cwu>",
        )
        .expect("identity-less sequence-cwu still parses");
        assert!(
            doc.parts
                .iter()
                .all(|part| matches!(part, DocumentPart::Opaque(_)))
        );
    }
}
