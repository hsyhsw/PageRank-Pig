package kr.ac.kaist.adward.pagerankpig.model;

public class WikiPage {
	private Long documentId;
	private int ns;
	private String title;
	private String wikiText;

	public WikiPage() {
		ns = 0;
	}

	public int getNs() {
		return ns;
	}

	public void setNs(int ns) {
		this.ns = ns;
	}

	public Long getDocumentId() {
		return documentId;
	}

	public void setDocumentId(Long documentId) {
		this.documentId = documentId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getWikiText() {
		return wikiText;
	}

	public void setWikiText(String wikiText) {
		this.wikiText = wikiText;
	}
}
