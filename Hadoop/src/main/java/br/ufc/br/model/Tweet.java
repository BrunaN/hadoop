package br.ufc.br.model;

public class Tweet {
	private Id _id;
	private String createdAt;
	private String text;
	private Author author;
	
	public class Id {
		private String $oid;

		public Id(Object value) {
		}
		
		public String getId() {
			return $oid;
		}
	}
		
	public Tweet(Object value) {
	}

	public String getCreatedAt() {
		return createdAt;
	}
	
	public String getText() {
		return text;
	}
	
	public Author getAuthor() {
		return author;
	}
	
	public String getId() {
		return _id.getId();
	}
}
