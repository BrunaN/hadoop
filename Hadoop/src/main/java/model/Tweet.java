package model;

import java.util.HashSet;
import java.util.Set;

import com.sun.istack.NotNull;

public class Tweet {
	private String text;
	
	private Author author;
	
	public Tweet(Object value) {
	}

	@NotNull
	public String getText() {
		return text;
	}
	
	public Author getAuthor() {
		return author;
	}
}
