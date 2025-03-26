package org.yinuo.rabbitmq101;

public class Album {
  private String title;
  private String artist;
  private int year;
  private int albumId;

  public Album(String artist, String title, int year, int albumId) {
    this.artist = artist;
    this.title = title;
    this.year = year;
    this.albumId = albumId;
  }

  // Getters and Setters
  public String getArtist() { return artist; }
  public void setArtist(String artist) { this.artist = artist; }

  public String getTitle() { return title; }
  public void setTitle(String title) { this.title = title; }

  public int getYear() { return year; }
  public void setYear(int year) { this.year = year; }

  public int getAlbumId() { return albumId; }
  public void setAlbumId(int albumId) { this.albumId = albumId; }
}
